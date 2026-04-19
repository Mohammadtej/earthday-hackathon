import json
import os
from django.shortcuts import render, redirect
from django.http import JsonResponse
from django.core.paginator import Paginator
import snowflake.connector
from google import genai
from dotenv import load_dotenv

# Load the Gemini Key from .env (Snowflake creds will come from JSON upload now)
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=GEMINI_API_KEY)

def get_snowflake_connection(request):
    """Helper function to establish a Snowflake connection using session creds."""
    creds = request.session.get('snowflake_creds')
    if not creds:
        return None
    
    clean_config = {k: v for k, v in creds.items() if v}
    # Clean the account URL just in case the JSON has a full URL
    if 'account' in clean_config:
        clean_config['account'] = clean_config['account'].replace("https://", "").replace(".snowflakecomputing.com", "").split(":")[0]
        
    return snowflake.connector.connect(**clean_config)

def login_view(request):
    """1) User provides Snowflake credentials via JSON file."""
    if request.method == 'POST' and 'creds_file' in request.FILES:
        try:
            creds_data = json.load(request.FILES['creds_file'])
            # Store credentials securely in the user's Django session
            request.session['snowflake_creds'] = creds_data
            return redirect('dashboard')
        except Exception as e:
            return render(request, 'auditor/login.html', {'error': f'Invalid JSON file: {str(e)}'})
            
    return render(request, 'auditor/login.html')

def logout_view(request):
    """Clears the session and redirects to the login page."""
    request.session.flush()
    return redirect('login')

def dashboard_view(request):
    """2) Dashboard with metrics and action buttons."""
    creds = request.session.get('snowflake_creds')
    if not creds:
        return redirect('login')
    
    # Parse the table stats stored as a JSON string in the session
    table_stats_json = request.session.get('table_stats')
    table_stats = []
    if table_stats_json:
        try:
            table_stats = json.loads(table_stats_json)
        except json.JSONDecodeError:
            pass

    # Manage CO2 saved as a float for math operations, starting at 0.0
    co2_saved = request.session.get('co2_saved', 0.0)
    if isinstance(co2_saved, str):
        try:
            co2_saved = float(co2_saved.replace(' kg/yr', '').replace(' kg', ''))
        except ValueError:
            co2_saved = 0.0
    request.session['co2_saved'] = co2_saved

    co2_display = f"{co2_saved:.2f} kg/yr" if co2_saved > 0 else "Pending (Accept suggestions)"

    context = {
        'database': creds.get('database', 'Not Specified'),
        'schema': creds.get('schema', 'Not Specified'),
        'co2_saved': co2_display,
        'zombie_tables_count': request.session.get('zombie_tables_count', 'Pending (Run "Identify Zombie Tables")'),
        'compute_efficiency': request.session.get('compute_efficiency', 'Pending (Run "Identify High Compute Queries")'),
        'table_stats': table_stats
    }
    return render(request, 'auditor/dashboard.html', context)

def gather_statistics(request):
    """Gathers preliminary statistics for the dashboard."""
    try:
        conn = get_snowflake_connection(request)
        if conn:
            cur = conn.cursor()
            # Heuristic to count tables in the current schema to simulate gathering stats
            # cur.execute(f"SHOW TABLES IN SCHEMA {request.session['snowflake_creds']['database']}.{request.session['snowflake_creds']['schema']}")

            cur.execute(f"""
                SELECT TABLE_NAME, ROW_COUNT, BYTES, LAST_ALTERED
                FROM {request.session['snowflake_creds']['database']}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = '{request.session['snowflake_creds']['schema']}'
                AND TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME;
            """)
            
            # Fetch and convert tuples to lists so we can modify the size column
            tables = [list(row) for row in cur.fetchall()]

            print(type(tables))

            for row in tables:
                row[2] = round(row[2] / (1024**2), 2)  # Convert bytes to GB for readability
            # Convert to JSON string handling datetime objects
            tables_json = json.dumps(tables, default=str)

            print(f"Tables JSON: {tables_json}")
            # Store the basic stats, leaving detailed metrics for explicit analysis
            request.session['table_stats'] = tables_json
            
            # Clear previously cached reports and metrics so new data triggers a fresh analysis
            request.session.pop('zombie_report_content', None)
            request.session.pop('high_compute_queries', None) # Clear cached queries list
            request.session.pop('zombie_tables_count', None)
            request.session.pop('compute_efficiency', None)
            conn.close()
    except Exception as e:
        print(f"Error gathering stats: {e}")
        
    return redirect('dashboard')

def zombie_tables_report(request):
    """4) Find zombie tables using ACCOUNT_USAGE and call Gemini for a tabular report."""
    cached_report = request.session.get('zombie_report_content')
    if cached_report:
        return render(request, 'auditor/report.html', {'title': 'Zombie Tables Report', 'report_content': cached_report})
        
    conn = get_snowflake_connection(request)
    if not conn:
        return redirect('login')
        
    try:
        cur = conn.cursor()
        db_name = request.session['snowflake_creds'].get('database', '')
        schema_name = request.session['snowflake_creds'].get('schema', '')
        
        zombie_sql = f"""
            SELECT 
                TABLE_NAME, 
                ROW_COUNT, 
                BYTES, 
                LAST_ALTERED
            FROM {db_name}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema_name}'
              AND TABLE_TYPE = 'BASE TABLE'
              -- Filter to find actual zombies: older than 30 days
              AND LAST_ALTERED < DATEADD(day, -30, CURRENT_DATE())
            ORDER BY BYTES DESC
            LIMIT 50;
        """
        cur.execute(zombie_sql)
        tables = cur.fetchall()
        
        if not tables:
            report_content = "Great news! No inactive 'Zombie' tables older than 30 days were found in this schema."
            request.session['zombie_report_content'] = report_content
            request.session['zombie_tables_count'] = 0
            return render(request, 'auditor/report.html', {'title': 'Zombie Tables Report', 'report_content': report_content})

        request.session['zombie_tables_count'] = len(tables)
        # Format the list for Gemini (convert bytes to MB)
        table_details = [f"- Table: {t[0]} | Rows: {t[1]} | Size: {round(t[2]/(1024**2), 2)} MB | Last Accessed: {t[3]}" for t in tables]
        tables_str = "\n".join(table_details)
        
        prompt = f"""
        You are a Database Carbon-Efficiency Expert. 
        We have queried our Snowflake ACCOUNT_USAGE views and identified the following inactive "Zombie Tables" (unaltered in >30 days):
        {tables_str}
        
        Tasks:
        1. Explain briefly why retaining unused tables wastes storage/compute and emits unnecessary CO2.
        2. Provide a report in a strict tabular format (Markdown) listing these tables with their actual rows, size, and "Last Accessed" date, and an "Action Recommendation" (e.g., Archive to S3, Drop).
        """
        
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
            config={'temperature': 0, 'top_p': 0.95, 'top_k': 20}
        )
        report_content = response.text
        request.session['zombie_report_content'] = report_content
    except Exception as e:
        report_content = f"Error fetching zombie tables (You may need ACCOUNTADMIN role to view ACCOUNT_USAGE): {str(e)}"
    finally:
        conn.close()
        
    return render(request, 'auditor/report.html', {'title': 'Zombie Tables Report', 'report_content': report_content})

def high_compute_list(request):
    """Fetch high compute queries, cache them, and display a paginated list."""
    queries = request.session.get('high_compute_queries')
    
    # Fetch from Snowflake if not already cached in the session
    if not queries:
        conn = get_snowflake_connection(request)
        if not conn:
            return redirect('login')
            
        try:
            cur = conn.cursor()
            audit_sql = """
            SELECT QUERY_ID, QUERY_TEXT, BYTES_SCANNED, TOTAL_ELAPSED_TIME
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE EXECUTION_STATUS = 'SUCCESS'
              AND BYTES_SCANNED > 0
              AND START_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
            ORDER BY TOTAL_ELAPSED_TIME DESC, BYTES_SCANNED DESC
            LIMIT 50;
            """
            cur.execute(audit_sql)
            raw_queries = cur.fetchall()
            
            queries = []
            for row in raw_queries:
                queries.append({
                    'query_id': row[0],
                    'query_text': row[1],
                    'bytes_scanned': row[2],
                    'total_elapsed_time': row[3],
                    'time_seconds': round(row[3] / 1000, 2) if row[3] else 0
                })
            request.session['high_compute_queries'] = queries
            
            # Compute efficiency heuristic (100% minus 2% for each inefficient query found in top 50)
            efficiency = max(0, 100 - (len(queries) * 2))
            request.session['compute_efficiency'] = f"{efficiency}%"
        except Exception as e:
            return render(request, 'auditor/report.html', {'title': 'Error', 'report_content': f"Error fetching queries: {str(e)}"})
        finally:
            conn.close()

    # Paginate the results (10 per page)
    paginator = Paginator(queries, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    
    return render(request, 'auditor/high_compute_list.html', {'page_obj': page_obj})

def high_compute_report(request, query_id):
    """Analyze a specific query with Gemini."""
    cache_key = f'high_compute_report_{query_id}'
    cached_report = request.session.get(cache_key)
    if cached_report:
        return render(request, 'auditor/report.html', {'title': 'Query Optimization Report', 'report_content': cached_report})
        
    queries = request.session.get('high_compute_queries', [])
    target_query = next((q for q in queries if q['query_id'] == query_id), None)
    
    if not target_query:
        return render(request, 'auditor/report.html', {'title': 'Error', 'report_content': "Query not found. Please gather statistics again."})
        
    try:
        raw_sql = target_query['query_text']
        gb_scanned = round(target_query['bytes_scanned'] / (1024**3), 4)
        time_sec = round(target_query['total_elapsed_time'] / 1000, 2)
        
        prompt = f"""
        You are a Carbon-Efficiency Database Expert. 
        The following SQL query scanned {gb_scanned} GB of data and took {time_sec} seconds in Snowflake. 
        This represents a potentially high carbon footprint. 
        
        SQL:
        {raw_sql}
        
        Tasks:
        1. Rewrite this SQL to be more 'Green' (e.g., add partitioning, prune columns, use better joins).
        2. Explain the estimated percentage of energy/compute reduction and projected annual CO2 savings.
        3. Suggest if this data should be 'archived' (Ghost Data) if it's rarely used.
        4. Provide the final optimized SQL query in a single Markdown code block at the very end of your response.
        
        Format the output in clean Markdown.
        """
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
            config={'temperature': 0, 'top_p': 0.95, 'top_k': 20}
        )
        report_content = response.text
        request.session[cache_key] = report_content
    except Exception as e:
        report_content = f"Error generating report: {str(e)}"

    return render(request, 'auditor/report.html', {'title': 'High Compute Queries Report', 'report_content': report_content})

def accept_suggestion(request):
    """Endpoint to handle accepting Gemini's green suggestion."""
    if request.method == 'POST':
        co2_val = request.session.get('co2_saved', 0.0)
        if isinstance(co2_val, str):
            try:
                co2_val = float(co2_val.replace(' kg/yr', '').replace(' kg', ''))
            except ValueError:
                co2_val = 0.0
        
        # Add an annualized estimated CO2 saving (e.g., 2.5 kg per run * 365 days)
        saved_amount = 912.5
        request.session['co2_saved'] = round(co2_val + saved_amount, 2)
        
        return JsonResponse({'status': 'success', 'saved': saved_amount, 'total': request.session['co2_saved']})
    return JsonResponse({'status': 'error', 'message': 'Invalid request method'})
