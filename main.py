import os
from dotenv import load_dotenv
from google import genai
import snowflake.connector

# Load variables from .env
load_dotenv()

# Access them safely
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Build Snowflake configuration from environment variables
SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "role": os.getenv("SNOWFLAKE_ROLE")
}

# Configure Gemini
client = genai.Client(api_key=GEMINI_API_KEY)

def fetch_inefficient_queries():
    # Clean the config to avoid passing 'None' values to the connector
    clean_config = {k: v for k, v in SNOWFLAKE_CONFIG.items() if v}

    try:
        conn = snowflake.connector.connect(**clean_config)
        cur = conn.cursor()
        
        # Query to find the top 1 query that scanned the most data
        audit_sql = """
        SELECT QUERY_TEXT, BYTES_SCANNED, TOTAL_ELAPSED_TIME
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE EXECUTION_STATUS = 'SUCCESS'
          AND BYTES_SCANNED > 0
        ORDER BY BYTES_SCANNED DESC
        LIMIT 1;
        """
        cur.execute(audit_sql)
        result = cur.fetchone()
        conn.close()
        return result
    except snowflake.connector.errors.Error as e:
        print(f"Snowflake Connection Error: {e.msg}")
        return None

# --- 3. THE GEMINI AUDIT ---
def get_green_advice(bad_sql, bytes_scanned):
    # Convert bytes to Gigabytes for the prompt
    gb_scanned = round(bytes_scanned / (1024**3), 2)
    
    prompt = f"""
    You are a Carbon-Efficiency Database Expert. 
    The following SQL query scanned {gb_scanned} GB of data in Snowflake. 
    This is a high carbon footprint. 
    
    SQL:
    {bad_sql}
    
    Tasks:
    1. Rewrite this SQL to be more 'Green' (e.g., add partitioning, prune columns, or use better joins).
    2. Explain the estimated percentage of energy/compute reduction.
    3. Suggest if this data should be 'archived' (Ghost Data) if it's rarely used.
    
    Format the output in clean Markdown.
    """
    
    response = client.models.generate_content(
    model='gemini-2.5-flash',
    contents=prompt,
    config={
        'temperature': 0,
        'top_p': 0.95,
        'top_k': 20,
        },
    )
    return response.text

# --- 4. EXECUTION ---
query_data = fetch_inefficient_queries()

print('The query data returned from the snowflake account', query_data)

if query_data:
    raw_sql, bytes_sc, time_ms = query_data
    print("--- FOUND GHOST DATA ---")
    print(f"Bytes Scanned: {bytes_sc}")
    
    green_report = get_green_advice(raw_sql, bytes_sc)
    print("\n--- GEMINI GREEN AUDIT REPORT ---")
    print(green_report)
else:
    print("No queries found in history. Go run a query in the Snowflake Worksheet first!")