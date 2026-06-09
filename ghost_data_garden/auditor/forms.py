from django.contrib.auth.forms import UserCreationForm
from django.contrib.auth import get_user_model
from django import forms

User = get_user_model()

class CustomUserCreationForm(UserCreationForm):
    class Meta(UserCreationForm.Meta):
        model = User
        fields = ("username", "email")

    def clean_email(self):
        email = self.cleaned_data.get('email')
        if email:
            domain = email.split('@')[-1].lower()
            # Add or remove domains from this list as needed
            disposable_domains = [
                'mailinator.com', '10minutemail.com', 'guerrillamail.com',
                'temp-mail.org', 'yopmail.com', 'throwawaymail.com', 'sharklasers.com'
            ]
            if domain in disposable_domains:
                raise forms.ValidationError("Please use a valid, non-disposable email address.")
        return email

class CustomLoginForm(forms.Form):
    email = forms.EmailField(widget=forms.EmailInput(attrs={'autofocus': True}))
    password = forms.CharField(label="Password", strip=False, widget=forms.PasswordInput)