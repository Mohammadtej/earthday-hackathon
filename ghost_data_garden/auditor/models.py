from django.db import models
from django.contrib.auth.models import AbstractUser

class User(AbstractUser):
    email = models.EmailField(unique=True)

    USERNAME_FIELD = 'email' # use email instead of username for auth
    REQUIRED_FIELDS = ['username']

    def __str__(self):
        return self.username

