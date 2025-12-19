"""Utility functions for FastHTML-Auth"""

from typing import Optional
import secrets
import string
import re

def generate_token(length: int = 32) -> str:
    """Generate a secure random token for password resets, etc."""
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))

def validate_email(email: str) -> bool:
    """Basic email validation"""
    if not email or '@' not in email:
        return False
    
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))

def validate_password(password: str) -> tuple[bool, str]:
    """
    Validate password strength
    Returns: (is_valid, error_message)
    """
    if not password:
        return False, "Password is required"
    
    if len(password) < 8:
        return False, "Password must be at least 8 characters long"
    
    # Optional: Add more complexity requirements
    if not any(c.isdigit() for c in password):
        return False, "Password must contain at least one number"
    
    if not any(c.isupper() for c in password):
        return False, "Password must contain at least one uppercase letter"
    
    return True, ""

def sanitize_username(username: str) -> str:
    """Sanitize username to only allow safe characters"""
    # Only allow alphanumeric and underscore
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '', username)
    return sanitized.lower()