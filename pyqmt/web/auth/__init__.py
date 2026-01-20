"""
FastHTML-Auth: Complete authentication system for FastHTML applications.

Provides user authentication, session management, role-based access control,
and beautiful UI components out of the box.
"""

__version__ = "0.1.2"
__author__ = "John Richmond"
__email__ = "confusedjohn46@gmail.com"

from .database import AuthDatabase
from .manager import AuthManager
from .middleware import AuthBeforeware
from .models import Session, User
from .repository import UserRepository

__all__ = [
    "AuthManager",
    "User",
    "Session",
    "AuthBeforeware",
    "AuthDatabase",
    "UserRepository",
]

# Convenience imports for common usage patterns
from .manager import AuthManager as Auth  # Shorter alias

# Version info tuple
VERSION = tuple(map(int, __version__.split(".")))
