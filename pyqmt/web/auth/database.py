# auth/database.py
from fasthtml.common import database
from pathlib import Path

class AuthDatabase:
    """Database manager owned by auth system"""

    def __init__(self, db_path="data/app.db"):
        # Ensure directory exists
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        # Create database connection
        self.db = database(db_path)
        self.db_path = db_path
        
        # Table references (will be populated)
        self.users = None
        self.sessions = None  # Optional session storage
        self.audit_log = None  # Optional security audit
    
    def initialize_auth_tables(self):
        from .models import User, Session
        
        # Force User to be fully processed as a dataclass
        import dataclasses
        if not dataclasses.is_dataclass(User):
            raise Exception("User is not a proper dataclass!")
        
        self.users = self.db.create(User, pk=User.pk)

        return self.db
    
    def get_db(self):
        """Get database instance for app to add tables"""
        return self.db