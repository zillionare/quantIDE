# auth/models.py
from dataclasses import dataclass
import bcrypt
from datetime import datetime


@dataclass
class User:
    id: int | None = None
    username: str | None = None
    email: str | None = None
    password: str | None = None
    role: str = "user"
    created_at: str = ""
    last_login: str = ""
    active: bool = True

    # Define primary key for fastlite
    pk = "id"

    @classmethod
    def get_hashed_password(cls, password: str) -> str:
        """Hash a password using bcrypt"""
        try:
            if not password:
                raise ValueError("Password cannot be empty")
            # Use bcrypt directly instead of passlib to avoid version issues
            salt = bcrypt.gensalt()
            hashed = bcrypt.hashpw(password.encode("utf-8"), salt)
            return hashed.decode("utf-8")
        except Exception as e:
            print(f"Error hashing password: {e}")
            raise

    @classmethod
    def is_hashed(cls, pwd: str) -> bool:
        """Check if password is already hashed (bcrypt hashes start with $2b$)"""
        return pwd and pwd.startswith("$2b$") and len(pwd) == 60

    @classmethod
    def verify_password(cls, pwd: str, hashed: str) -> bool:
        """Verify password against hash"""
        try:
            if not pwd or not hashed:
                return False
            return bcrypt.checkpw(pwd.encode("utf-8"), hashed.encode("utf-8"))
        except Exception as e:
            print(f"Error verifying password: {e}")
            return False

    def __post_init__(self):
        """Initialize timestamps and hash password if needed"""
        # Hash password if not already hashed
        if self.password and not self.is_hashed(self.password):
            print(f"Hashing password for user: {self.username}")
            self.password = self.get_hashed_password(self.password)

        # Initialize timestamps if not set
        now = datetime.now().isoformat()
        if not self.created_at:
            self.created_at = now
        if not self.last_login:
            self.last_login = now


@dataclass
class Session:
    """Pure Session model"""

    id: str
    user_id: int
    data: dict
    expires_at: str
    created_at: str

    # Define primary key for fastlite
    pk = "id"
