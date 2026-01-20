from typing import Optional

from .models import User


class UserRepository:
    """Handles all database operations for users"""

    def __init__(self, db):
        self.db = db
        self.users = db.t.user

    def _dict_to_user(self, user_dict) -> User:
        """Convert dictionary from database to User object"""
        if isinstance(user_dict, User):
            return user_dict

        return User(
            id=user_dict.get("id"),
            username=user_dict["username"],
            email=user_dict["email"],
            password=user_dict["password"],
            role=user_dict.get("role", "user"),
            created_at=user_dict.get("created_at", ""),
            last_login=user_dict.get("last_login", ""),
            active=user_dict.get("active", True),
        )

    def get_by_username(self, username: str) -> Optional[User]:
        """Get user by username using parameterized query"""
        try:
            user_found = self.users("username=?", (username,))
            if len(user_found) == 1:
                if isinstance(user_found[0], User):
                    return user_found[0]
                else:
                    return self._dict_to_user(user_found[0])
                return user_found[0]  # Return the single user object
            elif len(user_found) == 0:
                return None
            else:
                raise Exception(f"Multiple users found with username: {username}")
        except Exception as e:
            print(f"Error in get_by_username: {e}")
            return None

    def get_by_id(self, user_id: int) -> Optional[User]:
        """Get user by ID"""
        try:
            user_dict = self.users[user_id]
            if user_dict:
                return self._dict_to_user(user_dict)
            return None
        except Exception as e:
            print(f"Error in get_by_id: {e}")
            return None

    def create(
        self, username: str, email: str, password: str, role: str = "user"
    ) -> User:
        """Create new user.  Note that the dates will be updated by the class -_post_init__ method"""
        user = User(
            username=username,
            email=email,
            password=password,  # Use static method
            role=role,
            active=True,
            created_at="",
            last_login="",
        )
        inserted_user = self.users.insert(user)
        if isinstance(inserted_user, dict):
            return self._dict_to_user(inserted_user)
        else:
            return inserted_user

    def authenticate(self, username: str, password: str) -> Optional[User]:
        """Authenticate user and update last_login"""
        user = self.get_by_username(username)
        print(f"User: {user}")
        if user and user.active and User.verify_password(password, user.password):
            # Update last_login using the new fastlite approach
            from datetime import datetime

            now = datetime.now().isoformat()
            self.users.update(last_login=now, id=user.id)
            return user
        return None

    def update(self, user_id: int, **kwargs) -> bool:
        """Update user fields using fastlite kwargs approach"""
        try:
            # Hash password if being updated                    # <- NEW
            if "password" in kwargs and kwargs["password"]:  # <- NEW
                if not User.is_hashed(kwargs["password"]):  # <- NEW
                    kwargs["password"] = User.get_hashed_password(
                        kwargs["password"]
                    )  # <- NEW

            # Include the primary key in the update kwargs
            self.users.update(id=user_id, **kwargs)
            return True
        except Exception as e:
            print(f"Error updating user {user_id}: {e}")
            return False

    def delete(self, user_id: int) -> bool:
        """Delete a user by ID"""
        try:
            # First check if user exists
            user = self.get_by_id(user_id)
            if not user:
                print(f"User with id {user_id} not found")
                return False

            # Prevent deletion of last admin
            if user.role == "admin":
                admin_count = self.count_by_role().get("admin", 0)
                if admin_count <= 1:
                    print("Cannot delete the last admin user")
                    return False

            # Delete using the primary key value directly
            self.users.delete(user_id)
            return True

        except Exception as e:
            print(f"Error deleting user {user_id}: {e}")
            return False

    def delete_by_username(self, username: str) -> bool:
        """Delete a user by username - more practical for admin interfaces"""
        try:
            # Get the user to get their ID
            user = self.get_by_username(username)
            if not user:
                print(f"User '{username}' not found")
                return False

            # Prevent deletion of last admin
            if user.role == "admin":
                admin_count = self.count_by_role().get("admin", 0)
                if admin_count <= 1:
                    print("Cannot delete the last admin user")
                    return False

            # Delete using the user's ID
            self.users.delete(user.id)
            return True
        except Exception as e:
            print(f"Error deleting user '{username}': {e}")
            return False

    def count_by_role(self) -> dict:
        """Get count of users by role"""
        try:
            users = self.list_all()
            counts = {"user": 0, "manager": 0, "admin": 0}
            for user in users:
                if user.role in counts:
                    counts[user.role] += 1
            return counts
        except Exception as e:
            print(f"Error counting users by role: {e}")
            return {"user": 0, "manager": 0, "admin": 0}

    def search_users(
        self, query: str, role: Optional[str] = None, active: Optional[bool] = None
    ) -> list:
        """Search users with optional filters"""
        try:
            users = self.list_all()

            # Apply search filter
            if query:
                query_lower = query.lower()
                users = [
                    u
                    for u in users
                    if query_lower in u.username.lower()
                    or query_lower in u.email.lower()
                ]

            # Apply role filter
            if role:
                users = [u for u in users if u.role == role]

            # Apply active filter
            if active is not None:
                users = [u for u in users if u.active == active]

            return users
        except Exception as e:
            print(f"Error searching users: {e}")
            return []

    def list_all(self) -> list[User]:
        """Get all users"""
        try:
            users = []
            for user_dict in self.users():
                users.append(self._dict_to_user(user_dict))
            return users
        except Exception as e:
            print(f"Error listing users: {e}")
            return []

    def verify_password(self, password: str, hashed: str) -> bool:
        """Verify password against hash - delegates to User class"""
        return User.verify_password(password, hashed)
