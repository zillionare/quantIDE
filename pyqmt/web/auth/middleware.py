from fasthtml.common import *
import inspect
from typing import Optional, List, Any, Callable

class AuthBeforeware:
    """Authentication middleware for fastHTML"""

    def __init__(self, auth_manager, config=None):
        self.auth_manager = auth_manager
        self.config = config or {}

        # Configure paths
        login_path = '/auth/login'
        self.login_path = self.config.get("login_path", login_path)
        self.public_paths = self.config.get("public_paths", [])
        # Static files patterns
        self.static_patterns = [
            r'/favicon\.ico',
            r'/static/.*',
            r'.*\.css',
            r'.*\.js',
            r'.*\.png',
            r'.*\.jpg',
            r'.*\.jpeg',
            r'.*\.gif',
            r'.*\.svg',
        ]

    def create_beforeware(self, additional_public_paths=None):
        """Create beforeware for fastHTML"""
        skip_patterns = self._build_skip_patterns(additional_paths=additional_public_paths)

        # Create Beforeware authentication check function
        def auth_check(req, sess):
            """Check authentication prior to check"""
            auth_username = sess.get('auth')

            # If no session auth, check for remember me cookie
            if not auth_username:
                remember_user = req.cookies.get('remember_user')
                if remember_user:
                    # Verify user still exists and is active
                    user = self.auth_manager.get_user(remember_user)
                    if user and user.active:
                        # Restore session from remember me cookie
                        sess['auth'] = user.username
                        sess['user_id'] = user.id
                        sess['role'] = user.role
                        sess['remember_me'] = True
                        auth_username = user.username
                    else:
                        # Invalid remember me cookie, continue to redirect
                        pass

            if not auth_username:
                # No auth, redirect to login
                return RedirectResponse(self.login_path, status_code=303)
            
            # Verify user is still valid
            user = self.auth_manager.get_user(auth_username)
            if not user or not user.active:
                # Invalid user, clear session and redirect
                sess.clear()
                return RedirectResponse(self.login_path, status_code=303)
            
            # Add user info to request scope for route handlers
            req.scope['auth'] = auth_username
            req.scope['user'] = user
            req.scope['user.id'] = user.id
            req.scope['user_role'] = user.role
            req.scope['user_is_admin'] = user.role == "admin"

        # Return configured Beforeware
        return Beforeware(auth_check, skip=skip_patterns)


    def _build_skip_patterns(self, additional_paths=None) -> list:
        """Build list of paths to skip auth check"""

        skip = []

        # Static files
        skip.extend(self.static_patterns)

        # Auth routes
        skip.extend([
            self.login_path,
            '/auth/register',
            '/auth/forgot',
            '/auth/reset'
        ])

        # Configured public paths
        skip.extend(self.public_paths)

        # Additional paths from caller
        if additional_paths:
            skip.extend(additional_paths)

        # Health check and api endpoints
        skip.extend([
            "/health",
            "/api/public"
        ])

        return skip
    
    def require_admin(self):
        """Decorator for paths requiting admin role to access"""
        return self.require_role('admin')

    from typing import Any
    import functools

    def require_role(self, *allowed_roles):
        """Decorator to require a specific role"""
        def decorator(func):
            @functools.wraps(func)  # ← Copy original function signature
            def wrapper(req, *args: Any, **kwargs: Any):  # ← Add type annotations
                user = req.scope.get('user')
                if not user or user.role not in allowed_roles:
                    return Response("Forbidden", status_code=403)
                
                # Check if function accepts extra args
                sig = inspect.signature(func)
                if len(sig.parameters) == 1:  # Only takes req
                    return func(req)
                else:
                    return func(req, *args, **kwargs)
            return wrapper
        return decorator
