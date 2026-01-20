# auth/routes.py
from fasthtml.common import *
from monsterui.all import *

from .forms import (
    create_forgot_password_form,
    create_login_form,
    create_profile_form,
    create_register_form,
)


class AuthRoutes:
    """Handles route registration for auth system"""

    def __init__(self, auth_manager):
        self.auth = auth_manager
        self.routes = {}

    def register_all(
        self, app, prefix="/auth", include_admin=False, allow_custom_login=False
    ):
        """Register all auth routes with optional admin interface"""
        rt = app.route

        # Handle success/error messages for admin operations
        if include_admin:
            # Import AdminRoutes only when needed
            from .admin_routes import AdminRoutes

            admin_handler = AdminRoutes(self.auth)

        # Register each route group
        # Only register default login routes if custom login is not allowed
        if allow_custom_login:
            # Skip login routes to allow custom implementation
            self._register_logout_route(rt, prefix)
            self._register_profile_route(rt, prefix)
        else:
            # Register default login routes
            self._register_login_routes(rt, prefix)
            self._register_logout_route(rt, prefix)
            self._register_profile_route(rt, prefix)

        if self.auth.config.get("allow_registration"):
            self._register_registration_routes(rt, prefix)

        if self.auth.config.get("allow_password_reset"):
            self._register_password_reset_routes(rt, prefix)

        # Register admin routes if requested
        if include_admin:
            admin_routes = admin_handler.register_admin_routes(app, f"{prefix}/admin")
            self.routes.update(admin_routes)

            # Add admin dashboard route
            @rt(f"{prefix}/admin")
            @self.auth.require_admin()
            def admin_dashboard(req):

                # Get user statistics
                user_counts = self.auth.user_repo.count_by_role()
                total_users = sum(user_counts.values())

                return Title("Admin Dashboard"), Container(
                    DivFullySpaced(
                        H1("Admin Dashboard"),
                        A("← Back to Main Dashboard", href="/", cls=ButtonT.secondary),
                    ),
                    Grid(
                        Card(
                            CardHeader(H3("Total Users")),
                            CardBody(
                                H2(str(total_users), cls="text-3xl font-bold"),
                                P("Registered users", cls="text-muted-foreground"),
                            ),
                        ),
                        Card(
                            CardHeader(H3("Admins")),
                            CardBody(
                                H2(
                                    str(user_counts.get("admin", 0)),
                                    cls="text-3xl font-bold text-purple-600",
                                ),
                                P("Admin accounts", cls="text-muted-foreground"),
                            ),
                        ),
                        Card(
                            CardHeader(H3("Managers")),
                            CardBody(
                                H2(
                                    str(user_counts.get("manager", 0)),
                                    cls="text-3xl font-bold text-blue-600",
                                ),
                                P("Manager accounts", cls="text-muted-foreground"),
                            ),
                        ),
                        Card(
                            CardHeader(H3("Users")),
                            CardBody(
                                H2(
                                    str(user_counts.get("user", 0)),
                                    cls="text-3xl font-bold text-gray-600",
                                ),
                                P("Regular users", cls="text-muted-foreground"),
                            ),
                        ),
                        cols=1,
                        cols_md=2,
                        cols_lg=4,
                    ),
                    Card(
                        CardHeader(H3("Quick Actions")),
                        CardBody(
                            Grid(
                                A(
                                    "Manage Users",
                                    href=f"{prefix}/admin/users",
                                    cls=ButtonT.primary,
                                ),
                                A(
                                    "Create New User",
                                    href=f"{prefix}/admin/users/create",
                                    cls=ButtonT.secondary,
                                ),
                                A(
                                    "View Profile",
                                    href=f"{prefix}/profile",
                                    cls=ButtonT.secondary,
                                ),
                                A(
                                    "System Settings",
                                    href="#",
                                    cls=ButtonT.secondary
                                    + " opacity-50 cursor-not-allowed",
                                ),
                                cols=2,
                                cols_md=4,
                            )
                        ),
                        cls="mt-6",
                    ),
                    cls=ContainerT.xl,
                )

            self.routes["admin_dashboard"] = admin_dashboard

            # Update the user list route to show success/error messages
            original_list = self.routes.get("admin_users_list")
            if original_list:

                @rt(f"{prefix}/admin/users")
                @self.auth.require_admin()
                def admin_users_list_with_messages(req):
                    from .forms import create_message_alert

                    # Check for success/error messages
                    success = req.query_params.get("success")
                    error = req.query_params.get("error")

                    success_messages = {
                        "created": "User created successfully!",
                        "updated": "User updated successfully!",
                        "deleted": "User deleted successfully!",
                    }

                    error_messages = {
                        "user_not_found": "User not found.",
                        "cannot_delete_self": "You cannot delete your own account.",
                        "delete_failed": "Failed to delete user.",
                        "update_failed": "Failed to update user.",
                    }

                    # Get the original response
                    original_response = original_list(req)

                    # If there's a message, add it to the response
                    if success or error:
                        message = None
                        if success:
                            message = create_message_alert(
                                success_messages.get(success), "success"
                            )
                        elif error:
                            message = create_message_alert(
                                error_messages.get(error), "error"
                            )

                        # Insert message after the header
                        if message and hasattr(original_response, "__iter__"):
                            response_list = list(original_response)
                            # Find Container and insert message after header
                            for i, item in enumerate(response_list):
                                if hasattr(item, "children"):
                                    children = list(item.children)
                                    children.insert(1, message)  # Insert after header
                                    item.children = children
                                    break
                            return tuple(response_list)

                    return original_response

                self.routes["admin_users_list"] = admin_users_list_with_messages

    def _register_login_routes(self, rt, prefix):
        """Register login routes"""

        # Login routes
        @rt(f"{prefix}/login", methods=["GET"])
        def login_page(req):
            error = req.query_params.get("error")
            # Get redirect destination from query params
            redirect_to = req.query_params.get("redirect_to", "/")
            return Title("Login"), Container(
                create_login_form(
                    error=error, action=f"{prefix}/login", redirect_to=redirect_to
                )
            )

        self.routes["login_page"] = login_page

        @rt(f"{prefix}/login", methods=["POST"])
        async def login_submit(req, sess):
            form = await req.form()
            username = form.get("username", "").strip()
            password = form.get("password", "")
            remember_me = form.get("remember_me") == "on"

            # Authenticate
            user = self.auth.user_repo.authenticate(username, password)

            if user:
                # Set session
                sess["auth"] = user.username
                sess["user_id"] = user.id
                sess["role"] = user.role

                redirect_url = form.get("redirect_to", "/")
                response = RedirectResponse(redirect_url, status_code=303)

                if remember_me:
                    # Set a long-lived cookie (30 days)
                    response.set_cookie(
                        key="remember_user",
                        value=user.username,
                        max_age=30 * 24 * 60 * 60,  # 30 days in seconds
                        httponly=True,
                        samesite="strict",
                    )
                    sess["remember_me"] = True
                else:
                    # Remove remember me cookie if it exists
                    response.delete_cookie("remember_user")
                    sess.pop("remember_me", None)

                return response

            # On failure, preserve the redirect_to parameter
            redirect_to = form.get("redirect_to", "/")
            error_url = f"{prefix}/login?error=invalid"
            if redirect_to != "/":
                error_url += f"&redirect_to={redirect_to}"
            return RedirectResponse(error_url, status_code=303)

        self.routes["login_submit"] = login_submit

    def _register_logout_route(self, rt, prefix):
        @rt(f"{prefix}/logout")
        def logout(sess):
            sess.clear()
            return RedirectResponse(f"{prefix}/login", status_code=303)

        self.routes["logout"] = logout

    def _register_registration_routes(self, rt, prefix):

        # Optional: Register route
        if self.auth.config.get("allow_registration", False):

            @rt(f"{prefix}/register", methods=["GET"])
            def register_page(req):
                error = req.query_params.get("error")
                return Title("Register"), Container(
                    create_register_form(error=error, action=f"{prefix}/register")
                )

            @rt(f"{prefix}/register", methods=["POST"])
            async def register_submit(req, sess):
                form = await req.form()
                username = form.get("username", "").strip()
                email = form.get("email", "").strip()
                password = form.get("password", "")
                confirm = form.get("confirm_password", "")
                accept_terms = form.get("accept_terms") == "on"

                if not accept_terms:
                    return RedirectResponse(
                        f"{prefix}/register?error=terms_required", status_code=303
                    )

                # Validation
                if password != confirm:
                    return RedirectResponse(
                        f"{prefix}/register?error=password_mismatch", status_code=303
                    )

                if password != confirm:
                    return RedirectResponse(
                        f"{prefix}/register?error=password_mismatch", status_code=303
                    )

                # Check if user exists
                if self.auth.user_repo.get_by_username(username):
                    return RedirectResponse(
                        f"{prefix}/register?error=username_taken", status_code=303
                    )

                # Create user
                try:
                    user = self.auth.user_repo.create(username, email, password)
                    if user:
                        # Auto-login after registration
                        sess["auth"] = user.username
                        sess["user_id"] = user.id
                        sess["role"] = user.role
                        return RedirectResponse("/", status_code=303)
                except Exception as e:
                    print(f"Registration error: {e}")
                    return RedirectResponse(
                        f"{prefix}/register?error=creation_failed", status_code=303
                    )

                return RedirectResponse(
                    f"{prefix}/register?error=creation_failed", status_code=303
                )

            self.routes["register_page"] = register_page
            self.routes["register_submit"] = register_submit

    def _register_password_reset_routes(self, rt, prefix):
        # Optional: Password reset route
        if self.auth.config.get("allow_password_reset", False):

            @rt(f"{prefix}/forgot", methods=["GET"])
            def forgot_page(req):
                error = req.query_params.get("error")
                success = req.query_params.get("success")
                return Title("Forgot Password"), create_forgot_password_form(
                    error=error, success=success, action=f"{prefix}/forgot"
                )

            @rt(f"{prefix}/forgot", methods=["POST"])
            async def forgot_submit(req):
                form = await req.form()
                email = form.get("email", "").strip()

                # TODO: Implement actual password reset logic
                # For now, just show success message
                return RedirectResponse(
                    f"{prefix}/forgot?success=sent", status_code=303
                )

            self.routes["forgot_password"] = forgot_page
            self.routes["forgot_submit"] = forgot_submit

    def _register_profile_route(self, rt, prefix):
        # Register route to a profile form
        @rt(f"{prefix}/profile", methods=["GET"])
        def profile_page(req):
            user = req.scope["user"]  # Added by beforeware
            success = req.query_params.get("success")
            error = req.query_params.get("error")
            return Title("Profile"), create_profile_form(
                user=user, success=success, error=error, action=f"{prefix}/profile"
            )

        @rt(f"{prefix}/profile", methods=["POST"])
        async def profile_submit(req):
            user = req.scope["user"]
            form = await req.form()

            try:
                # Update email if changed
                new_email = form.get("email", "").strip()
                if new_email and new_email != user.email:
                    self.auth.user_repo.update(user.id, email=new_email)

                # Handle password change
                current_password = form.get("current_password", "")
                new_password = form.get("new_password", "")
                confirm_password = form.get("confirm_password", "")

                if current_password or new_password:
                    if not current_password:
                        return RedirectResponse(
                            f"{prefix}/profile?error=Current password required",
                            status_code=303,
                        )

                    if not self.auth.user_repo.verify_password(
                        current_password, user.password
                    ):
                        return RedirectResponse(
                            f"{prefix}/profile?error=Invalid current password",
                            status_code=303,
                        )

                    if new_password != confirm_password:
                        return RedirectResponse(
                            f"{prefix}/profile?error=New passwords do not match",
                            status_code=303,
                        )

                    if len(new_password) < 8:
                        return RedirectResponse(
                            f"{prefix}/profile?error=Password must be at least 8 characters",
                            status_code=303,
                        )

                    # Update password (repository will handle hashing)
                    self.auth.user_repo.update(user.id, password=new_password)

                return RedirectResponse(f"{prefix}/profile?success=1", status_code=303)

            except Exception as e:
                print(f"Profile update error: {e}")
                return RedirectResponse(
                    f"{prefix}/profile?error=Update failed", status_code=303
                )

        self.routes["profile_page"] = profile_page
        self.routes["profile_submit"] = profile_submit
