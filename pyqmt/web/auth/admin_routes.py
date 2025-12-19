# auth/admin_routes.py
from fasthtml.common import *
from monsterui.all import *
import math  # For pagination calculations
from .forms import create_message_alert
from typing import Optional
import math

class AdminRoutes:
    """Admin interface routes for user management"""
    
    def __init__(self, auth_manager):
        self.auth = auth_manager
        self.routes = {}
        
    def register_admin_routes(self, app, prefix="/auth/admin"):
        """
        Register all admin routes with proper async handling
        
        IMPORTANT: Async route handlers are defined outside the class method because
        FastHTML has issues with async functions defined inside class methods. The async
        functions don't get properly awaited when defined inline, causing "coroutine never
        awaited" RuntimeWarnings and function objects being returned instead of executed.
        
        Solution: Define async handlers as local functions outside the class method scope,
        then register them with the router. This allows FastHTML to properly handle the
        async/await lifecycle.
        """
        rt = app.route
        
        # Store reference to auth manager for use in route handlers
        auth_manager = self.auth
        
        # === ASYNC HANDLERS DEFINED OUTSIDE CLASS METHOD ===
        # These must be defined here (not as class methods) for FastHTML async support
        
        async def handle_user_create(req):
            """Handle user creation form submission"""
            form = await req.form()
            
            username = form.get('username', '').strip()
            email = form.get('email', '').strip()
            password = form.get('password', '')
            confirm_password = form.get('confirm_password', '')
            role = form.get('role', 'user')
            active = form.get('active') == 'on'
            
            # Validation
            if not username or not email or not password:
                return RedirectResponse(f"{prefix}/users/create?error=missing_fields", status_code=303)
            
            if password != confirm_password:
                return RedirectResponse(f"{prefix}/users/create?error=password_mismatch", status_code=303)
            
            if len(password) < 8:
                return RedirectResponse(f"{prefix}/users/create?error=password_weak", status_code=303)
            
            # Check if user exists
            if auth_manager.user_repo.get_by_username(username):
                return RedirectResponse(f"{prefix}/users/create?error=username_taken", status_code=303)
            
            try:
                # Create user
                user = auth_manager.user_repo.create(
                    username=username,
                    email=email,
                    password=password,
                    role=role
                )
                
                # Update active status if needed
                if not active:
                    auth_manager.user_repo.update(user.id, active=False)
                
                return RedirectResponse(f"{prefix}/users?success=created", status_code=303)
                
            except Exception as e:
                print(f"Error creating user: {e}")
                return RedirectResponse(f"{prefix}/users/create?error=creation_failed", status_code=303)
        
        async def handle_user_edit(req, id: int):
            """Handle user edit form submission"""
            user = auth_manager.user_repo.get_by_id(id)
            if not user:
                return RedirectResponse(f"{prefix}/users?error=user_not_found", status_code=303)
            
            form = await req.form()
            
            # Get form data
            email = form.get('email', '').strip()
            role = form.get('role', 'user')
            active = form.get('active') == 'on'
            new_password = form.get('new_password', '')
            confirm_password = form.get('confirm_password', '')
            
            try:
                # Update basic fields
                update_fields = {
                    'email': email,
                    'role': role,
                    'active': active
                }
                
                # Handle password change if provided
                if new_password:
                    if new_password != confirm_password:
                        return RedirectResponse(f"{prefix}/users/edit?id={id}&error=password_mismatch", status_code=303)
                    
                    if len(new_password) < 8:
                        return RedirectResponse(f"{prefix}/users/edit?id={id}&error=password_weak", status_code=303)
                    
                    update_fields['password'] = new_password
                
                # Update user
                success = auth_manager.user_repo.update(id, **update_fields)
                
                if success:
                    return RedirectResponse(f"{prefix}/users?success=updated", status_code=303)
                else:
                    return RedirectResponse(f"{prefix}/users/edit?id={id}&error=update_failed", status_code=303)
                    
            except Exception as e:
                print(f"Error updating user: {e}")
                return RedirectResponse(f"{prefix}/users/edit?id={id}&error=update_failed", status_code=303)
        
        async def handle_user_delete(req, id: int):
            """Handle user deletion confirmation"""
            current_user = req.scope['user']
            
            # Prevent self-deletion
            if current_user.id == id:
                return RedirectResponse(f"{prefix}/users?error=cannot_delete_self", status_code=303)
            
            try:
                # Delete user
                success = auth_manager.user_repo.delete(id)
                
                if success:
                    return RedirectResponse(f"{prefix}/users?success=deleted", status_code=303)
                else:
                    return RedirectResponse(f"{prefix}/users?error=delete_failed", status_code=303)
                    
            except Exception as e:
                print(f"Error deleting user: {e}")
                return RedirectResponse(f"{prefix}/users?error=delete_failed", status_code=303)
        
        # === ROUTE REGISTRATIONS ===
        
        # User list with pagination and search - COMPLETE IMPLEMENTATION
        @rt(f"{prefix}/users")
        @self.auth.require_admin()
        def admin_users_list(req):
            # Get query parameters
            page = int(req.query_params.get('page', 1))
            per_page = int(req.query_params.get('per_page', 10))
            search = req.query_params.get('search', '')
            role_filter = req.query_params.get('role', '')
            status_filter = req.query_params.get('status', '')
            
            # Get all users
            all_users = self.auth.user_repo.list_all()
            
            # Apply filters
            filtered_users = self._filter_users(all_users, search, role_filter, status_filter)
            
            # Calculate pagination
            total_users = len(filtered_users)
            total_pages = math.ceil(total_users / per_page)
            start = (page - 1) * per_page
            end = start + per_page
            users_page = filtered_users[start:end]
            
            return Title("User Management"), Container(
                self._create_user_list_header(),
                self._create_filters_section(search, role_filter, status_filter, prefix),
                self._create_users_table(users_page, prefix),
                self._create_pagination(page, total_pages, req.url.path, req.query_params),
                cls=ContainerT.xl
            )
        
        # Create new user form (GET)
        @rt(f"{prefix}/users/create", methods=["GET"])
        @self.auth.require_admin()
        def admin_user_create_form(req):
            error = req.query_params.get('error')
            return Title("Create User"), Container(
                self._create_user_form(action=f"{prefix}/users/create", error=error, prefix=prefix),
                cls=ContainerT.lg
            )
        
        # Create new user submission (POST) - using external async handler
        rt(f"{prefix}/users/create", methods=["POST"])(handle_user_create)
        
        # Edit user form (GET)
        @rt(f"{prefix}/users/edit", methods=["GET"])
        @self.auth.require_admin()
        def admin_user_edit_form(req, id: int):
            user = self.auth.user_repo.get_by_id(id)
            if not user:
                return RedirectResponse(f"{prefix}/users?error=user_not_found", status_code=303)
            
            error = req.query_params.get('error')
            return Title(f"Edit User: {user.username}"), Container(
                self._create_edit_user_form(user, action=f"{prefix}/users/edit", error=error, prefix=prefix),
                cls=ContainerT.lg
            )
        
        # Edit user submission (POST) - using external async handler
        rt(f"{prefix}/users/edit", methods=["POST"])(handle_user_edit)
        
        # Delete user confirmation (GET)
        @rt(f"{prefix}/users/delete", methods=["GET"])
        @self.auth.require_admin()
        def admin_user_delete_confirm(req, id: int):
            current_user = req.scope['user']
            
            # Prevent self-deletion
            if current_user.id == id:
                return RedirectResponse(f"{prefix}/users?error=cannot_delete_self", status_code=303)
            
            user = self.auth.user_repo.get_by_id(id)
            if not user:
                return RedirectResponse(f"{prefix}/users?error=user_not_found", status_code=303)
            
            return Title(f"Delete User: {user.username}"), Container(
                self._create_delete_confirmation(user, prefix),
                cls=ContainerT.sm
            )
        
        # Delete user submission (POST) - using external async handler  
        rt(f"{prefix}/users/delete", methods=["POST"])(handle_user_delete)
        
        # Store route references
        self.routes.update({
            'admin_users_list': admin_users_list,
            'admin_user_create_form': admin_user_create_form,
            'admin_user_create_submit': handle_user_create,
            'admin_user_edit_form': admin_user_edit_form,
            'admin_user_edit_submit': handle_user_edit,
            'admin_user_delete_confirm': admin_user_delete_confirm,
            'admin_user_delete_submit': handle_user_delete
        })
        
        return self.routes



    # Helper methods for UI components
    def _create_user_list_header(self):
        """Create header for user list page"""
        return DivFullySpaced(
            H1("User Management"),
            Div(
                A("← Back to Dashboard", href="/", cls=ButtonT.secondary),
                " ",
                A("+ Create User", href="/auth/admin/users/create", cls=ButtonT.primary)
            )
        )
    
    def _create_filters_section(self, search, role_filter, status_filter, prefix):
        """Create filters section for user list"""
        return Card(
            CardBody(
                Form(
                    Grid(
                        Input(
                            type="search",
                            name="search",
                            placeholder="Search by username or email...",
                            value=search,
                            cls="w-full"
                        ),
                        Select(
                            Option("All Roles", value=""),
                            Option("User", value="user", selected=role_filter=="user"),
                            Option("Manager", value="manager", selected=role_filter=="manager"),
                            Option("Admin", value="admin", selected=role_filter=="admin"),
                            name="role",
                            cls="w-full"
                        ),
                        Select(
                            Option("All Status", value=""),
                            Option("Active", value="active", selected=status_filter=="active"),
                            Option("Inactive", value="inactive", selected=status_filter=="inactive"),
                            name="status",
                            cls="w-full"
                        ),
                        Button("Filter", type="submit", cls=ButtonT.primary),
                        cols=1, cols_md=2, cols_lg=4
                    ),
                    method="get",
                    action=f"{prefix}/users"
                )
            ),
            cls="mb-4"
        )
    
    def _create_users_table(self, users, prefix):
        """Create users table"""
        if not users:
            return Card(
                CardBody(
                    P("No users found.", cls="text-center text-muted-foreground py-8")
                )
            )
        
        rows = []
        for user in users:
            status_badge = Span(
                "Active" if user.active else "Inactive",
                cls=f"px-2 py-1 text-xs rounded {'bg-green-100 text-green-800' if user.active else 'bg-red-100 text-red-800'}"
            )
            
            role_badge = Span(
                user.role.title(),
                cls=f"px-2 py-1 text-xs rounded {self._get_role_color(user.role)}"
            )
            
            rows.append(
                Tr(
                    Td(user.username),
                    Td(user.email),
                    Td(role_badge),
                    Td(status_badge),
                    Td(user.created_at[:10] if user.created_at else "Unknown"),
                    Td(
                        Div(
                            A("Edit", href=f"{prefix}/users/edit?id={user.id}", cls="text-primary hover:underline mr-3"),
                            A("Delete", href=f"{prefix}/users/delete?id={user.id}", cls="text-destructive hover:underline"),
                            cls="flex"
                        )
                    )
                )
            )
        
        return Card(
            Table(
                Thead(
                    Tr(
                        Th("Username"),
                        Th("Email"),
                        Th("Role"),
                        Th("Status"),
                        Th("Created"),
                        Th("Actions")
                    )
                ),
                Tbody(*rows),
                cls="w-full"
            )
        )
    
    def _create_pagination(self, current_page, total_pages, base_url, query_params):
        """Create pagination controls"""
        if total_pages <= 1:
            return None
        
        # Build query string without page parameter
        params = {k: v for k, v in query_params.items() if k != 'page'}
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        base = f"{base_url}?{query_string}&page=" if query_string else f"{base_url}?page="
        
        pagination_items = []
        
        # Previous button
        if current_page > 1:
            pagination_items.append(
                A("← Previous", href=f"{base}{current_page-1}", cls=ButtonT.secondary)
            )
        
        # Page numbers
        for page in range(1, total_pages + 1):
            if page == current_page:
                pagination_items.append(
                    Span(str(page), cls="px-3 py-2 bg-primary text-primary-foreground rounded")
                )
            else:
                pagination_items.append(
                    A(str(page), href=f"{base}{page}", cls="px-3 py-2 hover:bg-muted rounded")
                )
        
        # Next button
        if current_page < total_pages:
            pagination_items.append(
                A("Next →", href=f"{base}{current_page+1}", cls=ButtonT.secondary)
            )
        
        return DivCentered(
            Div(*pagination_items, cls="flex gap-2 items-center"),
            cls="mt-4"
        )
    
    def _create_user_form(self, action, error=None, prefix="/auth/admin"):
        """Create new user form"""
        error_messages = {
            'missing_fields': "Please fill in all required fields.",
            'username_taken': "Username already exists.",
            'password_mismatch': "Passwords do not match.",
            'password_weak': "Password must be at least 8 characters.",
            'creation_failed': "Failed to create user. Please try again."
        }
        
        return Card(
            CardHeader(
                DivFullySpaced(
                    H2("Create New User"),
                    A("← Back to Users", href=f"{prefix}/users", cls=ButtonT.secondary)
                )
            ),
            CardBody(
                create_message_alert(error_messages.get(error), "error") if error else None,
                
                Form(
                    Grid(
                        LabelInput(
                            "Username",
                            name="username",
                            required=True,
                            placeholder="Enter username",
                            pattern="[a-zA-Z0-9_]{3,20}",
                            title="3-20 characters, letters, numbers and underscore only"
                        ),
                        LabelInput(
                            "Email",
                            name="email",
                            type="email",
                            required=True,
                            placeholder="user@example.com"
                        ),
                        cols=1, cols_md=2
                    ),
                    
                    Grid(
                        LabelInput(
                            "Password",
                            name="password",
                            type="password",
                            required=True,
                            placeholder="Min 8 characters",
                            minlength=8
                        ),
                        LabelInput(
                            "Confirm Password",
                            name="confirm_password",
                            type="password",
                            required=True,
                            placeholder="Re-enter password",
                            minlength=8
                        ),
                        cols=1, cols_md=2
                    ),
                    
                    Grid(
                        Div(
                            Label("Role", cls="block text-sm font-medium mb-2"),
                            Select(
                                Option("User", value="user"),
                                Option("Manager", value="manager"),
                                Option("Admin", value="admin"),
                                name="role",
                                cls="w-full"
                            )
                        ),
                        Div(
                            Label("Status", cls="block text-sm font-medium mb-2"),
                            Label(
                                CheckboxX(name="active", selected=True),
                                Span(" Active", cls="ml-2"),
                                cls="flex items-center cursor-pointer"
                            )
                        ),
                        cols=1, cols_md=2
                    ),
                    
                    DivRAligned(
                        Button("Create User", type="submit", cls=ButtonT.primary),
                        cls="mt-6"
                    ),
                    
                    method="post",
                    action=action
                )
            )
        )
    
    def _create_edit_user_form(self, user, action, error=None, prefix="/auth/admin"):
        """Create edit user form"""
        error_messages = {
            'password_mismatch': "Passwords do not match.",
            'password_weak': "Password must be at least 8 characters.",
            'update_failed': "Failed to update user. Please try again."
        }

        return Card(
            CardHeader(
                DivFullySpaced(
                    H2(f"Edit User: {user.username}"),
                    A("← Back to Users", href=f"{prefix}/users", cls=ButtonT.secondary)
                )
            ),
            CardBody(
                create_message_alert(error_messages.get(error), "error") if error else None,
                
                Form(
                    Input(type="hidden", name="id", value=user.id),
                    Grid(
                        LabelInput(
                            "Username",
                            value=user.username,
                            disabled=True,
                            cls="bg-muted"
                        ),
                        LabelInput(
                            "Email",
                            name="email",
                            type="email",
                            value=user.email,
                            required=True
                        ),
                        cols=1, cols_md=2
                    ),
                    
                    Hr(cls="my-6"),
                    
                    H4("Change Password", cls="text-lg font-semibold mb-4"),
                    P("Leave blank to keep current password", cls="text-sm text-muted-foreground mb-4"),
                    
                    Grid(
                        LabelInput(
                            "New Password",
                            name="new_password",
                            type="password",
                            placeholder="Min 8 characters"
                        ),
                        LabelInput(
                            "Confirm New Password",
                            name="confirm_password",
                            type="password",
                            placeholder="Re-enter new password"
                        ),
                        cols=1, cols_md=2
                    ),
                    
                    Hr(cls="my-6"),
                    
                    Grid(
                        Div(
                            Label("Role", cls="block text-sm font-medium mb-2"),
                            Select(
                                Option("User", value="user", selected=user.role=="user"),
                                Option("Manager", value="manager", selected=user.role=="manager"),
                                Option("Admin", value="admin", selected=user.role=="admin"),
                                name="role",
                                cls="w-full"
                            )
                        ),
                        Div(
                            Label("Status", cls="block text-sm font-medium mb-2"),
                            Label(
                                CheckboxX(name="active", checked=True if user.active else False),
                                Span(" Active", cls="ml-2"),
                                cls="flex items-center cursor-pointer"
                            )
                        ),
                        cols=1, cols_md=2
                    ),
                    
                    Card(
                        CardBody(
                            H4("User Information", cls="font-semibold mb-3"),
                            Grid(
                                InfoRow("User ID", user.id),
                                InfoRow("Created", user.created_at[:10] if user.created_at else "Unknown"),
                                InfoRow("Last Login", user.last_login[:10] if user.last_login else "Never"),
                                cols=1, cols_md=3
                            ),
                            cls="bg-muted/50"
                        ),
                        cls="mt-6"
                    ),
                    
                    DivRAligned(
                        Button("Save Changes", type="submit", cls=ButtonT.primary),
                        cls="mt-6"
                    ),
                    
                    method="post",
                    action=f"{action}?id={user.id}"
                )
            )
        )
    
    def _create_delete_confirmation(self, user, prefix):
        """Create delete confirmation dialog"""
        return Card(
            CardHeader(
                H2("Delete User", cls="text-destructive")
            ),
            CardBody(
                Alert(
                    Div(
                        P(Strong("Warning:"), " This action cannot be undone."),
                        P(f"You are about to delete user: ", Strong(user.username)),
                        P(f"Email: {user.email}"),
                        P(f"Role: {user.role.title()}"),
                        cls="space-y-2"
                    ),
                    cls=AlertT.error
                ),
                
                Form(
                    Input(type="hidden", name="id", value=user.id),
                    DivFullySpaced(
                        A("Cancel", href=f"{prefix}/users", cls=ButtonT.secondary),
                        Button("Delete User", type="submit", cls="bg-destructive text-destructive-foreground hover:bg-destructive/90")
                    ),
                    method="post",
                    action=f"{prefix}/users/delete?id={user.id}",
                    cls="mt-6"
                )
            )
        )
    
    def _filter_users(self, users, search, role_filter, status_filter):
        """Filter users based on search and filters"""
        filtered = users
        
        # Search filter
        if search:
            search_lower = search.lower()
            filtered = [u for u in filtered if 
                       search_lower in u.username.lower() or 
                       search_lower in u.email.lower()]
        
        # Role filter
        if role_filter:
            filtered = [u for u in filtered if u.role == role_filter]
        
        # Status filter
        if status_filter == 'active':
            filtered = [u for u in filtered if u.active]
        elif status_filter == 'inactive':
            filtered = [u for u in filtered if not u.active]
        
        return filtered
    
    def _get_role_color(self, role):
        """Get color class for role badge"""
        colors = {
            'admin': 'bg-purple-100 text-purple-800',
            'manager': 'bg-blue-100 text-blue-800',
            'user': 'bg-gray-100 text-gray-800'
        }
        return colors.get(role, 'bg-gray-100 text-gray-800')

def InfoRow(label, value):
    """Helper for info display"""
    return Div(
        P(label, cls="text-sm text-muted-foreground"),
        P(str(value), cls="font-medium")
    )