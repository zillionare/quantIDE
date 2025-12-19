# auth/forms.py
from fasthtml.common import *
from monsterui.all import *


def create_login_form(error=None, action="/auth/login", redirect_to="/"):
    """Create login form component with consistent styling"""
    error_message = None
    if error == 'invalid':
        error_message = "Invalid username or password. Please try again."
    elif error == 'inactive':
        error_message = "Your account has been deactivated. Please contact support."
    elif error == 'system':
        error_message = "System error. Please try again."
    
    return DivCentered(
        Card(
            CardHeader(
                H3("Welcome Back", cls="text-center"),
                Subtitle("Sign in to your account", cls="text-center")
            ),
            CardBody(
                Alert(error_message, cls=AlertT.error) if error_message else None,
                Form(
                    LabelInput(
                        "Username",
                        id="username",
                        name="username",
                        placeholder="Enter your username",
                        required=True,
                        autofocus=True
                    ),
                    LabelInput(
                        "Password", 
                        id="password",
                        name="password",
                        type="password",
                        placeholder="Enter your password",
                        required=True
                    ),
                    Input(type="hidden", name="redirect_to", value=redirect_to),
                    
                    Div(
                        Label(
                            CheckboxX(name="remember_me", selected=False),
                            Span(" Remember me", cls="ml-2"),
                            cls="flex items-center text-sm cursor-pointer"
                        ),
                        cls="mb-4"
                    ),
                    
                    Button("Sign In", type="submit", cls=(ButtonT.primary, "w-full")),
                    
                    Div(
                        A("Forgot password?", href="/auth/forgot", 
                          cls="text-sm text-muted-foreground hover:underline"),
                        cls="text-center mt-4"
                    ),
                    
                    method="post",
                    action=action
                )
            ),
            footer=DivCentered(
                P("Don't have an account? ", 
                  A("Register", href="/auth/register", cls="font-medium hover:underline"),
                  cls="text-sm text-muted-foreground"),
                cls="p-4"
            ),
            cls="w-full max-w-md shadow-lg"
        ),
        cls="min-h-screen flex items-center justify-center p-4"
    )

def create_register_form(error=None, action="/auth/register"):
    """Create registration form component with same style as login"""
    error_message = None
    if error == 'username_taken':
        error_message = "Username already taken. Please choose another."
    elif error == 'email_taken':
        error_message = "Email already registered. Please sign in or use another email."
    elif error == 'password_mismatch':
        error_message = "Passwords do not match. Please try again."
    elif error == 'password_weak':
        error_message = "Password must be at least 8 characters long."
    elif error == 'invalid_email':
        error_message = "Please enter a valid email address."
    elif error == 'creation_failed':
        error_message = "Failed to create account. Please try again."
    elif error == 'terms_required':  # NEW ERROR MESSAGE
        error_message = "You must accept the Terms and Conditions to register."
    
    return DivCentered(
        Card(
            CardHeader(
                H3("Create Account", cls="text-center"),
                Subtitle("Join us today", cls="text-center")
            ),
            CardBody(
                Alert(error_message, cls=AlertT.error) if error_message else None,
                Form(
                    LabelInput(
                        "Username",
                        id="username",
                        name="username",
                        placeholder="Choose a username",
                        required=True,
                        autofocus=True,
                        pattern="[a-zA-Z0-9_]{3,20}",
                        title="3-20 characters, letters, numbers and underscore only"
                    ),
                    
                    LabelInput(
                        "Email",
                        id="email",
                        name="email",
                        type="email",
                        placeholder="your@email.com",
                        required=True
                    ),
                    
                    LabelInput(
                        "Password", 
                        id="password",
                        name="password",
                        type="password",
                        placeholder="Choose a strong password (8+ characters)",
                        required=True,
                        minlength=8
                    ),
                    
                    LabelInput(
                        "Confirm Password", 
                        id="confirm_password",
                        name="confirm_password",
                        type="password",
                        placeholder="Re-enter your password",
                        required=True,
                        minlength=8
                    ),
                    
                    Div(
                        Label(
                            CheckboxX(name="accept_terms", selected=False, required=True),
                            Span(" I accept the Terms and Conditions", cls="ml-2"),
                            cls="flex items-center text-sm cursor-pointer"
                        ),
                        cls="mb-4"
                    ),
                    
                    Button("Create Account", type="submit", cls=(ButtonT.primary, "w-full")),
                    
                    method="post",
                    action=action
                )
            ),
            footer=DivCentered(
                P("Already have an account? ", 
                  A("Sign In", href="/auth/login", cls="font-medium hover:underline"),
                  cls="text-sm text-muted-foreground"),
                cls="p-4"
            ),
            cls="w-full max-w-md shadow-lg"
        ),
        cls="min-h-screen flex items-center justify-center p-4"
    )

def create_forgot_password_form(error=None, success=None, action="/auth/forgot"):
    """Create password reset request form with same style"""
    error_message = None
    success_message = None
    
    if error == 'user_not_found':
        error_message = "No account found with that email address."
    elif error == 'send_failed':
        error_message = "Failed to send reset email. Please try again."
    
    if success == 'sent':
        success_message = "Password reset instructions have been sent to your email."
    
    return DivCentered(
        Card(
            CardHeader(
                H3("Reset Password", cls="text-center"),
                Subtitle("We'll send you reset instructions", cls="text-center")
            ),
            CardBody(
                Alert(error_message, cls=AlertT.error) if error_message else None,
                Alert(success_message, cls=AlertT.success) if success_message else None,
                
                Form(
                    LabelInput(
                        "Email Address",
                        id="email",
                        name="email",
                        type="email",
                        placeholder="your@email.com",
                        required=True,
                        autofocus=True
                    ),
                    
                    Button("Send Reset Link", type="submit", cls=(ButtonT.primary, "w-full")),
                    
                    method="post",
                    action=action
                ) if not success_message else Div(
                    P("Check your email for further instructions.", cls="text-center text-muted-foreground")
                ),
                
                DivCentered(
                    A("← Back to Sign In", href="/auth/login", 
                      cls=(ButtonT.secondary, "mt-4"))
                )
            ),
            cls="w-full max-w-md shadow-lg"
        ),
        cls="min-h-screen flex items-center justify-center p-4"
    )

def create_reset_password_form(token, error=None, action="/auth/reset"):
    """Create password reset form (after clicking email link)"""
    error_message = None
    
    if error == 'invalid_token':
        error_message = "Invalid or expired reset token. Please request a new one."
    elif error == 'password_mismatch':
        error_message = "Passwords do not match. Please try again."
    elif error == 'password_weak':
        error_message = "Password must be at least 8 characters long."
    
    return DivCentered(
        Card(
            CardHeader(
                H3("Choose New Password", cls="text-center"),
                Subtitle("Enter your new password below", cls="text-center")
            ),
            CardBody(
                Alert(error_message, cls=AlertT.error) if error_message else None,
                
                Form(
                    Input(type="hidden", name="token", value=token),
                    
                    LabelInput(
                        "New Password", 
                        id="password",
                        name="password",
                        type="password",
                        placeholder="Enter new password (8+ characters)",
                        required=True,
                        minlength=8,
                        autofocus=True
                    ),
                    
                    LabelInput(
                        "Confirm New Password", 
                        id="confirm_password",
                        name="confirm_password",
                        type="password",
                        placeholder="Re-enter new password",
                        required=True,
                        minlength=8
                    ),
                    
                    Button("Reset Password", type="submit", cls=(ButtonT.primary, "w-full")),
                    
                    method="post",
                    action=action
                )
            ),
            footer=DivCentered(
                P("Remember your password? ", 
                  A("Sign In", href="/auth/login", cls="font-medium hover:underline"),
                  cls="text-sm text-muted-foreground"),
                cls="p-4"
            ),
            cls="w-full max-w-md shadow-lg"
        ),
        cls="min-h-screen flex items-center justify-center p-4"
    )

def create_profile_form(user, success=None, error=None, action="/auth/profile"):
    """Create profile edit form for logged-in users"""
    return Container(
        DivFullySpaced(
            H1("Profile Settings"),
            A("← Back to Dashboard", href="/", cls=ButtonT.secondary)
        ),
        
        Grid(
            Card(
                CardHeader(H3("Account Information")),
                CardBody(
                    Alert("Profile updated successfully!", cls=AlertT.success) if success else None,
                    Alert(error, cls=AlertT.error) if error else None,
                    
                    Form(
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
                        
                        LabelInput(
                            "Current Password",
                            name="current_password",
                            type="password",
                            placeholder="Enter current password to change"
                        ),
                        
                        Grid(
                            LabelInput(
                                "New Password",
                                name="new_password",
                                type="password",
                                placeholder="Enter new password (8+ chars)",
                                minlength=8
                            ),
                            LabelInput(
                                "Confirm New Password",
                                name="confirm_password",
                                type="password",
                                placeholder="Confirm new password",
                                minlength=8
                            ),
                            cols=1, cols_md=2
                        ),
                        
                        DivRAligned(
                            Button("Save Changes", type="submit", cls=ButtonT.primary),
                            cls="mt-6"
                        ),
                        
                        method="post",
                        action=action
                    )
                )
            ),
            
            Card(
                CardHeader(H3("Account Details")),
                CardBody(
                    Div(
                        InfoRow("Username", user.username),
                        InfoRow("Role", user.role.title()),
                        InfoRow("Status", "Active" if user.active else "Inactive"),
                        InfoRow("Member Since", user.created_at[:10] if user.created_at else "Unknown"),
                        InfoRow("Last Login", user.last_login[:10] if user.last_login else "Never"),
                        cls="space-y-3"
                    )
                )
            ),
            
            cols=1, cols_lg=2
        ),
        cls=ContainerT.xl
    )

def InfoRow(label, value):
    """Helper for info display in profile"""
    return DivFullySpaced(
        Span(label, cls="font-medium"),
        Span(str(value), cls="text-muted-foreground")
    )

# Utility function for consistent error/success messaging
def create_message_alert(message, type="info"):
    """Create consistent alert messages"""
    alert_class = {
        "error": AlertT.error,
        "success": AlertT.success,
        "warning": AlertT.warning,
        "info": AlertT.info
    }.get(type, AlertT.info)
    
    return Alert(message, cls=alert_class)