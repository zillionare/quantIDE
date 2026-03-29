# auth/forms.py
from fasthtml.common import *
from monsterui.all import *


def _login_error_message(error):
    messages = {
        "invalid": "账号或密码错误，请重新输入。",
        "inactive": "当前账号已停用，请联系管理员。",
        "system": "系统暂时不可用，请稍后再试。",
    }
    return messages.get(error)


def _login_field(label_cn, label_en, field_id, name, placeholder, field_type="text"):
    return Div(
        Label(
            f"{label_cn}/{label_en}",
            fr=field_id,
            cls="qt-login-label",
        ),
        Input(
            id=field_id,
            name=name,
            type=field_type,
            placeholder=placeholder,
            required=True,
            autofocus=field_id == "username",
            cls="qt-login-input",
        ),
        cls="qt-login-field",
    )


def _brand_emblem():
    return Div(
        Div(
            Span("匡", cls="qt-emblem-char qt-emblem-char-top"),
            Span("醍", cls="qt-emblem-char qt-emblem-char-bottom"),
            cls="qt-emblem-text",
        ),
        Div(cls="qt-emblem-eye-top"),
        Div(cls="qt-emblem-eye-bottom"),
        cls="qt-emblem-yinyang",
    )


def create_login_form(error=None, action="/auth/login", redirect_to="/"):
    """Create branded login form while preserving auth behavior."""
    error_message = _login_error_message(error)

    return Div(
        Style(
            """
            .qt-login-page {
                min-height: 100vh;
                display: flex;
                align-items: center;
                justify-content: center;
                padding: 24px;
                background: #ffffff;
                font-family: 'PingFang SC', 'Hiragino Sans GB', 'Microsoft YaHei', sans-serif;
            }
            .qt-login-card {
                position: relative;
                display: grid;
                grid-template-columns: minmax(220px, 0.9fr) minmax(300px, 1fr);
                width: min(640px, 100%);
                min-height: 380px;
                border-radius: 0 24px 0 24px;
                overflow: hidden;
                background: #ffffff;
                box-shadow: 0 3px 5px rgba(0, 0, 0, 0.14);
                border: 1px solid rgba(131, 108, 76, 0.12);
            }
            .qt-login-brand {
                background: linear-gradient(180deg, #c90505 0%, #dc0909 100%);
                color: white;
                padding: 30px 24px 24px;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
                position: relative;
            }
            .qt-brand-mark {
                display: flex;
                flex-direction: column;
                align-items: center;
                gap: 10px;
                margin-top: 2px;
            }
            .qt-brand-logo {
                width: 108px;
                max-width: 72%;
                height: auto;
                display: block;
            }
            .qt-brand-logo-text {
                font-size: 32px;
                font-weight: bold;
                color: white;
                letter-spacing: 0.1em;
                text-align: center;
            }
            .qt-brand-divider {
                width: 140px;
                height: 1px;
                background: rgba(255, 248, 231, 0.8);
            }
            .qt-brand-tagline {
                font-size: 13px;
                letter-spacing: 0.12em;
            }
            .qt-brand-slogan {
                align-self: center;
                text-align: center;
                font-size: 14px;
                line-height: 1.6;
                letter-spacing: 0.08em;
                font-style: italic;
                color: rgba(255, 248, 231, 0.9);
                padding: 10px 0;
                width: 100%;
                max-width: 260px;
            }
            .qt-brand-footer {
                display: flex;
                flex-direction: column;
                align-items: center;
                gap: 6px;
                text-align: center;
                font-size: 11px;
                letter-spacing: 0.12em;
            }
            .qt-brand-contact {
                font-size: 10px;
                letter-spacing: 0;
            }
            .qt-login-panel {
                background: #fff;
                padding: 40px 38px 30px 66px;
                display: flex;
                flex-direction: column;
                justify-content: center;
            }
            .qt-login-title {
                font-size: 28px;
                color: #2b231c;
                letter-spacing: 0.08em;
                margin-bottom: 28px;
                font-weight: 600;
            }
            .qt-login-form {
                display: flex;
                flex-direction: column;
                gap: 16px;
            }
            .qt-login-field {
                display: flex;
                flex-direction: column;
                gap: 6px;
            }
            .qt-login-label {
                color: #8a8278;
                font-size: 12px;
                letter-spacing: 0.03em;
            }
            .qt-login-input {
                width: 100%;
                height: 40px;
                border: none;
                background: #f0ede8;
                color: #2d241d;
                padding: 0 12px;
                font-size: 14px;
                border-radius: 0;
                box-shadow: inset 0 0 0 1px rgba(86, 69, 54, 0.05);
            }
            .qt-login-input::placeholder {
                color: #c2bbb0;
            }
            .qt-login-input:focus {
                outline: 2px solid rgba(201, 5, 5, 0.25);
                outline-offset: 2px;
            }
            .qt-login-remember {
                display: flex;
                align-items: center;
                gap: 8px;
                color: #6c6258;
                font-size: 12px;
                margin-top: 2px;
            }
            .qt-login-remember input {
                width: 14px;
                height: 14px;
                accent-color: #c90505;
            }
            .qt-login-error {
                background: rgba(201, 5, 5, 0.08);
                color: #9d1111;
                border-left: 4px solid #c90505;
                padding: 8px 10px;
                font-size: 12px;
            }
            .qt-login-button {
                margin-top: 10px;
                height: 42px;
                border: none;
                background: linear-gradient(180deg, #cd0505 0%, #b90303 100%);
                color: #fff6ef;
                font-size: 18px;
                letter-spacing: 0.3em;
                text-indent: 0.3em;
                cursor: pointer;
                border-radius: 4px;
                box-shadow: 0 2px 5px rgba(201, 5, 5, 0.25);
                transition: box-shadow 0.2s ease, transform 0.1s ease;
            }
            .qt-login-button:hover {
                box-shadow: 0 6px 16px rgba(201, 5, 5, 0.35);
            }
            .qt-login-button:active {
                transform: translateY(1px);
                box-shadow: 0 2px 8px rgba(201, 5, 5, 0.25);
            }
            .qt-emblem-yinyang {
                position: absolute;
                left: 47%;
                top: 45%;
                transform: translate(-50%, -50%);
                width: 46px;
                height: 46px;
                border-radius: 50%;
                z-index: 3;
                overflow: hidden;
                background:
                    linear-gradient(90deg, #121212 0 50%, #c90505 50% 100%);
                box-shadow: 0 2px 8px rgba(0, 0, 0, 0.4);
            }
            .qt-emblem-yinyang::before,
            .qt-emblem-yinyang::after {
                content: '';
                position: absolute;
                left: 50%;
                transform: translateX(-50%);
                width: 23px;
                height: 23px;
                border-radius: 50%;
            }
            .qt-emblem-yinyang::before {
                top: 0;
                background: #121212;
            }
            .qt-emblem-yinyang::after {
                bottom: 0;
                background: #c90505;
            }
            .qt-emblem-eye-top {
                position: absolute;
                left: 50%;
                transform: translateX(-50%);
                width: 4px;
                height: 4px;
                border-radius: 50%;
                background: #ffffff;
                z-index: 4;
                top: 11px;
            }
            .qt-emblem-eye-bottom {
                position: absolute;
                left: 50%;
                transform: translateX(-50%);
                width: 4px;
                height: 4px;
                border-radius: 50%;
                background: #121212;
                z-index: 4;
                bottom: 11px;
            }
            .qt-emblem-text {
                position: absolute;
                inset: 0;
                # display: flex;
                display: none;
                flex-direction: column;
                align-items: center;
                justify-content: center;
                font-weight: 700;
                letter-spacing: 0.04em;
            }
            .qt-emblem-char {
                display: block;
                line-height: 1;
                font-size: 20px;
                margin: 8px 0;
            }
            .qt-emblem-char-top {
                color: #c90505;
            }
            .qt-emblem-char-bottom {
                color: #ffffff;
                z-index: 2;
            }
            @media (max-width: 900px) {
                .qt-login-page {
                    padding: 18px;
                }
                .qt-login-card {
                    grid-template-columns: 1fr;
                    min-height: auto;
                    width: min(360px, 100%);
                    border-radius: 0 18px 0 18px;
                }
                .qt-login-brand {
                    padding: 28px 22px 84px;
                    min-height: 240px;
                }
                .qt-brand-slogan {
                    font-size: 16px;
                    max-width: 220px;
                }
                .qt-login-panel {
                    padding: 72px 24px 28px;
                }
                .qt-login-title {
                    font-size: 24px;
                    margin-bottom: 20px;
                    text-align: center;
                }
                .qt-emblem-yinyang {
                    left: 50%;
                    top: 240px;
                    transform: translate(-50%, -50%);
                    width: 82px;
                    height: 82px;
                }
            }
            """
        ),
        Div(
            Div(
                Div(
                    Div(
                        Div("匡醍量化", cls="qt-brand-logo-text"),
                        Div(cls="qt-brand-divider"),
                        Div("开启财富之门", cls="qt-brand-tagline"),
                        cls="qt-brand-mark",
                    ),
                    Div("量化软件 · 策略 · 课程", cls="qt-brand-slogan"),
                    Div(
                        Div("匡醍（武汉）信息技术有限责任公司"),
                        Div("商务洽谈: business@quantide.cn", cls="qt-brand-contact"),
                        cls="qt-brand-footer",
                    ),
                    cls="qt-login-brand",
                ),
                Div(
                    H2("大富翁智能交易", cls="qt-login-title"),
                    Form(
                        Input(type="hidden", name="redirect_to", value=redirect_to),
                        Alert(error_message, cls="qt-login-error") if error_message else None,
                        _login_field("账号", "Account", "username", "username", "请输入账户"),
                        _login_field("密码", "Password", "password", "password", "请输入密码", field_type="password"),
                        Label(
                            Input(type="checkbox", name="remember_me", value="on"),
                            Span("记住我/Remember me"),
                            cls="qt-login-remember",
                        ),
                        Button("登录", type="submit", cls="qt-login-button"),
                        method="post",
                        action=action,
                        cls="qt-login-form",
                    ),
                    cls="qt-login-panel",
                ),
                _brand_emblem(),
                cls="qt-login-card",
            ),
            cls="qt-login-page",
        ),
    )


def create_register_form(error=None, action="/auth/register"):
    """Create registration form component with same style as login"""
    error_message = None
    if error == "username_taken":
        error_message = "Username already taken. Please choose another."
    elif error == "email_taken":
        error_message = "Email already registered. Please sign in or use another email."
    elif error == "password_mismatch":
        error_message = "Passwords do not match. Please try again."
    elif error == "password_weak":
        error_message = "Password must be at least 8 characters long."
    elif error == "invalid_email":
        error_message = "Please enter a valid email address."
    elif error == "creation_failed":
        error_message = "Failed to create account. Please try again."
    elif error == "terms_required":  # NEW ERROR MESSAGE
        error_message = "You must accept the Terms and Conditions to register."

    return DivCentered(
        Card(
            CardHeader(
                H3("Create Account", cls="text-center"),
                Subtitle("Join us today", cls="text-center"),
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
                        title="3-20 characters, letters, numbers and underscore only",
                    ),
                    LabelInput(
                        "Email",
                        id="email",
                        name="email",
                        type="email",
                        placeholder="your@email.com",
                        required=True,
                    ),
                    LabelInput(
                        "Password",
                        id="password",
                        name="password",
                        type="password",
                        placeholder="Choose a strong password (8+ characters)",
                        required=True,
                        minlength=8,
                    ),
                    LabelInput(
                        "Confirm Password",
                        id="confirm_password",
                        name="confirm_password",
                        type="password",
                        placeholder="Re-enter your password",
                        required=True,
                        minlength=8,
                    ),
                    Div(
                        Label(
                            CheckboxX(
                                name="accept_terms", selected=False, required=True
                            ),
                            Span(" I accept the Terms and Conditions", cls="ml-2"),
                            cls="flex items-center text-sm cursor-pointer",
                        ),
                        cls="mb-4",
                    ),
                    Button(
                        "Create Account", type="submit", cls=(ButtonT.primary, "w-full")
                    ),
                    method="post",
                    action=action,
                ),
            ),
            footer=DivCentered(
                P(
                    "Already have an account? ",
                    A("Sign In", href="/auth/login", cls="font-medium hover:underline"),
                    cls="text-sm text-muted-foreground",
                ),
                cls="p-4",
            ),
            cls="w-full max-w-md shadow-lg",
        ),
        cls="min-h-screen flex items-center justify-center p-4",
    )


def create_forgot_password_form(error=None, success=None, action="/auth/forgot"):
    """Create password reset request form with same style"""
    error_message = None
    success_message = None

    if error == "user_not_found":
        error_message = "No account found with that email address."
    elif error == "send_failed":
        error_message = "Failed to send reset email. Please try again."

    if success == "sent":
        success_message = "Password reset instructions have been sent to your email."

    return DivCentered(
        Card(
            CardHeader(
                H3("Reset Password", cls="text-center"),
                Subtitle("We'll send you reset instructions", cls="text-center"),
            ),
            CardBody(
                Alert(error_message, cls=AlertT.error) if error_message else None,
                Alert(success_message, cls=AlertT.success) if success_message else None,
                (
                    Form(
                        LabelInput(
                            "Email Address",
                            id="email",
                            name="email",
                            type="email",
                            placeholder="your@email.com",
                            required=True,
                            autofocus=True,
                        ),
                        Button(
                            "Send Reset Link",
                            type="submit",
                            cls=(ButtonT.primary, "w-full"),
                        ),
                        method="post",
                        action=action,
                    )
                    if not success_message
                    else Div(
                        P(
                            "Check your email for further instructions.",
                            cls="text-center text-muted-foreground",
                        )
                    )
                ),
                DivCentered(
                    A(
                        "← Back to Sign In",
                        href="/auth/login",
                        cls=(ButtonT.secondary, "mt-4"),
                    )
                ),
            ),
            cls="w-full max-w-md shadow-lg",
        ),
        cls="min-h-screen flex items-center justify-center p-4",
    )


def create_reset_password_form(token, error=None, action="/auth/reset"):
    """Create password reset form (after clicking email link)"""
    error_message = None

    if error == "invalid_token":
        error_message = "Invalid or expired reset token. Please request a new one."
    elif error == "password_mismatch":
        error_message = "Passwords do not match. Please try again."
    elif error == "password_weak":
        error_message = "Password must be at least 8 characters long."

    return DivCentered(
        Card(
            CardHeader(
                H3("Choose New Password", cls="text-center"),
                Subtitle("Enter your new password below", cls="text-center"),
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
                        autofocus=True,
                    ),
                    LabelInput(
                        "Confirm New Password",
                        id="confirm_password",
                        name="confirm_password",
                        type="password",
                        placeholder="Re-enter new password",
                        required=True,
                        minlength=8,
                    ),
                    Button(
                        "Reset Password", type="submit", cls=(ButtonT.primary, "w-full")
                    ),
                    method="post",
                    action=action,
                ),
            ),
            footer=DivCentered(
                P(
                    "Remember your password? ",
                    A("Sign In", href="/auth/login", cls="font-medium hover:underline"),
                    cls="text-sm text-muted-foreground",
                ),
                cls="p-4",
            ),
            cls="w-full max-w-md shadow-lg",
        ),
        cls="min-h-screen flex items-center justify-center p-4",
    )


def create_profile_form(user, success=None, error=None, action="/auth/profile"):
    """Create profile edit form for logged-in users"""
    return Container(
        DivFullySpaced(
            H1("Profile Settings"),
            A("← Back to Dashboard", href="/", cls=ButtonT.secondary),
        ),
        Grid(
            Card(
                CardHeader(H3("Account Information")),
                CardBody(
                    (
                        Alert("Profile updated successfully!", cls=AlertT.success)
                        if success
                        else None
                    ),
                    Alert(error, cls=AlertT.error) if error else None,
                    Form(
                        Grid(
                            LabelInput(
                                "Username",
                                value=user.username,
                                disabled=True,
                                cls="bg-muted",
                            ),
                            LabelInput(
                                "Email",
                                name="email",
                                type="email",
                                value=user.email,
                                required=True,
                            ),
                            cols=1,
                            cols_md=2,
                        ),
                        Hr(cls="my-6"),
                        H4("Change Password", cls="text-lg font-semibold mb-4"),
                        LabelInput(
                            "Current Password",
                            name="current_password",
                            type="password",
                            placeholder="Enter current password to change",
                        ),
                        Grid(
                            LabelInput(
                                "New Password",
                                name="new_password",
                                type="password",
                                placeholder="Enter new password (8+ chars)",
                                minlength=8,
                            ),
                            LabelInput(
                                "Confirm New Password",
                                name="confirm_password",
                                type="password",
                                placeholder="Confirm new password",
                                minlength=8,
                            ),
                            cols=1,
                            cols_md=2,
                        ),
                        DivRAligned(
                            Button("Save Changes", type="submit", cls=ButtonT.primary),
                            cls="mt-6",
                        ),
                        method="post",
                        action=action,
                    ),
                ),
            ),
            Card(
                CardHeader(H3("Account Details")),
                CardBody(
                    Div(
                        InfoRow("Username", user.username),
                        InfoRow("Role", user.role.title()),
                        InfoRow("Status", "Active" if user.active else "Inactive"),
                        InfoRow(
                            "Member Since",
                            user.created_at[:10] if user.created_at else "Unknown",
                        ),
                        InfoRow(
                            "Last Login",
                            user.last_login[:10] if user.last_login else "Never",
                        ),
                        cls="space-y-3",
                    )
                ),
            ),
            cols=1,
            cols_lg=2,
        ),
        cls=ContainerT.xl,
    )


def InfoRow(label, value):
    """Helper for info display in profile"""
    return DivFullySpaced(
        Span(label, cls="font-medium"), Span(str(value), cls="text-muted-foreground")
    )


# Utility function for consistent error/success messaging
def create_message_alert(message, type="info"):
    """Create consistent alert messages"""
    alert_class = {
        "error": AlertT.error,
        "success": AlertT.success,
        "warning": AlertT.warning,
        "info": AlertT.info,
    }.get(type, AlertT.info)

    return Alert(message, cls=alert_class)
