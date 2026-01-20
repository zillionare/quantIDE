from fasthtml.common import *
from monsterui.all import *

from .base import BaseLayout


class LoginLayout(BaseLayout):
    """登录页面布局，不包含header和sidebar"""

    def __init__(self, title: str = "用户登录 - PyQMT系统", error: str = None):
        super().__init__(title)
        self.error = error

    def main_block(self):
        """登录页面的主要内容，子类可以重写"""
        return DivCentered(
            Card(
                CardHeader(
                    H2("PyQMT系统登录", cls="text-center"),
                    P("请输入您的账户信息", cls="text-center text-muted-foreground"),
                ),
                CardBody(
                    Alert(self.error, cls=AlertT.error) if self.error else None,
                    Form(
                        LabelInput(
                            "用户名",
                            name="username",
                            placeholder="请输入用户名",
                            required=True,
                            autofocus=True,
                        ),
                        LabelInput(
                            "密码",
                            name="password",
                            type="password",
                            placeholder="请输入密码",
                            required=True,
                        ),
                        Button("登录", type="submit", cls=(ButtonT.primary, "w-full")),
                        method="post",
                        cls="space-y-4",
                    ),
                ),
                CardFooter(
                    DivCentered(
                        P(
                            "默认账户: admin / admin123",
                            cls="text-sm text-muted-foreground",
                        )
                    )
                ),
                cls="w-full max-w-sm",
            ),
            cls="min-h-screen flex items-center justify-center",
        )

    def render(self):
        """渲染登录页面"""
        return Html(
            Head(
                Meta(charset="utf-8"),
                Meta(name="viewport", content="width=device-width, initial-scale=1.0"),
                Title(self.title),
                *(Theme.blue.headers())
            ),
            Body(self.main_block()),
        )
