import asyncio
import mimetypes
import os
from email.message import EmailMessage
from typing import Awaitable, List

import aiosmtplib
import aiosmtplib.errors
import cfg4py
from loguru import logger
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from quantide.config.runtime import (
    get_runtime_mail_receivers,
    get_runtime_mail_sender,
    get_runtime_mail_server,
)


def mail_notify(
    subject: str = "Quantide 交易通知",
    body: str | None = None,
    msg: EmailMessage | None = None,
    html=False,
    receivers=None,
) -> Awaitable:
    """发送邮件通知。

    发送者、接收者及邮件服务器等配置请通过cfg4py配置

    ```
    notify:
        mail_from: aaron_yang@jieyu.ai
        mail_to:
            - code@quantide.cn
        mail_server: smtp.ym.163.com
    ```
    密码请通过环境变量`QUANTIDE_MAIL_PASSWORD`来配置。

    subject/body与msg必须提供其一。

    ???+ Important
        必须在异步线程(即运行asyncio loop的线程）中调用此方法，否则会抛出异常。
        此方法返回一个Awaitable，您可以等待它完成，也可以忽略返回值，此时它将作为一个后台任务执行，但完成的时间不确定。

    Args:
        msg (EmailMessage, optional): [description]. Defaults to None.
        subject (str, optional): [description]. Defaults to None.
        body (str, optional): [description]. Defaults to None.
        html (bool, optional): body是否按html格式处理？ Defaults to False.
        receivers (List[str], Optional): 接收者信息。如果不提供，将使用预先配置的接收者信息。

    Returns:
        发送消息的后台任务。您可以使用此返回句柄来取消任务。
    """
    if all([msg is not None, subject or body]):
        raise TypeError("msg参数与subject/body只能提供其中之一")
    elif all([msg is None, subject is None, body is None]):
        raise TypeError("必须提供msg参数或者subjecdt/body参数")

    if msg is None:
        assert body is not None
        if html:
            msg = compose(subject, html=body)
        else:
            msg = compose(subject, plain_txt=body)

    if not receivers:
        receivers = get_runtime_mail_receivers()

    password = os.environ.get("QUANTIDE_MAIL_PASSWORD")
    return send_mail(
        get_runtime_mail_sender(),
        receivers,
        password,
        msg,
        host=get_runtime_mail_server(),
    )


@retry(
    retry=retry_if_exception_type(aiosmtplib.errors.SMTPConnectError),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=10),
)
def send_mail(
    sender: str,
    receivers: List[str],
    password: str,
    msg: EmailMessage | None = None,
    host: str | None = None,
    port: int = 25,
    cc: List[str] | None = None,
    bcc: List[str] | None = None,
    subject: str | None = None,
    body: str | None = None,
    username: str | None = None,
) -> Awaitable:
    """发送邮件通知。

    如果只发送简单的文本邮件，请使用 send_mail(sender, receivers, subject=subject, plain=plain)。如果要发送较复杂的带html和附件的邮件，请先调用compose()生成一个EmailMessage,然后再调用send_mail(sender, receivers, msg)来发送邮件。

    ???+ Important
        必须在异步线程(即运行asyncio loop的线程）中调用此方法，否则会抛出异常。
        此方法返回一个Awaitable，您可以等待它完成，也可以忽略返回值，此时它将作为一个后台任务执行，但完成的时间不确定。

    Args:
        sender (str): [description]
        receivers (List[str]): [description]
        msg (EmailMessage, optional): [description]. Defaults to None.
        host (str, optional): [description]. Defaults to None.
        port (int, optional): [description]. Defaults to 25.
        cc (List[str], optional): [description]. Defaults to None.
        bcc (List[str], optional): [description]. Defaults to None.
        subject (str, optional): [description]. Defaults to None.
        plain (str, optional): [description]. Defaults to None.
        username (str, optional): the username used to logon to mail server. if not provided, then `sender` is used.

    Returns:
        发送消息的后台任务。您可以使用此返回句柄来取消任务。
    """
    if all([msg is not None, subject is not None or body is not None]):
        raise TypeError("msg参数与subject/body只能提供其中之一")
    elif all([msg is None, subject is None, body is None]):
        raise TypeError("必须提供msg参数或者subjecdt/body参数")

    msg = msg or EmailMessage()

    if isinstance(receivers, str):
        receivers = [receivers]

    msg["From"] = sender
    msg["To"] = ", ".join(receivers)

    if subject:
        msg["subject"] = subject

    if body:
        msg.set_content(body)

    if cc:
        msg["Cc"] = ", ".join(cc)
    if bcc:
        msg["Bcc"] = ", ".join(bcc)

    username = username or sender

    if host is None:
        host = sender.split("@")[-1]

    task = asyncio.create_task(
        aiosmtplib.send(
            msg, hostname=host, port=port, username=sender, password=password
        )
    )

    return task


def compose(
    subject: str,
    plain_txt: str | None = None,
    html: str | None = None,
    attachment: str | None = None,
) -> EmailMessage:
    """编写MIME邮件。

    Args:
        subject (str): 邮件主题
        plain_txt (str): 纯文本格式的邮件内容
        html (str, optional): html格式的邮件内容. Defaults to None.
        attachment (str, optional): 附件文件名
    Returns:
        MIME mail
    """
    msg = EmailMessage()

    msg["Subject"] = subject

    if html:
        msg.preamble = plain_txt or ""
        msg.set_content(html, subtype="html")
    else:
        assert plain_txt, "Either plain_txt or html is required."
        msg.set_content(plain_txt)

    if attachment:
        ctype, encoding = mimetypes.guess_type(attachment)
        if ctype is None or encoding is not None:
            ctype = "application/octet-stream"

        maintype, subtype = ctype.split("/", 1)
        with open(attachment, "rb") as f:
            msg.add_attachment(
                f.read(), maintype=maintype, subtype=subtype, filename=attachment
            )

    return msg
