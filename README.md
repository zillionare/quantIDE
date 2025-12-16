# pyqmt


<p align="center">
<a href="https://pypi.python.org/pypi/pyqmt">
    <img src="https://img.shields.io/pypi/v/pyqmt.svg"
        alt = "Release Status">
</a>

<a href="https://github.com/zillionare/pyqmt/actions">
    <img src="https://github.com/zillionare/pyqmt/actions/workflows/main.yml/badge.svg?branch=release" alt="CI Status">
</a>

<a href="https://zillionare.github.io/pyqmt/">
    <img src="https://img.shields.io/website/https/zillionare.github.io/pyqmt/index.html.svg?label=docs&down_message=unavailable&up_message=available" alt="Documentation Status">
</a>

</p>


Skeleton project created by Python Project Wizard (ppw)


* Free software: MIT
* Documentation: <https://zillionare.github.io/pyqmt/>


## Features

* 基于FastAPI和Vue.js的现代化Web界面
* 支持会话认证和API签名认证两种方式
* 完整的用户登录和权限管理系统
* 可扩展的API架构设计

## API Authentication

本项目支持两种认证方式：

### 1. Session-based Authentication (会话认证)
用于Web浏览器访问，通过登录页面获取会话Cookie进行认证。

### 2. API Signature Authentication (API签名认证)
用于第三方系统集成，通过HMAC-SHA256签名算法进行认证。

#### 签名算法
签名字符串格式：
```
METHOD\nPATH\nQUERY_STRING\nBODY\nTIMESTAMP\nCLIENT_ID
```

各部分说明：
- METHOD: HTTP方法（GET, POST, PUT, DELETE等）
- PATH: 请求路径
- QUERY_STRING: 查询参数
- BODY: 请求体内容（GET请求为空）
- TIMESTAMP: Unix时间戳
- CLIENT_ID: 客户端ID

#### 请求头
```
X-Client-ID: 客户端ID
X-Timestamp: Unix时间戳
X-Signature: HMAC-SHA256签名
```

#### 示例代码（Python）
```python
import hmac
import hashlib
import time
import requests

# 配置信息
client_id = "your_client_id"
client_secret = "your_client_secret"
base_url = "http://localhost:8000"

# 构造请求参数
method = "POST"
path = "/api/data"
timestamp = str(int(time.time()))
body = '{"key": "value"}'

# 构造签名字符串
sign_string = f"{method}\n{path}\n\n{body}\n{timestamp}\n{client_id}"

# 计算签名
signature = hmac.new(
    client_secret.encode('utf-8'),
    sign_string.encode('utf-8'),
    hashlib.sha256
).hexdigest()

# 发送请求
headers = {
    "X-Client-ID": client_id,
    "X-Timestamp": timestamp,
    "X-Signature": signature,
    "Content-Type": "application/json"
}

response = requests.post(f"{base_url}{path}", data=body, headers=headers)
```

## Credits

This package was created with the [ppw](https://zillionare.github.io/python-project-wizard) tool. For more information, please visit the [project page](https://zillionare.github.io/python-project-wizard/).
