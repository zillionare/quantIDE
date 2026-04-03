# 任务计划

## 目标
根据用户确认的最新反馈，修复当前项目中 `Header` 和 `Sidebar` 的实现样式，实现规范：
- `Sidebar`: 
  1. 移除 `Svg` 组件创建，改用 `UkIcon`
  2. 实现 Accordion 风格的多级菜单
  3. 修正硬编码的颜色（蓝/灰等），按规范调整为主色和暗色 (`#2c3030`)
- `Header`: 
  1. 增加 Box-shadow
  2. （目前已确认不需要强制背景为 `#e41815`），由用户修改文档规范。
  3. 精简头像下拉菜单，只保留"重设密码"和"退出登录"。
  4. 头像菜单的重置密码项改用弹窗（对话框）形式展现，并在修改密码成功后清空会话、重定向用户至登录页要求再次登录。

## 阶段
- [x] **阶段 1**: 读取并分析 `.dev/specs/02-layout-nav-style.md` 规范文件内容。
- [x] **阶段 2**: 查找并读取当前代码库中 Header、Sidebar 及 Layout 的实现文件。
- [x] **阶段 3**: 对比规范与实际实现，记录差异并向用户汇报。
- [x] **阶段 4**: 修改 `quantide/web/components/sidebar.py` 以适应 Accordion 风格和新样式。
- [x] **阶段 5**: 修改 `quantide/web/components/header.py` 以添加 box shadow 并精简用户菜单。
- [x] **阶段 6**: 修改 Header 菜单"重设密码"的交互方式：添加模态框和提交成功后安全跳转（注销身份信息）至 `/login` 页面的逻辑流程。

## 遇到的错误
| 错误 | 尝试次数 | 解决方案 |
|------|---------|---------|

## 文件跟踪
- `quantide/web/components/sidebar.py` (修改完成)
- `quantide/web/components/header.py` (修改完成)
- `quantide/web/auth/routes.py` (补充修改重设密码强制登出逻辑处理)