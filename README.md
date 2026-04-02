# Turnstile Solver 门户版

一个基于 Python 的 Turnstile 求解服务，集成了统一账户中心、管理员后台、代理池管理、开发者接口文档和运维面板。

## 项目亮点

- 统一服务门户：首页提供滚动导航、能力介绍、服务说明和页面展示。
- 开发者文档：面向调用方展示公开接口，不暴露管理员后台接口。
- 统一账户中心：普通用户与管理员都可登录账户中心，但权限展示严格隔离。
- 管理后台：提供账号管理、代理池管理、任务耗时统计和高级运维图表。
- 丰富交互：右上角提示、若依风格新增/编辑弹窗、多主题切换。

## 页面预览

### 平台门户

![平台门户](static/images/portal-overview.svg)

### 接口文档

![接口文档](static/images/docs-preview.svg)

### 账户中心

![账户中心](static/images/account-center.svg)

## 页面入口

- `/`：服务门户首页
- `/docs`：开发者接口文档
- `/user/login`：统一账户登录
- `/user/center`：账户中心
- `/admin/login`：管理员登录页
- `/admin`：运维面板
- `/admin/accounts`：账号管理
- `/admin/proxies`：代理池管理

## 公共接口

### 1. 提交求解任务

```http
GET /turnstile?url=https://example.com/login&sitekey=0x4AAAAAACgP975UcSubdv3v
```

参数说明：

- `url`：目标页面地址，必填
- `sitekey`：Turnstile 站点 Key，必填
- `action`：可选动作参数
- `cdata`：可选自定义参数

成功示例：

```json
{
  "taskId": "f0dbe75b-fa76-41ad-89aa-4d3a392040af"
}
```

### 2. 查询任务结果

```http
GET /result?id=f0dbe75b-fa76-41ad-89aa-4d3a392040af
```

处理中示例：

```json
{
  "status": "processing"
}
```

成功示例：

```json
{
  "status": "ready",
  "solution": {
    "token": "0.xxx"
  },
  "elapsed_time": 7.62
}
```

失败示例：

```json
{
  "status": "failed",
  "value": "CAPTCHA_FAIL",
  "elapsed_time": 30.0
}
```

## 安装说明

建议使用 Python 3.8+。

### 1. 创建虚拟环境

```bash
python -m venv .venv
```

### 2. 激活环境

Windows：

```bash
.venv\Scripts\activate
```

macOS / Linux：

```bash
source .venv/bin/activate
```

### 3. 安装依赖

```bash
pip install -r requirements.txt
```

### 4. 安装浏览器运行时

Chromium：

```bash
python -m patchright install chromium
```

Edge：

```bash
python -m patchright install msedge
```

Camoufox：

```bash
python -m camoufox fetch
```

### 5. 启动服务

PostgreSQL：

```bash
python api_solver.py --db-type pgsql --db-url postgresql://postgres:postgres@127.0.0.1:5432/turnstile_solver
```

SQLite：

```bash
python api_solver.py --db-type sqlite --db-path results.db
```

## 常用参数

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `--browser_type` | `chromium` | 浏览器类型 |
| `--thread` | `4` | 浏览器线程数 |
| `--host` | `0.0.0.0` | 监听地址 |
| `--port` | `5072` | 服务端口 |
| `--db-type` | `pgsql` | 数据库类型 |
| `--db-url` | 空 | PostgreSQL 连接地址 |
| `--db-path` | `results.db` | SQLite 文件路径 |
| `--proxy` | `False` | 是否启用代理 |
| `--random` | `False` | 是否启用随机浏览器配置 |

## 当前内置模块

- Turnstile 求解接口
- 结果轮询接口
- 统一账户中心
- 管理员后台
- 代理池管理
- 开发者接口文档
- 运维监控面板
- API Key、Webhook、IP 白名单、套餐配额、账单等服务入口预留

## 权限说明

- 访客：可访问首页与开发者文档。
- 普通用户：可访问账户中心，不可见管理员菜单与后台数据。
- 管理员：可登录后台进行账号、代理池和运维管理。

## 开发说明

如果你继续扩展本项目，建议优先补齐以下模块：

- API Key 的创建、删除、禁用与签名校验
- 调用日志分页、筛选与导出
- Webhook 配置与签名回调
- 套餐、订单、充值与额度治理

## 免责声明

本项目仅用于学习与研究，请在合法合规的前提下使用。
