import time
from typing import Any, Dict

from quart import jsonify, redirect, render_template, request, session, url_for

from db_results import (
    adjust_portal_user_points,
    authenticate_portal_admin,
    authenticate_portal_user,
    create_service_api_key,
    create_service_billing_order,
    create_service_ip_whitelist,
    create_service_webhook,
    create_portal_admin,
    create_portal_user,
    delete_service_api_key,
    delete_service_ip_whitelist,
    delete_service_webhook,
    delete_portal_admin,
    delete_portal_user,
    get_database_config,
    get_portal_stats,
    list_points_transactions,
    list_portal_admins,
    list_portal_users,
    list_recent_results,
    list_recent_results_for_owner,
    list_service_api_keys,
    list_service_billing_orders,
    list_service_ip_whitelist,
    list_service_webhooks,
    promote_user_to_admin,
    update_service_api_key,
    update_service_api_key_status,
    update_portal_admin,
    update_portal_admin_password,
    update_portal_admin_status,
    update_portal_user,
    update_portal_user_password,
    update_portal_user_status,
)
from proxy_pool import (
    create_proxy_pool,
    delete_proxy_pool,
    get_active_proxy_pool,
    import_proxies,
    list_proxy_pools,
    remove_proxy,
    set_active_proxy_pool,
    update_proxy_pool,
)


def _message() -> str:
    return request.args.get("message", "")


API_KEY_SCOPE_OPTIONS = [
    {"value": "solve", "label": "仅验证求解"},
    {"value": "docs", "label": "仅文档访问"},
    {"value": "solve,docs", "label": "全部权限"},
]


def _normalize_api_key_scopes(raw_scopes: str) -> str:
    allowed_scopes = ["solve", "docs"]
    selected_scopes = []
    for scope in (raw_scopes or "").split(","):
        scope_name = scope.strip()
        if scope_name in allowed_scopes and scope_name not in selected_scopes:
            selected_scopes.append(scope_name)
    if not selected_scopes:
        return "solve"
    return ",".join(selected_scopes)


class ManagementPortal:
    def __init__(self, server: Any):
        self.server = server
        self.app = server.app

    def register_routes(self) -> None:
        self.app.route("/docs", methods=["GET"])(self.docs_page)
        self.app.route("/login", methods=["GET", "POST"])(self.user_login_page)
        self.app.route("/admin/login", methods=["GET", "POST"])(self.login_page)
        self.app.route("/admin/logout", methods=["POST"])(self.logout_action)
        self.app.route("/user/login", methods=["GET", "POST"])(self.user_login_page)
        self.app.route("/user/register", methods=["GET", "POST"])(self.user_register_page)
        self.app.route("/user/logout", methods=["POST"])(self.user_logout_action)
        self.app.route("/user/center", methods=["GET", "POST"])(self.user_center_page)

        self.app.route("/admin", methods=["GET"])(self.dashboard_page)
        self.app.route("/admin/monitor", methods=["GET"])(self.dashboard_page)
        self.app.route("/admin/api/metrics", methods=["GET"])(self.metrics_api)
        self.app.route("/admin/accounts", methods=["GET"])(self.accounts_page)
        self.app.route("/admin/proxies", methods=["GET"])(self.proxies_page)
        self.app.route("/admin/users", methods=["GET"])(self.accounts_redirect)
        self.app.route("/admin/admins", methods=["GET"])(self.accounts_redirect)

        self.app.route("/admin/accounts/user/create", methods=["POST"])(self.user_create_action)
        self.app.route("/admin/accounts/user/<user_id>/update", methods=["POST"])(self.user_update_action)
        self.app.route("/admin/accounts/user/<user_id>/status", methods=["POST"])(self.user_status_action)
        self.app.route("/admin/accounts/user/<user_id>/points", methods=["POST"])(self.user_points_action)
        self.app.route("/admin/accounts/user/<user_id>/password", methods=["POST"])(self.user_password_action)
        self.app.route("/admin/accounts/user/<user_id>/promote", methods=["POST"])(self.user_promote_action)
        self.app.route("/admin/accounts/user/<user_id>/delete", methods=["POST"])(self.user_delete_action)

        self.app.route("/admin/accounts/admin/create", methods=["POST"])(self.admin_create_action)
        self.app.route("/admin/accounts/admin/<admin_id>/update", methods=["POST"])(self.admin_update_action)
        self.app.route("/admin/accounts/admin/<admin_id>/password", methods=["POST"])(self.admin_reset_password_action)
        self.app.route("/admin/accounts/admin/<admin_id>/status", methods=["POST"])(self.admin_status_action)
        self.app.route("/admin/accounts/admin/<admin_id>/delete", methods=["POST"])(self.admin_delete_action)
        self.app.route("/admin/password", methods=["POST"])(self.admin_password_action)
        self.app.route("/admin/proxies/create", methods=["POST"])(self.proxy_create_action)
        self.app.route("/admin/proxies/<pool_id>/update", methods=["POST"])(self.proxy_update_action)
        self.app.route("/admin/proxies/<pool_id>/activate", methods=["POST"])(self.proxy_activate_action)
        self.app.route("/admin/proxies/<pool_id>/import", methods=["POST"])(self.proxy_import_action)
        self.app.route("/admin/proxies/<pool_id>/delete", methods=["POST"])(self.proxy_delete_action)
        self.app.route("/admin/proxies/<pool_id>/remove-item", methods=["POST"])(self.proxy_item_delete_action)

    async def _page_context(self, title: str, active_nav: str) -> Dict[str, Any]:
        stats = await get_portal_stats()
        runtime = self.server.get_runtime_metrics()
        return {
            "title": title,
            "active_nav": active_nav,
            "message": _message(),
            "stats": stats,
            "runtime": runtime,
            "db": get_database_config(),
            "recent_logs": self.server.get_recent_logs(limit=30),
            "admin_session": {
                "id": session.get("admin_id"),
                "username": session.get("admin_username"),
                "role": session.get("admin_role"),
                "logged_in": bool(session.get("admin_id")),
            },
            "user_session": {
                "id": session.get("user_id"),
                "username": session.get("user_username"),
                "plan": session.get("user_plan"),
                "role": session.get("user_role"),
                "kind": session.get("user_kind", "user"),
                "logged_in": bool(session.get("user_id")),
            },
        }

    def _clear_admin_session(self) -> None:
        session.pop("admin_id", None)
        session.pop("admin_username", None)
        session.pop("admin_role", None)

    def _clear_user_session(self) -> None:
        session.pop("user_id", None)
        session.pop("user_username", None)
        session.pop("user_plan", None)
        session.pop("user_role", None)
        session.pop("user_kind", None)

    def _set_user_session(self, account_id: str, username: str, plan: str = "-", role: str = "", kind: str = "user") -> None:
        session["user_id"] = account_id
        session["user_username"] = username
        session["user_plan"] = plan
        session["user_role"] = role
        session["user_kind"] = kind

    def _set_admin_session(self, admin_id: str, username: str, role: str) -> None:
        session["admin_id"] = admin_id
        session["admin_username"] = username
        session["admin_role"] = role

    async def _authenticate_unified_account(self, username: str, password: str):
        admin = await authenticate_portal_admin(username, password)
        if admin:
            self._set_admin_session(admin["id"], admin["username"], admin["role"])
            self._set_user_session(
                admin["id"],
                admin["username"],
                plan="管理员账户",
                role=admin.get("role") or "operator",
                kind="admin",
            )
            return "admin", admin

        user = await authenticate_portal_user(username, password)
        if user:
            self._clear_admin_session()
            self._set_user_session(user["id"], user["username"], user.get("plan") or "free", kind="user")
            return "user", user

        return None, None

    async def _ensure_admin_auth(self):
        if not session.get("admin_id"):
            return redirect(url_for("login_page", message="请先登录管理员账号"))
        return None

    async def _ensure_user_auth(self):
        if not session.get("user_id"):
            return redirect(url_for("user_login_page", message="请先登录后访问账户中心"))
        return None

    async def login_page(self):
        if request.method == "POST":
            form = await request.form
            username = (form.get("username") or "").strip()
            password = (form.get("password") or "").strip()
            kind, _ = await self._authenticate_unified_account(username, password)
            if kind == "admin":
                return redirect(url_for("dashboard_page", message="管理员登录成功"))
            if kind == "user":
                return redirect(url_for("user_center_page", message="登录成功"))
            return redirect(url_for("login_page", message="用户名或密码错误"))

        context = await self._page_context("管理员登录", "login")
        context["auth_mode"] = "admin"
        return await render_template("user_login.html", **context)

    async def logout_action(self):
        if session.get("user_kind") == "admin":
            self._clear_user_session()
        self._clear_admin_session()
        return redirect(url_for("login_page", message="已退出登录"))

    async def user_login_page(self):
        if request.method == "POST":
            form = await request.form
            username = (form.get("username") or "").strip()
            password = (form.get("password") or "").strip()
            kind, _ = await self._authenticate_unified_account(username, password)
            if kind == "user":
                return redirect(url_for("user_center_page", message="登录成功"))
            if kind == "admin":
                return redirect(url_for("user_center_page", message="已使用管理员账户登录"))

            return redirect(url_for("user_login_page", message="用户名或密码错误"))

        context = await self._page_context("账户登录", "user_login")
        context["auth_mode"] = "user"
        return await render_template("user_login.html", **context)

    async def user_register_page(self):
        if request.method == "POST":
            form = await request.form
            username = (form.get("username") or "").strip()
            email = (form.get("email") or "").strip()
            password = (form.get("password") or "").strip()
            if username and email and password:
                await create_portal_user(username=username, email=email, password=password, plan="free", points=0)
                return redirect(url_for("user_login_page", message="注册成功，请登录"))
            return redirect(url_for("user_register_page", message="请完整填写注册信息"))

        context = await self._page_context("用户注册", "user_register")
        return await render_template("user_register.html", **context)

    async def user_logout_action(self):
        if session.get("user_kind") == "admin":
            self._clear_admin_session()
        self._clear_user_session()
        return redirect(url_for("user_login_page", message="已安全退出"))

    async def user_center_page(self):
        auth_redirect = await self._ensure_user_auth()
        if auth_redirect:
            return auth_redirect

        section = (request.args.get("section") or "overview").strip()
        if section not in {"overview", "keys", "security", "logs", "billing"}:
            section = "overview"

        def _redirect_user_center(message: str):
            return redirect(url_for("user_center_page", section=section, message=message))

        if request.method == "POST":
            form = await request.form
            action = (form.get("action") or "password").strip()
            owner_id = session.get("user_id")
            owner_kind = session.get("user_kind", "user")
            if action == "password":
                new_password = (form.get("new_password") or "").strip()
                if new_password:
                    if owner_kind == "admin":
                        await update_portal_admin_password(owner_id, new_password)
                        return _redirect_user_center("管理员密码已更新")
                    await update_portal_user_password(owner_id, new_password)
                    return _redirect_user_center("账户密码已更新")
                return _redirect_user_center("请输入新的密码")
            if action == "create_api_key":
                name = (form.get("name") or "默认密钥").strip()
                scopes = _normalize_api_key_scopes((form.get("scopes") or "solve").strip())
                expires_at = (form.get("expires_at") or "").strip()
                await create_service_api_key(owner_id, owner_kind, name, scopes, expires_at)
                return _redirect_user_center("API Key 已创建")
            if action == "toggle_api_key":
                key_id = (form.get("key_id") or "").strip()
                status = (form.get("status") or "disabled").strip()
                await update_service_api_key_status(key_id, status)
                return _redirect_user_center("API Key 状态已更新")
            if action == "update_api_key":
                key_id = (form.get("key_id") or "").strip()
                name = (form.get("name") or "默认密钥").strip()
                scopes = _normalize_api_key_scopes((form.get("scopes") or "solve").strip())
                expires_at = (form.get("expires_at") or "").strip()
                await update_service_api_key(key_id, name, scopes, expires_at)
                return _redirect_user_center("API Key 已更新")
            if action == "delete_api_key":
                await delete_service_api_key((form.get("key_id") or "").strip())
                return _redirect_user_center("API Key 已删除")
            if action == "create_webhook":
                endpoint = (form.get("endpoint") or "").strip()
                events = (form.get("events") or "task.ready,task.failed").strip()
                secret = (form.get("secret") or "").strip()
                if not endpoint:
                    return _redirect_user_center("Webhook 地址不能为空")
                await create_service_webhook(owner_id, owner_kind, endpoint, events, secret)
                return _redirect_user_center("Webhook 已创建")
            if action == "delete_webhook":
                await delete_service_webhook((form.get("webhook_id") or "").strip())
                return _redirect_user_center("Webhook 已删除")
            if action == "create_ip_whitelist":
                ip_address = (form.get("ip_address") or "").strip()
                note = (form.get("note") or "").strip()
                if not ip_address:
                    return _redirect_user_center("IP 地址不能为空")
                await create_service_ip_whitelist(owner_id, owner_kind, ip_address, note)
                return _redirect_user_center("IP 白名单已添加")
            if action == "delete_ip_whitelist":
                await delete_service_ip_whitelist((form.get("whitelist_id") or "").strip())
                return _redirect_user_center("IP 白名单已删除")
            if action == "create_billing_order":
                amount = float((form.get("amount") or "0").strip() or 0)
                points = int((form.get("points") or "0").strip() or 0)
                description = (form.get("description") or "账户充值").strip()
                if amount <= 0 or points <= 0:
                    return _redirect_user_center("充值金额和积分必须大于 0")
                await create_service_billing_order(owner_id, owner_kind, amount, points, description)
                return _redirect_user_center("充值订单已创建")

        title_map = {
            "overview": "账户总览",
            "keys": "API Key",
            "security": "安全设置",
            "logs": "调用日志",
            "billing": "账单与充值",
        }
        active_nav_map = {
            "overview": "user_overview",
            "keys": "user_keys",
            "security": "user_security",
            "logs": "user_logs",
            "billing": "user_billing",
        }
        context = await self._page_context(title_map[section], active_nav_map[section])
        users = await list_portal_users()
        admins = await list_portal_admins()
        if session.get("user_kind") == "admin":
            current_account = next((item for item in admins if item["id"] == session.get("user_id")), None)
            point_logs = []
        else:
            current_account = next((item for item in users if item["id"] == session.get("user_id")), None)
            point_logs = [item for item in await list_points_transactions(limit=50) if item.get("user_id") == session.get("user_id")]
        context["current_user"] = current_account
        context["point_logs"] = point_logs
        owner_id = session.get("user_id")
        owner_kind = session.get("user_kind", "user")
        if owner_kind == "admin":
            context["recent_tasks"] = await list_recent_results(limit=20)
        else:
            context["recent_tasks"] = await list_recent_results_for_owner(owner_id, owner_kind, limit=20)
        context["service_api_keys"] = await list_service_api_keys(session.get("user_id"), session.get("user_kind", "user"))
        context["service_webhooks"] = await list_service_webhooks(session.get("user_id"), session.get("user_kind", "user"))
        context["service_ip_whitelist"] = await list_service_ip_whitelist(session.get("user_id"), session.get("user_kind", "user"))
        context["billing_orders"] = await list_service_billing_orders(session.get("user_id"), session.get("user_kind", "user"))
        context["api_key_scope_options"] = API_KEY_SCOPE_OPTIONS
        context["user_section"] = section
        return await render_template("user_center.html", **context)

    async def docs_page(self):
        context = await self._page_context("开发者文档", "docs")
        return await render_template("docs.html", **context)

    async def dashboard_page(self):
        auth_redirect = await self._ensure_admin_auth()
        if auth_redirect:
            return auth_redirect
        context = await self._page_context("控制台概览", "dashboard")
        context["recent_tasks"] = await list_recent_results(limit=12)
        context["point_logs"] = await list_points_transactions(limit=12)
        return await render_template("dashboard.html", **context)

    async def metrics_api(self):
        auth_redirect = await self._ensure_admin_auth()
        if auth_redirect:
            return auth_redirect
        return jsonify(
            {
                "stats": await get_portal_stats(),
                "runtime": self.server.get_runtime_metrics(),
                "recent_tasks": await list_recent_results(limit=8),
                "recent_logs": self.server.get_recent_logs(limit=12),
                "server_time": int(time.time()),
            }
        )

    async def accounts_redirect(self):
        auth_redirect = await self._ensure_admin_auth()
        if auth_redirect:
            return auth_redirect
        return redirect(url_for("accounts_page"))

    async def accounts_page(self):
        auth_redirect = await self._ensure_admin_auth()
        if auth_redirect:
            return auth_redirect
        context = await self._page_context("用户管理", "accounts")
        users = await list_portal_users()
        admins = await list_portal_admins()
        admin_by_username = {item["username"]: item for item in admins}
        linked_usernames = set()
        account_rows = []

        for user in users:
            admin = admin_by_username.get(user["username"])
            if admin:
                linked_usernames.add(user["username"])
            account_rows.append(
                {
                    "kind": "user",
                    "user": user,
                    "admin": admin,
                    "is_admin": bool(admin),
                }
            )

        for admin in admins:
            if admin["username"] in linked_usernames:
                continue
            account_rows.append(
                {
                    "kind": "admin_only",
                    "user": None,
                    "admin": admin,
                    "is_admin": True,
                }
            )

        context["users"] = users
        context["admins"] = admins
        context["account_rows"] = account_rows
        context["point_logs"] = await list_points_transactions(limit=20)
        return await render_template("accounts.html", **context)

    async def proxies_page(self):
        auth_redirect = await self._ensure_admin_auth()
        if auth_redirect:
            return auth_redirect
        context = await self._page_context("代理池管理", "proxies")
        context["proxy_pools"] = list_proxy_pools()
        context["active_proxy_pool"] = get_active_proxy_pool()
        return await render_template("proxies.html", **context)

    async def user_create_action(self):
        auth_redirect = await self._ensure_admin_auth()
        if auth_redirect:
            return auth_redirect
        form = await request.form
        username = (form.get("username") or "").strip()
        email = (form.get("email") or "").strip()
        plan = (form.get("plan") or "free").strip()
        points = int((form.get("points") or "0").strip() or 0)
        password = (form.get("password") or "demo123456").strip() or "demo123456"
        role = (form.get("role") or "user").strip()
        await create_portal_user(
            username=username,
            email=email,
            plan=plan,
            points=points,
            password=password,
        )
        if role != "user":
            await create_portal_admin(username=username, password=password, role=role)
        return redirect(url_for("accounts_page", message="用户创建成功"))

    async def user_update_action(self, user_id: str):
        auth_redirect = await self._ensure_admin_auth()
        if auth_redirect:
            return auth_redirect
        form = await request.form
        users = await list_portal_users()
        admins = await list_portal_admins()
        current_user = next((item for item in users if item["id"] == user_id), None)
        if not current_user:
            return redirect(url_for("accounts_page", message="用户不存在"))

        linked_admin = next((item for item in admins if item["username"] == current_user["username"]), None)
        username = (form.get("username") or "").strip()
        email = (form.get("email") or "").strip()
        plan = (form.get("plan") or "free").strip()
        user_status = (form.get("status") or current_user.get("status") or "active").strip()
        role = (form.get("role") or (linked_admin.get("role") if linked_admin else "user") or "user").strip()
        new_password = (form.get("new_password") or "").strip()
        points_delta = int((form.get("points_delta") or "0").strip() or 0)
        points_description = (form.get("points_description") or "").strip()

        await update_portal_user(
            user_id=user_id,
            username=username,
            email=email,
            plan=plan,
            status=user_status,
        )

        if role == "user":
            if linked_admin:
                await delete_portal_admin(linked_admin["id"])
        elif linked_admin:
            await update_portal_admin(linked_admin["id"], username, role, linked_admin.get("status") or "active")
        else:
            await create_portal_admin(username=username, password=new_password or "admin123456", role=role)

        if new_password:
            await update_portal_user_password(user_id, new_password)
            refreshed_admins = await list_portal_admins()
            current_admin = next((item for item in refreshed_admins if item["username"] == username), None)
            if current_admin:
                await update_portal_admin_password(current_admin["id"], new_password)

        if points_delta:
            await adjust_portal_user_points(user_id, points_delta, description=points_description)

        return redirect(url_for("accounts_page", message="用户信息已更新"))

    async def user_status_action(self, user_id: str):
        auth_redirect = await self._ensure_admin_auth()
        if auth_redirect:
            return auth_redirect
        form = await request.form
        status = (form.get("status") or "active").strip()
        await update_portal_user_status(user_id, status)
        users = await list_portal_users()
        admins = await list_portal_admins()
        current_user = next((item for item in users if item["id"] == user_id), None)
        if current_user:
            linked_admin = next((item for item in admins if item["username"] == current_user["username"]), None)
            if linked_admin:
                await update_portal_admin_status(linked_admin["id"], status)
        return redirect(url_for("accounts_page", message="用户状态已更新"))

    async def user_points_action(self, user_id: str):
        auth_redirect = await self._ensure_admin_auth()
        if auth_redirect:
            return auth_redirect
        form = await request.form
        await adjust_portal_user_points(
            user_id,
            int((form.get("amount") or "0").strip() or 0),
            description=(form.get("description") or "").strip(),
        )
        return redirect(url_for("accounts_page", message="积分已调整"))

    async def user_password_action(self, user_id: str):
        auth_redirect = await self._ensure_admin_auth()
        if auth_redirect:
            return auth_redirect
        form = await request.form
        new_password = (form.get("new_password") or "").strip()
        if not new_password:
            return redirect(url_for("accounts_page", message="请输入新的用户密码"))
        await update_portal_user_password(user_id, new_password)
        return redirect(url_for("accounts_page", message="用户密码已更新"))

    async def user_promote_action(self, user_id: str):
        auth_redirect = await self._ensure_admin_auth()
        if auth_redirect:
            return auth_redirect
        form = await request.form
        await promote_user_to_admin(
            user_id=user_id,
            role=(form.get("role") or "operator").strip(),
            password=(form.get("password") or "admin123456").strip() or "admin123456",
        )
        return redirect(url_for("accounts_page", message="用户权限已提升"))

    async def user_delete_action(self, user_id: str):
        auth_redirect = await self._ensure_admin_auth()
        if auth_redirect:
            return auth_redirect
        users = await list_portal_users()
        admins = await list_portal_admins()
        current_user = next((item for item in users if item["id"] == user_id), None)
        if current_user:
            linked_admin = next((item for item in admins if item["username"] == current_user["username"]), None)
            if linked_admin:
                await delete_portal_admin(linked_admin["id"])
        await delete_portal_user(user_id)
        return redirect(url_for("accounts_page", message="用户已删除"))

    async def admin_create_action(self):
        auth_redirect = await self._ensure_admin_auth()
        if auth_redirect:
            return auth_redirect
        form = await request.form
        await create_portal_admin(
            username=(form.get("username") or "").strip(),
            password=(form.get("password") or "admin123456").strip() or "admin123456",
            role=(form.get("role") or "operator").strip(),
        )
        return redirect(url_for("accounts_page", message="管理员已创建"))

    async def admin_update_action(self, admin_id: str):
        auth_redirect = await self._ensure_admin_auth()
        if auth_redirect:
            return auth_redirect
        form = await request.form
        new_password = (form.get("new_password") or "").strip()
        await update_portal_admin(
            admin_id=admin_id,
            username=(form.get("username") or "").strip(),
            role=(form.get("role") or "operator").strip(),
            status=(form.get("status") or "active").strip(),
        )
        if new_password:
            await update_portal_admin_password(admin_id, new_password)
        return redirect(url_for("accounts_page", message="管理员信息已更新"))

    async def admin_status_action(self, admin_id: str):
        auth_redirect = await self._ensure_admin_auth()
        if auth_redirect:
            return auth_redirect
        form = await request.form
        status = (form.get("status") or "active").strip()
        await update_portal_admin_status(admin_id, status)
        admins = await list_portal_admins()
        users = await list_portal_users()
        current_admin = next((item for item in admins if item["id"] == admin_id), None)
        if current_admin:
            linked_user = next((item for item in users if item["username"] == current_admin["username"]), None)
            if linked_user:
                await update_portal_user_status(linked_user["id"], status)
        return redirect(url_for("accounts_page", message="管理员状态已更新"))

    async def admin_delete_action(self, admin_id: str):
        auth_redirect = await self._ensure_admin_auth()
        if auth_redirect:
            return auth_redirect
        admins = await list_portal_admins()
        users = await list_portal_users()
        current_admin = next((item for item in admins if item["id"] == admin_id), None)
        if current_admin:
            linked_user = next((item for item in users if item["username"] == current_admin["username"]), None)
            if linked_user:
                await delete_portal_user(linked_user["id"])
        await delete_portal_admin(admin_id)
        return redirect(url_for("accounts_page", message="管理员已删除"))

    async def admin_password_action(self):
        auth_redirect = await self._ensure_admin_auth()
        if auth_redirect:
            return auth_redirect
        form = await request.form
        new_password = (form.get("new_password") or "").strip()
        if not new_password:
            return redirect(url_for("accounts_page", message="请输入新的密码"))
        await update_portal_admin_password(session["admin_id"], new_password)
        return redirect(url_for("accounts_page", message="管理员密码已更新"))

    async def admin_reset_password_action(self, admin_id: str):
        auth_redirect = await self._ensure_admin_auth()
        if auth_redirect:
            return auth_redirect
        form = await request.form
        new_password = (form.get("new_password") or "").strip()
        if not new_password:
            return redirect(url_for("accounts_page", message="请输入新的管理员密码"))
        await update_portal_admin_password(admin_id, new_password)
        return redirect(url_for("accounts_page", message="管理员密码已更新"))

    async def proxy_create_action(self):
        auth_redirect = await self._ensure_admin_auth()
        if auth_redirect:
            return auth_redirect
        form = await request.form
        create_proxy_pool(
            name=(form.get("name") or "未命名代理池").strip(),
            strategy=(form.get("strategy") or "round_robin").strip(),
            enabled=(form.get("enabled") or "true") == "true",
        )
        return redirect(url_for("proxies_page", message="代理池已创建"))

    async def proxy_update_action(self, pool_id: str):
        auth_redirect = await self._ensure_admin_auth()
        if auth_redirect:
            return auth_redirect
        form = await request.form
        update_proxy_pool(
            pool_id=pool_id,
            name=(form.get("name") or "").strip(),
            strategy=(form.get("strategy") or "round_robin").strip(),
            enabled=(form.get("enabled") or "false") == "true",
        )
        return redirect(url_for("proxies_page", message="代理池已更新"))

    async def proxy_activate_action(self, pool_id: str):
        auth_redirect = await self._ensure_admin_auth()
        if auth_redirect:
            return auth_redirect
        set_active_proxy_pool(pool_id)
        return redirect(url_for("proxies_page", message="已切换当前代理池"))

    async def proxy_import_action(self, pool_id: str):
        auth_redirect = await self._ensure_admin_auth()
        if auth_redirect:
            return auth_redirect
        form = await request.form
        count = import_proxies(pool_id, (form.get("proxies") or "").strip())
        return redirect(url_for("proxies_page", message=f"已导入 {count} 条代理"))

    async def proxy_delete_action(self, pool_id: str):
        auth_redirect = await self._ensure_admin_auth()
        if auth_redirect:
            return auth_redirect
        delete_proxy_pool(pool_id)
        return redirect(url_for("proxies_page", message="代理池已删除"))

    async def proxy_item_delete_action(self, pool_id: str):
        auth_redirect = await self._ensure_admin_auth()
        if auth_redirect:
            return auth_redirect
        form = await request.form
        remove_proxy(pool_id, (form.get("proxy_value") or "").strip())
        return redirect(url_for("proxies_page", message="代理已移除"))
