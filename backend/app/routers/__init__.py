"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。

schema_refresh 为固定路径工作流路由，必须在通用动态路由 resource 之前注册，
否则 /api/schema-refresh/... 会被 /api/{module} 抢先匹配。
"""
from __future__ import annotations

from app.routers import resource, schema_refresh

ROUTERS = [schema_refresh, resource]
