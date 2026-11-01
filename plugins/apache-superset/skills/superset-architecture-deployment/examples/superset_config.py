"""Platform Superset configuration (Superset 6.x). Secrets come from env injected by Vault."""
import os
from celery.schedules import crontab
from flask_appbuilder.security.manager import AUTH_OAUTH

# --- core ---------------------------------------------------------------
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = os.environ["SUPERSET_META_DB_URI"]      # postgresql+psycopg2://...
ENABLE_PROXY_FIX = True
ROW_LIMIT = 10_000
SQL_MAX_ROW = 100_000
SUPERSET_WEBSERVER_TIMEOUT = 120                                 # < ingress timeout
SQLLAB_TIMEOUT = 60
SQLLAB_ASYNC_TIME_LIMIT_SEC = 600
LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"

# --- caches (Redis) -----------------------------------------------------
REDIS = os.environ["SUPERSET_REDIS_URL"]                         # redis://redis.platform:6379
def _cache(db: int, ttl: int, prefix: str) -> dict:
    return {"CACHE_TYPE": "RedisCache", "CACHE_DEFAULT_TIMEOUT": ttl,
            "CACHE_KEY_PREFIX": f"{prefix}_", "CACHE_REDIS_URL": f"{REDIS}/{db}"}
CACHE_CONFIG = _cache(1, 300, "meta")
DATA_CACHE_CONFIG = _cache(2, 3600, "data")                       # dataset cache_timeout overrides
FILTER_STATE_CACHE_CONFIG = _cache(3, 86400, "filter")
EXPLORE_FORM_DATA_CACHE_CONFIG = _cache(4, 86400, "explore")
THUMBNAIL_CACHE_CONFIG = _cache(5, 86400, "thumb")

# --- celery -------------------------------------------------------------
class CeleryConfig:
    broker_url = f"{REDIS}/10"
    result_backend = f"{REDIS}/11"
    imports = ("superset.sql_lab", "superset.tasks.scheduler", "superset.tasks.thumbnails", "superset.tasks.cache")
    task_routes = {"superset.sql_lab.*": {"queue": "sql_lab"},
                   "superset.tasks.scheduler.*": {"queue": "reports"},
                   "superset.tasks.thumbnails.*": {"queue": "thumbnails"},
                   "superset.tasks.cache.*": {"queue": "cache_warmup"}}
    worker_prefetch_multiplier = 1
    task_acks_late = True
    beat_schedule = {
        "reports.scheduler": {"task": "reports.scheduler", "schedule": crontab(minute="*", hour="*")},
        "reports.prune_log": {"task": "reports.prune_log", "schedule": crontab(minute=0, hour=0)},
        # cache warm-up is triggered by the platform after pipelines finish (API), not by cron
    }
CELERY_CONFIG = CeleryConfig
RESULTS_BACKEND_USE_MSGPACK = True
from cachelib.redis import RedisCache
RESULTS_BACKEND = RedisCache(host=os.environ["SUPERSET_REDIS_HOST"], port=6379, db=12, key_prefix="sqllab_")

# --- feature flags ------------------------------------------------------
FEATURE_FLAGS = {
    "EMBEDDED_SUPERSET": True,
    "DISABLE_EMBEDDED_SUPERSET_LOGOUT": True,
    "DASHBOARD_RBAC": True,
    "TAGGING_SYSTEM": True,
    "DRILL_TO_DETAIL": True,
    "DRILL_BY": True,
    "ALERT_REPORTS": True,
    "THUMBNAILS": True,
    "GLOBAL_ASYNC_QUERIES": True,
    "ENABLE_TEMPLATE_PROCESSING": False,   # on only for allow-listed deployments
}
GLOBAL_ASYNC_QUERIES_JWT_SECRET = os.environ["SUPERSET_GAQ_JWT_SECRET"]
GLOBAL_ASYNC_QUERIES_REDIS_CONFIG = {"host": os.environ["SUPERSET_REDIS_HOST"], "port": 6379, "db": 13}

# --- embedding ----------------------------------------------------------
GUEST_ROLE_NAME = "EmbeddedViewer"           # minimal role created by the control plane
GUEST_TOKEN_JWT_SECRET = os.environ["SUPERSET_GUEST_JWT_SECRET"]
GUEST_TOKEN_JWT_AUDIENCE = "superset"
GUEST_TOKEN_JWT_EXP_SECONDS = 300
TALISMAN_ENABLED = True
TALISMAN_CONFIG = {
    "content_security_policy": {
        "default-src": ["'self'"], "img-src": ["'self'", "data:", "blob:"],
        "worker-src": ["'self'", "blob:"], "connect-src": ["'self'", os.environ.get("SUPERSET_HOST_APP_ORIGIN", "")],
        "frame-ancestors": ["'self'", os.environ.get("SUPERSET_HOST_APP_ORIGIN", "")],
        "object-src": "'none'", "style-src": ["'self'", "'unsafe-inline'"], "script-src": ["'self'", "'strict-dynamic'"],
    },
    "content_security_policy_nonce_in": ["script-src"], "force_https": True, "session_cookie_secure": True,
}
SESSION_COOKIE_SAMESITE = "None"             # required for cross-site iframe; with Secure
SESSION_COOKIE_SECURE = True
SESSION_COOKIE_HTTPONLY = True
WTF_CSRF_ENABLED = True

# --- auth: Keycloak OIDC, groups -> roles -------------------------------
AUTH_TYPE = AUTH_OAUTH
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = None           # deny users without a mapped group
AUTH_ROLES_SYNC_AT_LOGIN = True
_CLIENT = os.environ["KEYCLOAK_CLIENT_ID"]
OAUTH_PROVIDERS = [{
    "name": "keycloak", "icon": "fa-key", "token_key": "access_token",
    "remote_app": {
        "client_id": _CLIENT, "client_secret": os.environ["KEYCLOAK_CLIENT_SECRET"],
        "server_metadata_url": f"{os.environ['KEYCLOAK_ISSUER']}/.well-known/openid-configuration",
        "client_kwargs": {"scope": "openid email profile groups", "code_challenge_method": "S256"},
        "api_base_url": f"{os.environ['KEYCLOAK_ISSUER']}/protocol/openid-connect/",
    },
}]
# Platform convention: group "{client}_{tenant}_{role}" -> role "{tenant}_{role}" (created by the Tenant controller)
AUTH_ROLES_MAPPING = {f"{_CLIENT}_platform_admin": ["Admin"]}
# tenant roles are appended at startup from the control plane manifest
import json
AUTH_ROLES_MAPPING.update(json.loads(os.environ.get("SUPERSET_TENANT_ROLE_MAP", "{}")))
AUTH_USER_REGISTRATION_ROLE_JMESPATH = None

from platform_superset.security import PlatformSecurityManager   # reads the 'groups' claim, handles logout
CUSTOM_SECURITY_MANAGER = PlatformSecurityManager

# --- observability ------------------------------------------------------
from superset.stats_logger import StatsdStatsLogger
STATS_LOGGER = StatsdStatsLogger(host=os.environ.get("STATSD_HOST", "localhost"), port=9125, prefix="superset")
