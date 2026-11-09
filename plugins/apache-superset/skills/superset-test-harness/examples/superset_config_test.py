"""Test overrides on top of the platform production config."""
from superset_config import *          # noqa: F401,F403  (production config from the image)
import os

TESTING = True
WTF_CSRF_ENABLED = True
TALISMAN_ENABLED = True
GUEST_TOKEN_JWT_EXP_SECONDS = 120
CACHE_DEFAULT_TIMEOUT = 30
for _c in (CACHE_CONFIG, DATA_CACHE_CONFIG, FILTER_STATE_CACHE_CONFIG, EXPLORE_FORM_DATA_CACHE_CONFIG):  # noqa: F405
    _c["CACHE_DEFAULT_TIMEOUT"] = 30
ROW_LIMIT = 1000
SQLLAB_TIMEOUT = 15
FEATURE_FLAGS.update({"EMBEDDED_SUPERSET": True, "ALERT_REPORTS": True, "THUMBNAILS": True, "DASHBOARD_RBAC": True})  # noqa: F405

# DB auth users in addition to OIDC so API tests can log in without a browser (light harness path)
AUTH_TYPE = AUTH_OAUTH if os.environ.get("HARNESS") == "full" else 0      # 0 = AUTH_DB
AUTH_USER_REGISTRATION_ROLE = None

# Alerts & Reports to Mailpit, real execution
ALERT_REPORTS_NOTIFICATION_DRY_RUN = False
SMTP_HOST = os.environ.get("SMTP_HOST", "mailpit"); SMTP_PORT = 1025; SMTP_STARTTLS = False; SMTP_SSL = False
SMTP_MAIL_FROM = "reports@test.local"
WEBDRIVER_BASEURL = "http://superset:8088/"
WEBDRIVER_BASEURL_USER_FRIENDLY = "http://localhost:8088/"
