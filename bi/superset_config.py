import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "dev-only-not-a-secret-change-me-0123456789abcdef")
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"
WTF_CSRF_ENABLED = False  # local dev only; API provisioning script uses JWT
TALISMAN_ENABLED = False
