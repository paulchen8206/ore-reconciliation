import os

LOG_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "simple": {
            "format": "[%(asctime)s] [%(levelname)s] [%(name)s] "
            "[%(module)s:%(lineno)d] %(message)s"
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "DEBUG",
            "formatter": "simple",
            "stream": "ext://sys.stdout",
        },
        "file": {
            "class": "logging.FileHandler",
            "level": "DEBUG",
            "formatter": "simple",
            "filename": "log/ore_reconcile.log",
        }
    },
    "loggers": {
        "ORE_RECONCILIATION": {
            "level": os.getenv("LOG_LEVEL", "INFO"),
            "handlers": ["console", "file"],
        }
    },
}
