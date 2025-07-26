import os


class Settings:
    REDIS_HOST = os.getenv("REDIS_HOST", "redis")
    PROCESSOR_DEFAULT_URL = os.getenv(
        "PROCESSOR_DEFAULT_URL",
        "http://payment-processor-default:8080",
    )
    PROCESSOR_FALLBACK_URL = os.getenv(
        "PROCESSOR_FALLBACK_URL",
        "http://payment-processor-fallback:8080",
    )


settings = Settings()
