from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    ENV: str = "production"
    KOPIS_API_KEY: str = ""
    KOPIS_BASE_URL: str = "http://www.kopis.or.kr/openApi/restful"
    DATABASE_URL: str = "sqlite+aiosqlite:///./festiverse.db"
    DEFAULT_GENRE: str = "CCCD"
    BATCH_HOUR: int = 3
    TICKET_BATCH_HOUR: int = 5
    CRAWL_DELAY_SECONDS: float = 2.0
    SEARCH_BATCH_LIMIT: int = 50
    SEARCH_DELAY_SECONDS: float = 3.0
    NAVER_CLIENT_ID: str = ""
    NAVER_CLIENT_SECRET: str = ""
    TEST_DATABASE_URL: str = ""

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
