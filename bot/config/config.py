from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_ignore_empty=True)

    API_ID: int
    API_HASH: str

    USE_RANDOM_DELAY_IN_RUN: bool = False
    RANDOM_DELAY_IN_RUN: list[int] = [5, 39600]

    REF_LINK: str = "https://t.me/cexio_tap_bot?start=1716965343267932"

    AUTO_TAP: bool = True
    RANDOM_TAPS_COUNT: list = [25, 75]
    SLEEP_BETWEEN_TAPS: list = [25, 35]
    SLEEP_BY_MIN_ENERGY: int = 250
    AUTO_CONVERT: bool = True
    MINIMUM_TO_CONVERT: float = 0.1
    AUTO_BUY_UPGRADE: bool = True
    AUTO_TASK: bool = True
    AUTO_CLAIM_SQUAD_BONUS: bool = False

    USE_PROXY: bool = True


settings = Settings()
