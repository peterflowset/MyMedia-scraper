import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Config:
    outscraper_api_key: str
    openrouter_api_key: str
    google_service_account_file: str
    google_drive_folder_id: str

    @classmethod
    def from_env(cls) -> "Config":
        outscraper_key = os.getenv("OUTSCRAPER_API_KEY", "")
        openrouter_key = os.getenv("OPENROUTER_API_KEY", "")
        service_account = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "credentials/service_account.json")
        drive_folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "")

        missing = []
        if not outscraper_key:
            missing.append("OUTSCRAPER_API_KEY")
        if not openrouter_key:
            missing.append("OPENROUTER_API_KEY")

        if missing:
            raise ValueError(f"Fehlende Umgebungsvariablen: {', '.join(missing)}")

        return cls(
            outscraper_api_key=outscraper_key,
            openrouter_api_key=openrouter_key,
            google_service_account_file=service_account,
            google_drive_folder_id=drive_folder_id,
        )
