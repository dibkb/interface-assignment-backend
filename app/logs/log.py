import json
from datetime import datetime
import logging
from pathlib import Path

def log_error(error: Exception, context: str = "",level="ERROR", additional_info=None):
    print("Attempting to log error")
    error_log = {
        "timestamp": datetime.now().isoformat(),
        "level": level,
        "error_type": type(error).__name__,
        "message": str(error),
        "context": context,
        "additional_info": additional_info or {}
    }

    log_dir = Path("logs")
    log_file = log_dir / "error_log.json"

    try:
        log_dir.mkdir(exist_ok=True)

        if log_file.exists():
            with log_file.open("r+") as file:
                try:
                    logs = json.load(file)
                except json.JSONDecodeError:
                    logs = []
                logs.append(error_log)
                file.seek(0)
                file.truncate()
                json.dump(logs, file, indent=4)
        else:
            with log_file.open("w") as file:
                json.dump([error_log], file, indent=4)

        print(f"Error logged successfully to {log_file}")
    except Exception as e:
        print(f"Failed to log error to file: {str(e)}")
        logging.error(f"Failed to log error to file: {str(e)}")
        logging.error(f"Original error: {error_log}")

    return error_log