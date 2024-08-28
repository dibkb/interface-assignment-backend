import json
import os
from datetime import datetime

def log_error(error: Exception, context: str = ""):
    error_log = {
        "timestamp": datetime.now().isoformat(),
        "error_type": type(error).__name__,
        "message": str(error),
        "context": context,
    }

    log_file = "error_log.json"

    # add to logfile
    if os.path.exists(log_file):
        with open(log_file, "r+") as file:
            logs = json.load(file)
            logs.append(error_log)
            file.seek(0)
            json.dump(logs, file, indent=4)
    else:
        with open(log_file, "w") as file:
            json.dump([error_log], file, indent=4)

    return error_log