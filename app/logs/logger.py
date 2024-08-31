from typing import Union, Optional
from fastapi import Request
from ..db.database import get_db_context, get_db_from_request
from ..db.schema.error_log import ErrorLog, LevelType
import datetime


def log_errors(
    error: Union[str, Exception],
    context: str = "",
    level=LevelType.ERROR,
    additional_info=None,
    request: Optional[Request] = None
):
    with get_db_context() as db:
        if request:
            db = get_db_from_request(request)

        error_log = ErrorLog(
            timestamp=datetime.datetime.now(),
            level=level,
            error_type=type(error).__name__ if isinstance(
                error,
                Exception) else "INFO",
            message=str(error),
            context=context,
            additional_info=additional_info or {})
        db.add(error_log)
        db.commit()

    return {"message": str(error), "context": context}
