from datetime import datetime
import json


class DateTimeEncoder(json.JSONEncoder):
    """JSON Encoder that handles datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)