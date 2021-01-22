from datetime import datetime, timezone

import pytz

MSK_TIMEZONE = pytz.timezone("Europe/Moscow")


def current_time():
    return str(MSK_TIMEZONE.localize(datetime.utcnow()))


