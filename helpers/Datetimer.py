import datetime
from datetime import timedelta
import pytz


def get_local_time() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc).astimezone(pytz.timezone('Asia/Taipei'))

def get_local_date(date_delta = 0) -> datetime.date:
    return get_local_time().date() + timedelta(days=date_delta)

def get_last_weeks_day(day = 1) -> datetime.date:
    """ Get the date of the last week's day

    Args:
        day (int, optional): [description]. Defaults to 0, equal to Monday.

    Returns:
        datetime.date: Date value of the requested date
    """ 
    current_time = get_local_time()

    return (current_time.date() - datetime.timedelta(days=current_time.weekday()) + datetime.timedelta(days=day - 1, weeks=-1))

def get_this_weeks_day(day = 1) -> datetime.date:
    """ Get the date of the last week's day

    Args:
        day (int, optional): [description]. Defaults to 0, equal to Monday.

    Returns:
        datetime.date: Date value of the requested date
    """ 
    current_time = get_local_time()

    return current_time.date() -datetime.timedelta(days=current_time.weekday()) + datetime.timedelta(days=day - 1)