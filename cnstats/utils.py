from datetime import datetime
from dateutil.relativedelta import relativedelta


def get_periods(start_month: str, end_month: str):
    """
    >>> periods("202001", "202003")
    ['202001', '202002', '202003']
    """
    start = datetime.strptime(start_month, "%Y%m")
    end = datetime.strptime(end_month, "%Y%m")
    return (
        datetime.strftime(start + relativedelta(months=i), "%Y%m")
        for i in range((end.year - start.year) * 12 + end.month - start.month + 1)
    )
