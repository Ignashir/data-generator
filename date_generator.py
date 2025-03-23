from datetime import date
import calendar
from typing import Optional
import random
from scipy.stats import norm


class DateGenerator:
    @staticmethod
    def generate_date(begin_year: int, end_year: int):
        year = random.randint(begin_year, end_year)
        mean, std = 6, 3
        while (month := int(norm(loc=mean, scale=std).rvs(1)[0])) < 1 or month > 12:
            pass
        days_in_month = calendar.monthrange(year, month)[1]
        day = random.randint(1, days_in_month-1)
        while (generated_date := date(year=year, month=month, day=day)).weekday() > 5:
            day = random.randint(1, days_in_month-1)
        return generated_date

    def __init__(self, begin_year: Optional[int] = 2023, end_year: Optional[int] = 2025, format: Optional[str] = "%Y-%m-%d"):
        self.begin_year = begin_year
        self.end_year = end_year
        self.format = format
    
    def __call__(self):
        while (generated := DateGenerator.generate_date(self.begin_year, self.end_year)) > date.today():
            pass
        return generated.strftime(self.format)