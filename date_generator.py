from datetime import date
import calendar
from typing import Optional
import random
from scipy.stats import norm


class DateGenerator:
    @staticmethod
    def generate_date(begin_year: int, end_year: int):
        # Randomly choose year
        year = random.randint(begin_year, end_year)
        # Take one sample from normal distribution and check if it is within <1;12> range 
        mean, std = 6, 3
        while (month := int(norm(loc=mean, scale=std).rvs(1)[0])) < 1 or month > 12:
            pass
        # Get all days in month
        days_in_month = calendar.monthrange(year, month)[1]
        # Randomly choose a day until it is a working day
        day = random.randint(1, days_in_month-1)
        while (generated_date := date(year=year, month=month, day=day)).weekday() > 5:
            day = random.randint(1, days_in_month-1)
        return generated_date

    def __init__(self, begin_year: Optional[int] = 2023, end_year: Optional[int] = 2025, format: Optional[str] = "%Y-%m-%d"):
        self.begin_year = begin_year
        self.end_year = end_year
        self.format = format
    
    def __call__(self):
        # If generated date is in the future, regenerate it
        while (generated := DateGenerator.generate_date(self.begin_year, self.end_year)) > date.today():
            pass
        return generated.strftime(self.format)