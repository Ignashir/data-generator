from datetime import datetime, time, timedelta
from scipy.stats import norm
from typing import Optional


class HourGenerator:
    @staticmethod
    def generate_hours(begin: int, end: int, format = str):
        """Function generating hours (tailored to yield 2 hour variables that are some time apart)
        It is important that when you want to generate 2 columns with hours, you place them in such order (earlier hour, later hour)
        """
        while (begin_h := norm(loc=13, scale=3).rvs(1)[0]) < begin or begin_h > end:
            pass
        hours = int(begin_h)
        minutes = int((begin_h - hours) * 60)
        begin_time = time(hour=hours, minute=minutes)
        while True:
            yield begin_time.strftime(format)
            duration = norm(loc=45, scale=10).rvs(1)[0]
            end_time = timedelta(minutes=int(duration))
            begin_time = (datetime.combine(datetime.today(), begin_time) + end_time).time()

    def __init__(self, /, reset_limit: Optional[int] = 2, begin: Optional[int] = 8, end: Optional[int] = 18, format: Optional[str] = "%H:%M"):
        self.limit = reset_limit
        self.count = 0
        self.func = HourGenerator.generate_hours
        self.begin_arg = begin
        self.end_arg = end
        self.format = format
        self.func_gen = self.func(self.begin_arg, self.end_arg, self.format)

    def __next__(self):
        if self.count == self.limit:
            self.count = 0
            self.func_gen = self.func(self.begin_arg, self.end_arg, self.format)
        self.count += 1
        return next(self.func_gen)
        

    def __iter__(self):
        return self
    
    def __call__(self):
        return self.__next__()