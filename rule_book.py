from typing import Any, Optional
from faker import Faker
import random
from scipy.stats import norm
from datetime import time, datetime, timedelta

import re
import warnings

from .hours_generator import HourGenerator

class RuleBook:
    """Class that implements all rules for generation of values
    """
    def __init__(self, localization: str = "en_US"):
        fake = Faker(locale=localization)
        self.hour_generator = HourGenerator(reset_limit=2, begin=8, end=18)
        # TODO pomyslec jak zmienic to na cos ładniejszego
        # Each column name have a certain value presented by a tuple (value generator, index)
        # if index = -1 -> we want the whole output
        # if index = 0/1/2/... -> we want to split the output by spaces and retrieve only the ith element
        self.rules = {
        "name": (fake.first_name, -1), 
        "secondname": (fake.last_name, -1),
        "surname": (fake.last_name, -1), 
        "lastname": (fake.last_name, 1),
        "pesel": (fake.pesel, -1),
        "address": (fake.address, -1),
        "telephonenumber": (fake.phone_number, -1), 
        "telephonenum": (fake.phone_number, -1), 
        "phonenumber": (fake.phone_number, -1), 
        "phonenum": (fake.phone_number, -1),
        "licensenumber": (fake.identity_card_number, -1),
        "gearbox": (lambda : fake.boolean(60), -1),
        "brand": (lambda : fake.random_element(elements = ["Toyota", "Suzuki", "Renault", "Ford", "Opel", "Skoda", "Kia", "Volkswagen"]), -1),
        "cartype": (lambda : fake.random_element(elements = ["car", "truck", "motorcycle"]), -1),
        "inspectiondate": (lambda : fake.date_between(start_date="-1y", end_date="-1w").strftime("%d-%m-%Y"), -1),
        "examdate": (lambda : fake.date_between(start_date="-3y", end_date="-1w").strftime("%d-%m-%Y"), -1),
        "beginhour": (self.hour_generator, -1),
        "endhour": (self.hour_generator, -1),
        "result": (lambda : fake.boolean(70), -1),
        "category": (lambda : fake.random_element(elements= ["AM", "A1", "A2", "B1", "B", "B+E" , "C", "C+E"]), -1),
        "type": (lambda : fake.boolean(55), -1),
        "examcomment": (lambda : fake.text(max_nb_chars=200), -1),
        "registrationnumber": (lambda : fake.bothify(text='??######', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ'), -1),
        "dateofendofwork": (lambda : fake.date_between(start_date="+1w", end_date="+1y").strftime("%d-%m-%Y"), -1),
        "dateofacceptance": (lambda : fake.date_between(start_date="-5y", end_date="-1w").strftime("%d-%m-%Y"), -1),
        "comments": (lambda : fake.text(20), -1),
        "examid": (lambda : fake.random_int(min=1000, max=9999), -1),
        "reservationdate": (lambda : fake.date_between(start_date="-3w").strftime("%d-%m-%Y"), -1),
        "reservationhour": (lambda : fake.time(pattern="%H:%M"), -1),
        "examtype": (lambda : fake.boolean(50), -1), 
        "assignedexaminerid": (fake.identity_card_number, -1)
    }   

    def generate_column_value(self, column_name_: str) -> Any:
        # Lowercase whole string
        column_name = column_name_.lower()
        # Delete all spaces, _, -, ...
        column_name = re.sub(r"[\_\-\s]+", "", column_name)
        try:
            value = self.rules[column_name]
            if value[1] == -1:
                res = value[0]()
                if isinstance(res, int): return res
                res = re.sub(r"[\s]+", "", res)
                return res
            else:
                return value[0]().split(" ")[value[1]]
        except KeyError:
            warnings.warn(f"{column_name_} not in rules. Change column name or add new rule")
            return None


BasicRuleBook = RuleBook("pl_PL")
PolishRuleBook = RuleBook("pl_PL")
