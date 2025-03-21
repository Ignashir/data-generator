from typing import Any
from faker import Faker

import re

class RuleBook:
    """Class that implements all rules for generation of values
    """
    def __init__(self, localization: str = "en_US"):
        fake = Faker(locale=localization)
        # TODO pomyslec jak zmienic to na cos ładniejszego
        # Each column name have a certain value presented by a tuple (value generator, index)
        # if index = -1 -> we want the whole output
        # if index = 0/1/2/... -> we want to split the output by spaces and retrieve only the ith element
        self.rules = {
        "name": (fake.name, 0), 
        "secondname": (fake.name, 1),
        "surname": (fake.name, 1), 
        "lastname": (fake.name, 1),
        "pesel": (fake.pesel, -1),
        "address": (fake.address, -1),
        "telephonenumber": (fake.phone_number, -1), 
        "telephonenum": (fake.phone_number, -1), 
        "phonenumber": (fake.phone_number, -1), 
        "phonenum": (fake.phone_number, -1),
        "licensenumber": (fake.identity_card_number, -1)
    }

    def generate_column_value(self, column_name: str) -> Any:
        # Lowercase whole string
        column_name = column_name.lower()
        # Delete all spaces, _, -, ...
        column_name = re.sub(r"[\_\-\s]+", "", column_name)
        try:
            value = self.rules[column_name]
            if value[1] == -1:
                return value[0]()
            else:
                return value[0]().split(" ")[value[1]]
        except KeyError:
            return None


BasicRuleBook = RuleBook("pl_PL")
PolishRuleBook = RuleBook("pl_PL")
