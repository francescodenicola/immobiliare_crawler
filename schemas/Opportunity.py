from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, ValidationError, validator


class Opportunity(BaseModel):
    DATE: str
    COUNTRY: str
    REGION: str
    PROVINCE: str
    CITY: str
    MICROZONE: str
    ID: str
    URL: str
    ADDRESS: str
    MQ: str
    FLOOR: str
    PRICE: str
    AGENCY: str
    