from pydantic import BaseModel,ValidationError, validator
from typing import Optional

class Mapping(BaseModel):
    Country: str
    Region: str
    Province: str
    City: str
    Microzone: str
    id: str
    url: str
    Agency: str
    Address: str
    MQ: str
    Range: Optional[str] = ""
    Price: str
    Floor: str
    Auction: Optional[int] = 0
    Toilet: Optional[int] = 0
    Date: str

class Opportunity(BaseModel):
    Owner:Optional[str] = ''
    Data_opp: str
    Stato_opp:Optional[str] = 'Nuova'
    Country: str
    Region: str
    Province: str
    City: str
    Microzone: str
    id: str
    url: str
    Address: str
    MQ: str
    Range: str
    Price: str
    Floor: str
    Auction: Optional[int] = 0
    Toilet: Optional[int] = 0
    Date: str


class Range(BaseModel):
    min:int
    max:int
    range:str
    abbattimento:float
    