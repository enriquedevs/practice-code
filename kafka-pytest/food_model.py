from pydantic import BaseModel
from typing import List


class SubGroup(BaseModel):
    name: str


class Group(BaseModel):
    name: str
    sub_group: List[SubGroup]


class Food(BaseModel):
    name: str
    scientific_name: str
    group_name: Group

