# SDK Pullenti Address (client version), version 4.28, february 2025.
# Copyright (c) 2013-2025, Pullenti. All rights reserved.
# Non-Commercial Freeware and Commercial Software.
# This class is generated using the converter Unisharping (www.unisharping.ru) from Pullenti C# project.
# The latest version of the code is available on the site www.pullenti.ru

from enum import IntEnum

class DetailType(IntEnum):
    """ Тип детализирующего указателя """
    UNDEFINED = 0
    """ Не определено """
    NEAR = 1
    """ Около, в районе """
    NORTH = 2
    """ Направление на север """
    EAST = 3
    """ Направление на восток """
    SOUTH = 4
    """ Направление на юг """
    WEST = 5
    """ Направление на запад """
    NORTHWEST = 6
    """ Направление на северо-запад """
    NORTHEAST = 7
    """ Направление на северо-восток """
    SOUTHWEST = 8
    """ Направление на юго-запад """
    SOUTHEAST = 9
    """ Направление на юго-восток """
    CENTRAL = 10
    """ Центральная часть """
    LEFT = 11
    """ Левая часть """
    RIGHT = 12
    """ Правая часть """
    KMRANGE = 13
    """ Километровый диапазон (км.455+990-км.456+830) """
    
    @classmethod
    def has_value(cls, value):
        return any(value == item.value for item in cls)