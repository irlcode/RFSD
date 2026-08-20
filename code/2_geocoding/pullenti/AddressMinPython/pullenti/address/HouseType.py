# SDK Pullenti Address (client version), version 4.28, february 2025.
# Copyright (c) 2013-2025, Pullenti. All rights reserved.
# Non-Commercial Freeware and Commercial Software.
# This class is generated using the converter Unisharping (www.unisharping.ru) from Pullenti C# project.
# The latest version of the code is available on the site www.pullenti.ru

from enum import IntEnum

class HouseType(IntEnum):
    """ Типы зданий и участков """
    UNDEFINED = 0
    """ Не определено """
    ESTATE = 1
    """ Владение """
    HOUSE = 2
    """ Дом """
    HOUSEESTATE = 3
    """ Домовладение """
    SPECIAL = 4
    """ Специальное строение (типа АЗС) """
    GARAGE = 5
    """ Гараж """
    WELL = 6
    """ Скважина (для месторождений) """
    MINE = 7
    """ Шахта """
    BOILER = 8
    """ Котельная """
    UNFINISHED = 9
    """ Объект незавершенного строительства """
    
    @classmethod
    def has_value(cls, value):
        return any(value == item.value for item in cls)