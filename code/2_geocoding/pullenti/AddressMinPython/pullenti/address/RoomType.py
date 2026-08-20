# SDK Pullenti Address (client version), version 4.28, february 2025.
# Copyright (c) 2013-2025, Pullenti. All rights reserved.
# Non-Commercial Freeware and Commercial Software.
# This class is generated using the converter Unisharping (www.unisharping.ru) from Pullenti C# project.
# The latest version of the code is available on the site www.pullenti.ru

from enum import IntEnum

class RoomType(IntEnum):
    """ Типы помещений """
    UNDEFINED = 0
    """ Не определено """
    SPACE = 1
    """ Помещение """
    FLAT = 2
    """ Квартира """
    OFFICE = 3
    """ Офис """
    ROOM = 4
    """ Комната """
    PAVILION = 9
    """ Павильон """
    GARAGE = 13
    """ Гараж (вроде это в House ???) """
    CARPLACE = 100
    """ Машиноместо """
    
    @classmethod
    def has_value(cls, value):
        return any(value == item.value for item in cls)