# SDK Pullenti Address (client version), version 4.28, february 2025.
# Copyright (c) 2013-2025, Pullenti. All rights reserved.
# Non-Commercial Freeware and Commercial Software.
# This class is generated using the converter Unisharping (www.unisharping.ru) from Pullenti C# project.
# The latest version of the code is available on the site www.pullenti.ru

from enum import IntEnum

class GarLevel(IntEnum):
    """ Уровень адресного объекта ГАР """
    UNDEFINED = 0
    REGION = 1
    """ Регион """
    ADMINAREA = 2
    """ Административный район """
    MUNICIPALAREA = 3
    """ Муниципальный район """
    SETTLEMENT = 4
    """ Сельское/городское поселение """
    CITY = 5
    """ Город """
    LOCALITY = 6
    """ Населенный пункт """
    DISTRICT = 14
    """ Район """
    AREA = 7
    """ Элемент планировочной структуры """
    STREET = 8
    """ Элемент улично-дорожной сети """
    PLOT = 9
    """ Земельный участок """
    BUILDING = 10
    """ Здание (сооружение) """
    ROOM = 11
    """ Помещение """
    CARPLACE = 17
    """ Машино-место """
    
    @classmethod
    def has_value(cls, value):
        return any(value == item.value for item in cls)