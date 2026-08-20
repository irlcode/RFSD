# SDK Pullenti Address (client version), version 4.28, february 2025.
# Copyright (c) 2013-2025, Pullenti. All rights reserved.
# Non-Commercial Freeware and Commercial Software.
# This class is generated using the converter Unisharping (www.unisharping.ru) from Pullenti C# project.
# The latest version of the code is available on the site www.pullenti.ru

from enum import IntEnum

class AddrLevel(IntEnum):
    """ Уровень адресного объекта """
    UNDEFINED = 0
    """ Не определено """
    COUNTRY = 1
    """ Страна """
    REGIONAREA = 2
    """ Регион """
    REGIONCITY = 3
    """ Город-регион """
    DISTRICT = 4
    """ Район (административный, муниципальный, городской) """
    SETTLEMENT = 5
    """ Поселение (сельское, городское) """
    CITY = 6
    """ Город """
    CITYDISTRICT = 7
    """ Городской район """
    LOCALITY = 8
    """ Населенный пункт """
    TERRITORY = 9
    """ Элемент планировочной структуры (территории организаций, СНТ и т.п.) """
    STREET = 10
    """ Элемент улично-дорожной сети """
    PLOT = 11
    """ Участок """
    BUILDING = 12
    """ Здание (сооружение) """
    APARTMENT = 13
    """ Помещение, квартира, офис и пр. в здании """
    ROOM = 14
    """ Комната в помещении """
    
    @classmethod
    def has_value(cls, value):
        return any(value == item.value for item in cls)