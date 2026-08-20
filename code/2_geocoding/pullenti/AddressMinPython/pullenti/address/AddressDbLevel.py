# SDK Pullenti Address (client version), version 4.28, february 2025.
# Copyright (c) 2013-2025, Pullenti. All rights reserved.
# Non-Commercial Freeware and Commercial Software.
# This class is generated using the converter Unisharping (www.unisharping.ru) from Pullenti C# project.
# The latest version of the code is available on the site www.pullenti.ru

from enum import IntEnum

class AddressDbLevel(IntEnum):
    """ Уровень адресного элемента в записи БД """
    UNDEFINED = 0
    """ Не определено """
    COUNTRY = 1
    """ Страна """
    REGION = 2
    """ Регион, область, край """
    CITY = 3
    """ Город """
    DISTRICT = 4
    """ Район """
    LOCALITY = 5
    """ Населенный пункт """
    TERRITORY = 6
    """ Территория """
    STREET = 7
    """ Улица """
    HOUSE = 8
    """ Дом, земельный участок """
    APARTMENT = 9
    """ Квартира, комната, помещение, машиноместо... """
    
    @classmethod
    def has_value(cls, value):
        return any(value == item.value for item in cls)