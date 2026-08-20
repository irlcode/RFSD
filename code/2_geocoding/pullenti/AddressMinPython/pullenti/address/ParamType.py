# SDK Pullenti Address (client version), version 4.28, february 2025.
# Copyright (c) 2013-2025, Pullenti. All rights reserved.
# Non-Commercial Freeware and Commercial Software.
# This class is generated using the converter Unisharping (www.unisharping.ru) from Pullenti C# project.
# The latest version of the code is available on the site www.pullenti.ru

from enum import IntEnum

class ParamType(IntEnum):
    """ Дополнительные параметры адреса """
    UNDEFINED = 0
    ORDER = 1
    """ Очередь (например, в ГСК) """
    PART = 2
    """ Часть """
    FLOOR = 3
    """ Этаж """
    DELIVERYAREA = 4
    """ Доставочный участок """
    ZIP = 5
    """ Индекс """
    SUBSCRIBERBOX = 6
    """ Абон.ящик """
    ORGANIZATION = 7
    """ Организация в здании или помещении (БЦ, гипермаркет, ателье...) """
    METRO = 8
    """ Станция метро """
    
    @classmethod
    def has_value(cls, value):
        return any(value == item.value for item in cls)