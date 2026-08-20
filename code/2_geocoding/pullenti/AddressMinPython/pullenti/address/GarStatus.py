# SDK Pullenti Address (client version), version 4.28, february 2025.
# Copyright (c) 2013-2025, Pullenti. All rights reserved.
# Non-Commercial Freeware and Commercial Software.
# This class is generated using the converter Unisharping (www.unisharping.ru) from Pullenti C# project.
# The latest version of the code is available on the site www.pullenti.ru

from enum import IntEnum

class GarStatus(IntEnum):
    """ Статус анализа ГАР-объекта (наименования, номера) """
    OK = 0
    """ Анализ без замечаний """
    WARNING = 1
    """ Анализ прошёл с замечаниями, возможна непривязка к объекту """
    ERROR = 2
    """ Анализ не прошёл, к объекту привязка производиться не будет """
    OK2 = 3
    """ Анализ без замечаний, но в ГАР-объекте слепились 2 разных объекта (нужно дополнительно спец. обработку при проверке) """
    
    @classmethod
    def has_value(cls, value):
        return any(value == item.value for item in cls)