# SDK Pullenti Address (client version), version 4.28, february 2025.
# Copyright (c) 2013-2025, Pullenti. All rights reserved.
# Non-Commercial Freeware and Commercial Software.
# This class is generated using the converter Unisharping (www.unisharping.ru) from Pullenti C# project.
# The latest version of the code is available on the site www.pullenti.ru

from enum import IntEnum

class GarParam(IntEnum):
    """ Типы параметров ГАР """
    UNDEFINED = 0
    """ Не определён """
    GUID = 1
    """ Уникальный GUID (из ГАР) """
    KLADRCODE = 2
    """ Код КЛАДР (тип 10 в ГАР) """
    POSTINDEX = 3
    """ Почтовый индекс (тип 5 в ГАР) """
    OKATO = 4
    """ Код ОКАТО (тип 6 в ГАР) """
    OKTMO = 5
    """ Код ОКТМО (тип 7 в ГАР) """
    KADASTERNUMBER = 6
    """ Кадастровый номер (тип 8) """
    REESTERNUMBER = 7
    """ Реестровый номер (тип 13) """
    OBJECTID = 8
    """ Внутренний числовой идентификатор ГАР (пригодится для формирования
    URL на объект в самом ГАР) """
    GPSPOINT = 100
    """ Точка GPS в формате "Lat Lon" """
    GPSRECTANGLE = 101
    """ Прямоугольная область в формате "LatMin-LatMax LonMin-LonMax" """
    PURPOSE = 104
    """ Назначение сооружения """
    FLOORS = 105
    """ Этажность, если есть подвальная часть, то задаётся через прямой слэш """
    YEAR = 106
    """ Год постройки или ввода в эксплуатацию """
    
    @classmethod
    def has_value(cls, value):
        return any(value == item.value for item in cls)