# SDK Pullenti Address (client version), version 4.28, february 2025.
# Copyright (c) 2013-2025, Pullenti. All rights reserved.
# Non-Commercial Freeware and Commercial Software.
# This class is generated using the converter Unisharping (www.unisharping.ru) from Pullenti C# project.
# The latest version of the code is available on the site www.pullenti.ru

from enum import IntEnum

class MessageCode(IntEnum):
    """ Код сообщение """
    NOADDRESS = 0
    """ Не адрес """
    PROGRAMERROR = 1
    """ Ошибка в программе """
    MANYADDRESSES = 2
    """ В строке несколько адресов """
    UNDEFINEDFRAGMENT = 3
    """ Нераспознанный фрагмент текста """
    UNDEFINEDOBJECT = 4
    """ Нераспознанный непонятный объект """
    NOCOUNTRY = 5
    """ Страна не определена """
    BADFIRSTOBJECT = 6
    """ Первый объект слишком низкого уровня """
    BADOBJECTLEVEL = 7
    """ Объект имеет непонятный уровень """
    BADOBJECT = 8
    """ Объект указан в адресе неправильно """
    BADHIERARCHY = 9
    """ Некорректная иерархия объектов """
    EMPTYGARATTACH = 10
    """ Объект не привязался к ГАР """
    MANYGARATTACH = 11
    """ К объекту привязалось много ГАР объектов """
    CHANGEREGION = 12
    """ Смена региона (например, была МО, а стала Москва) - предупреждение """
    CORPUSORFLAT = 13
    """ Возможно, не корпус, а квартира (когда в тексте "к" после дома) """
    
    @classmethod
    def has_value(cls, value):
        return any(value == item.value for item in cls)