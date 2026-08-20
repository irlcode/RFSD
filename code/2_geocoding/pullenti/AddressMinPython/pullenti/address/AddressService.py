# SDK Pullenti Address (client version), version 4.28, february 2025.
# Copyright (c) 2013-2025, Pullenti. All rights reserved.
# Non-Commercial Freeware and Commercial Software.
# This class is generated using the converter Unisharping (www.unisharping.ru) from Pullenti C# project.
# The latest version of the code is available on the site www.pullenti.ru

import typing
from pullenti.unisharp.Utils import Utils

from pullenti.address.MessageCode import MessageCode
from pullenti.address.Message import Message

class AddressService:
    """ Сервис работы с адресами
    
    """
    
    VERSION = "4.32"
    """ Текущая версия """
    
    VERSION_DATE = "2025.11.06"
    """ Дата создания текущей версии """
    
    @staticmethod
    def get_gar_statistic() -> 'GarStatistic':
        """ Получить информацию по индексу и его объектам
        
        """
        from pullenti.address.internal.ServerHelper import ServerHelper
        try: 
            if (ServerHelper.SERVER_URI is not None): 
                return ServerHelper.get_gar_statistic()
            return None
        except Exception as ex: 
            return None
    
    @staticmethod
    def set_server_connection(uri : str) -> bool:
        """ Для работы установить связь с сервером и все запросы делать через него
        (используется для ускорения работы для JS и Python)
        
        Args:
            uri(str): например, http://localhost:2222, если null, то связь разрывается
        
        """
        from pullenti.address.internal.ServerHelper import ServerHelper
        if (uri is None): 
            ServerHelper.SERVER_URI = (None)
            return True
        if (not uri.startswith("http")): 
            uri = ("http://" + uri)
        ver = ServerHelper.get_server_version(uri)
        if (ver is None): 
            ServerHelper.SERVER_URI = (None)
            return False
        else: 
            ServerHelper.SERVER_URI = uri
            return True
    
    @staticmethod
    def get_server_uri() -> str:
        """ Если связь с сервером установлена, то вернёт адрес
        
        """
        from pullenti.address.internal.ServerHelper import ServerHelper
        return ServerHelper.SERVER_URI
    
    @staticmethod
    def get_server_version(uri : str) -> str:
        """ Получить версию SDK на сервере
        
        Args:
            uri(str): 
        
        Returns:
            str: версия или null при недоступности сервера
        """
        from pullenti.address.internal.ServerHelper import ServerHelper
        return ServerHelper.get_server_version(uri)
    
    @staticmethod
    def process_text(txt : str, pars : 'ProcessTextParams'=None) -> typing.List['TextAddress']:
        """ Обработать произвольный текст, в котором есть адреса
        
        Args:
            txt(str): текст
            pars(ProcessTextParams): дополнительные параметры (null - дефолтовые)
        
        Returns:
            typing.List[TextAddress]: результат - для каждого найденного адреса свой экземпляр
        
        """
        from pullenti.address.internal.ServerHelper import ServerHelper
        try: 
            if (ServerHelper.SERVER_URI is not None): 
                return ServerHelper.process_text(txt, pars)
            return None
        except Exception as ex: 
            return None
    
    @staticmethod
    def process_single_address_text(txt : str, pars : 'ProcessTextParams'=None) -> 'TextAddress':
        """ Обработать текст с одним адресом (адресное поле)
        
        Args:
            txt(str): исходный текст
            pars(ProcessTextParams): дополнительные параметры (null - дефолтовые)
        
        Returns:
            TextAddress: результат обработки
        
        """
        from pullenti.address.TextAddress import TextAddress
        from pullenti.address.internal.ServerHelper import ServerHelper
        try: 
            if (ServerHelper.SERVER_URI is not None): 
                return ServerHelper.process_single_address_text(txt, pars)
            return None
        except Exception as ex: 
            res = TextAddress._new2(txt)
            res.messages.append(Message(MessageCode.PROGRAMERROR, str(ex), True))
            return res
    
    @staticmethod
    def process_single_address_texts(txts : typing.List[str], pars : 'ProcessTextParams'=None) -> typing.List['TextAddress']:
        """ Обработать порцию адресов. Использовать в случае сервера, посылая ему порцию на обработку
        (не более 100-300 за раз), чтобы сократить время на издержки взаимодействия.
        Для обычной работы (не через сервер) это эквивалентно вызову в цикле ProcessSingleAddressText
        и особого смысла не имеет.
        
        Args:
            txts(typing.List[str]): список адресов
            pars(ProcessTextParams): дополнительные параметры (null - дефолтовые)
        
        Returns:
            typing.List[TextAddress]: результат (количество совпадает с исходным списком), если null, то какая-то ошибка
        """
        from pullenti.address.internal.ServerHelper import ServerHelper
        try: 
            if (ServerHelper.SERVER_URI is not None): 
                return ServerHelper.process_single_address_texts(txts, pars)
            return None
        except Exception as ex: 
            return None
    
    @staticmethod
    def process_single_address_texts_records(txts : typing.List[str], pars : 'ProcessTextParams'=None) -> typing.List['AddressDbRecord']:
        """ Пробразовать порцию адресов сразу в представление для записи в БД
        (фактически вызываются ProcessSingleAddressTexts и затем для каждого AddressDbRecord.AddressDbRecord,
        для случая взаимодействия с сервером так получится эффективнее)
        
        Args:
            txts(typing.List[str]): список адресов
            pars(ProcessTextParams): дополнительные параметры (null - дефолтовые)
        
        Returns:
            typing.List[AddressDbRecord]: результат (количество совпадает с исходным списком), если null, то какая-то ошибка
        """
        from pullenti.address.internal.ServerHelper import ServerHelper
        try: 
            if (ServerHelper.SERVER_URI is not None): 
                return ServerHelper.process_single_address_texts_records(txts, pars)
            return None
        except Exception as ex: 
            return None
    
    @staticmethod
    def search_objects(search_pars : 'SearchParams') -> 'SearchResult':
        """ Искать объекты (для выпадающих списков)
        
        Args:
            search_pars(SearchParams): параметры запроса
        
        Returns:
            SearchResult: результат
        """
        from pullenti.address.internal.ServerHelper import ServerHelper
        try: 
            if (search_pars is None): 
                return None
            if (ServerHelper.SERVER_URI is not None): 
                return ServerHelper.search_objects(search_pars)
            return None
        except Exception as ex: 
            return None
    
    @staticmethod
    def get_children_objects(obj_id : str, ignore_houses : bool=False) -> typing.List['GarObject']:
        """ Получить список дочерних объектов для ГАР-объекта
        
        Args:
            obj_id(str): идентификатор объект ГАР (если null, то вернёт объекты первого уровня - регионы)
            ignore_houses(bool): игнорировать дома и помещения
        
        Returns:
            typing.List[GarObject]: дочерние объекты
        """
        from pullenti.address.internal.ServerHelper import ServerHelper
        try: 
            if (ServerHelper.SERVER_URI is not None): 
                return ServerHelper.get_children_objects(obj_id, ignore_houses)
            return None
        except Exception as ex3: 
            return None
    
    @staticmethod
    def get_object(obj_id : str) -> 'GarObject':
        """ Получить объект по внутреннему идентификатору (он может меняться от версии к версии индекса,
        долгосрочно привязываться к нему НЕЛЬЗЯ)
        
        Args:
            obj_id(str): внутренний идентификатор объекта ГАР
        
        Returns:
            GarObject: объект
        """
        from pullenti.address.internal.ServerHelper import ServerHelper
        if (Utils.isNullOrEmpty(obj_id)): 
            return None
        try: 
            if (ServerHelper.SERVER_URI is not None): 
                return ServerHelper.get_object(obj_id)
            return None
        except Exception as ex4: 
            return None