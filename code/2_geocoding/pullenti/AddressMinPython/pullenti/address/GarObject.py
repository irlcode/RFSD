# SDK Pullenti Address (client version), version 4.28, february 2025.
# Copyright (c) 2013-2025, Pullenti. All rights reserved.
# Non-Commercial Freeware and Commercial Software.
# This class is generated using the converter Unisharping (www.unisharping.ru) from Pullenti C# project.
# The latest version of the code is available on the site www.pullenti.ru

import typing
import io
import math
import xml.etree
from pullenti.unisharp.Utils import Utils
from pullenti.unisharp.Misc import RefOutArgWrapper
from pullenti.unisharp.Xml import XmlWriter

from pullenti.address.GarParam import GarParam
from pullenti.address.GpsObject import GpsObject
from pullenti.address.AddressService import AddressService
from pullenti.address.BaseAttributes import BaseAttributes
from pullenti.address.GarLevel import GarLevel
from pullenti.address.HouseAttributes import HouseAttributes
from pullenti.address.RoomAttributes import RoomAttributes
from pullenti.address.GarStatus import GarStatus
from pullenti.address.AddressHelper import AddressHelper
from pullenti.address.AreaAttributes import AreaAttributes

class GarObject(object):
    """ Адресный объект ГАР
    
    """
    
    def __init__(self, attrs_ : 'BaseAttributes') -> None:
        self.attrs = None;
        self.level = GarLevel.UNDEFINED
        self.expired = False
        self.guid = None;
        self.region_number = 0
        self.status = GarStatus.OK
        self.id0_ = None;
        self.parent_ids = list()
        self.actual_object_id = None;
        self.source_text = None;
        self.tag = None;
        self.tag2 = None;
        self._internal_tag = 0
        self.internal_byte = 0
        self.children_count = 0
        self.__m_params = None
        self.__m_full_path = None;
        self.attrs = attrs_
    
    def __str__(self) -> str:
        if (self.attrs is None): 
            return "?"
        aa = Utils.asObjectOrNull(self.attrs, AreaAttributes)
        if (aa is not None and len(aa.types) > 0 and len(aa.names) > 0): 
            return "{0} {1}".format(aa.types[0], aa.names[0])
        if (self.source_text is not None): 
            return self.source_text
        return str(self.attrs)
    
    def get_param_value(self, ty : 'GarParam') -> str:
        """ Получить значение параметра (код КЛАДР, почтовый индекс и т.п.)
        
        Args:
            ty(GarParam): тип параметра
        
        Returns:
            str: значение или null
        """
        if (ty == GarParam.GUID): 
            return self.guid
        self.__load_params()
        res = None
        wrapres9 = RefOutArgWrapper(None)
        inoutres10 = Utils.tryGetValue(self.__m_params, ty, wrapres9)
        res = wrapres9.value
        if (self.__m_params is not None and inoutres10): 
            return res
        return None
    
    def get_params(self) -> typing.List[tuple]:
        """ Получить все параметры
        
        """
        self.__load_params()
        return self.__m_params
    
    def out_info(self, res : io.StringIO, out_name_analyze_info : bool=True) -> None:
        """ Вывести подробную текстовую информацию об объекте (для отладки)
        
        Args:
            res(io.StringIO): куда выводить
        """
        if (self.attrs is not None): 
            self.attrs.out_info(res)
        print("\r\nУровень: {0} - {1}\r\n".format(self.level, AddressHelper.get_gar_level_string(self.level)), end="", file=res, flush=True)
        if (self.expired): 
            print("Актуальность: НЕТ\r\n".format(), end="", file=res, flush=True)
        if (self.__m_params is None): 
            self.__load_params()
        if (self.__m_params is not None): 
            for p in self.__m_params.items(): 
                print("{0}: {1}".format(AddressHelper.get_gar_param_string(p[0]), p[1]), end="", file=res, flush=True)
                if (p[0] == GarParam.GPSRECTANGLE): 
                    gps = GpsObject(p[1])
                    lat = round(gps.calc_lat_km(), 3)
                    lon = round(gps.calc_lon_km(), 3)
                    squ = gps.calc_square()
                    print(" ({0} x {1} = {2} km2)".format(GpsObject.out_double(lat), GpsObject.out_double(lon), GpsObject.out_double(squ)), end="", file=res, flush=True)
                print("\r\n", end="", file=res)
        print("\r\nПолный путь: {0}\r\n".format(self.get_full_path(None, False, None)), end="", file=res, flush=True)
        if (self.actual_object_id is not None and out_name_analyze_info): 
            act_obj = AddressService.get_object(self.actual_object_id)
            if (act_obj is not None): 
                print("\r\n\r\n-------------------------------------------------\r\nДанный объект был заменён более новым объектом:\r\n".format(), end="", file=res, flush=True)
                act_obj.out_info(res, False)
                print("\r\n-------------------------------------------------\r\n".format(), end="", file=res, flush=True)
    
    def __load_params(self) -> None:
        from pullenti.address.internal.ServerHelper import ServerHelper
        if (self.__m_params is not None): 
            return
        self.__m_params = dict()
        pars = None
        if (ServerHelper.SERVER_URI is not None): 
            pars = ServerHelper.get_object_params(self.id0_)
        if (pars is not None): 
            for kp in pars.items(): 
                if (kp[0] != GarParam.GUID): 
                    self.__m_params[kp[0]] = kp[1]
        if (not GarParam.GUID in self.__m_params): 
            self.__m_params[GarParam.GUID] = self.guid
    
    def get_full_path(self, delim : str=None, correct : bool=False, addr : 'TextAddress'=None) -> str:
        """ Получить полную строку адреса с учётом родителей
        
        Args:
            delim(str): разделитель (по умолчанию, запятая с пробелом)
            correct(bool): корректировать исходные наимeнования
            addr(TextAddress): если объект выделен внутри адреса, то для скорости можно указать этот адрес, чтобы родителей искал там, а не в индексе
        
        Returns:
            str: результат
        """
        if (self.__m_full_path is not None): 
            return self.__m_full_path
        if (delim is None): 
            delim = ", "
        path = list()
        o = self
        while o is not None: 
            path.insert(0, o)
            if (len(o.parent_ids) == 0): 
                break
            if (addr is not None): 
                oo = addr.find_gar_by_ids(o.parent_ids)
                if (oo is not None): 
                    o = oo
                    continue
            o = AddressService.get_object(o.parent_ids[0])
        tmp = io.StringIO()
        prev = None
        i = 0
        while i < len(path): 
            if (i > 0): 
                print(delim, end="", file=tmp)
            if (path[i].attrs is None): 
                print("?", end="", file=tmp)
            else: 
                str0_ = str(path[i].attrs)
                if (prev is not None and prev == str0_): 
                    Utils.setLengthStringIO(tmp, tmp.tell() - len(delim))
                else: 
                    print(str0_, end="", file=tmp)
                    prev = str0_
            i += 1
        return Utils.toStringStringIO(tmp)
    
    def serialize(self, xml0_ : XmlWriter) -> None:
        xml0_.write_start_element("gar")
        xml0_.write_element_string("id", self.id0_)
        xml0_.write_element_string("level", Utils.enumToString(self.level).lower())
        for p in self.parent_ids: 
            xml0_.write_element_string("parent", p)
        xml0_.write_element_string("guid", Utils.ifNotNull(self.guid, ""))
        if (self.expired): 
            xml0_.write_element_string("expired", "true")
        if (self.region_number > 0): 
            xml0_.write_element_string("reg", str(self.region_number))
        if (self.status != GarStatus.OK): 
            xml0_.write_element_string("status", Utils.enumToString(self.status).lower())
        if (self.children_count > 0): 
            xml0_.write_element_string("chcount", str(self.children_count))
        if (self.internal_byte != (0)): 
            xml0_.write_element_string("ib", str(self.internal_byte))
        xml0_.write_element_string("path", self.get_full_path(None, False, None))
        self.attrs.serialize(xml0_)
        if (self.__m_params is None): 
            self.__load_params()
        if (self.__m_params is not None): 
            for kp in self.__m_params.items(): 
                if (kp[0] != GarParam.GUID): 
                    xml0_.write_start_element("param")
                    xml0_.write_attribute_string("name", Utils.enumToString(kp[0]).lower())
                    xml0_.write_string(Utils.ifNotNull(kp[1], ""))
                    xml0_.write_end_element()
        xml0_.write_end_element()
    
    def deserialize(self, xml0_ : xml.etree.ElementTree.Element) -> None:
        for x in xml0_: 
            if (Utils.getXmlLocalName(x) == "id"): 
                self.id0_ = Utils.getXmlInnerText(x)
            elif (Utils.getXmlLocalName(x) == "path"): 
                self.__m_full_path = Utils.getXmlInnerText(x)
            elif (Utils.getXmlLocalName(x) == "parent"): 
                self.parent_ids.append(Utils.getXmlInnerText(x))
            elif (Utils.getXmlLocalName(x) == "guid"): 
                self.guid = Utils.getXmlInnerText(x)
            elif (Utils.getXmlLocalName(x) == "expired"): 
                self.expired = Utils.getXmlInnerText(x) == "true"
            elif (Utils.getXmlLocalName(x) == "chcount"): 
                self.children_count = int(Utils.getXmlInnerText(x))
            elif (Utils.getXmlLocalName(x) == "reg"): 
                self.region_number = int(Utils.getXmlInnerText(x))
            elif (Utils.getXmlLocalName(x) == "ib"): 
                self.internal_byte = (int(Utils.getXmlInnerText(x)))
            elif (Utils.getXmlLocalName(x) == "status"): 
                try: 
                    self.status = (Utils.valToEnum(Utils.getXmlInnerText(x), GarStatus))
                except Exception as ex11: 
                    pass
            elif (Utils.getXmlLocalName(x) == "level"): 
                try: 
                    self.level = (Utils.valToEnum(Utils.getXmlInnerText(x), GarLevel))
                except Exception as ex12: 
                    pass
            elif (Utils.getXmlLocalName(x) == "area"): 
                self.attrs = (AreaAttributes())
                self.attrs.deserialize(x)
            elif (Utils.getXmlLocalName(x) == "house"): 
                self.attrs = (HouseAttributes())
                self.attrs.deserialize(x)
            elif (Utils.getXmlLocalName(x) == "room"): 
                self.attrs = (RoomAttributes())
                self.attrs.deserialize(x)
            elif (Utils.getXmlLocalName(x) == "param"): 
                if (self.__m_params is None): 
                    self.__m_params = dict()
                try: 
                    ty = Utils.valToEnum(Utils.getXmlAttrByName(x.attrib, "name")[1], GarParam)
                    if (ty != GarParam.UNDEFINED): 
                        self.__m_params[ty] = Utils.getXmlInnerText(x)
                except Exception as ex13: 
                    pass
        if (self.__m_params is not None and self.guid is not None and not GarParam.GUID in self.__m_params): 
            self.__m_params[GarParam.GUID] = self.guid
    
    def compareTo(self, other : 'GarObject') -> int:
        if ((self.level) < (other.level)): 
            return -1
        if ((self.level) > (other.level)): 
            return 1
        aa1 = Utils.asObjectOrNull(self.attrs, AreaAttributes)
        aa2 = Utils.asObjectOrNull(other.attrs, AreaAttributes)
        if (aa1 is not None and aa2 is not None): 
            if (len(aa1.names) > 0 and len(aa2.names) > 0): 
                return Utils.compareStrings(aa1.names[0], aa2.names[0], False)
        return Utils.compareStrings(str(self), str(other), False)
    
    def is_region_city(self) -> bool:
        """ Проверка, что регион является городом (Москва, СПБ, Севестополь)
        
        """
        if (self.level != GarLevel.REGION): 
            return False
        a = Utils.asObjectOrNull(self.attrs, AreaAttributes)
        if ("Москва" in a.names or "Санкт-Петербург" in a.names or "Севастополь" in a.names): 
            return True
        return False