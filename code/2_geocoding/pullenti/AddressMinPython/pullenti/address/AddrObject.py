# SDK Pullenti Address (client version), version 4.28, february 2025.
# Copyright (c) 2013-2025, Pullenti. All rights reserved.
# Non-Commercial Freeware and Commercial Software.
# This class is generated using the converter Unisharping (www.unisharping.ru) from Pullenti C# project.
# The latest version of the code is available on the site www.pullenti.ru

import typing
import io
import xml.etree
from pullenti.unisharp.Utils import Utils
from pullenti.unisharp.Xml import XmlWriter

from pullenti.address.GarLevel import GarLevel
from pullenti.address.HouseType import HouseType
from pullenti.address.AddressHelper import AddressHelper
from pullenti.address.BaseAttributes import BaseAttributes
from pullenti.address.RoomAttributes import RoomAttributes
from pullenti.address.AddrLevel import AddrLevel
from pullenti.address.DetailType import DetailType
from pullenti.address.AreaAttributes import AreaAttributes
from pullenti.address.HouseAttributes import HouseAttributes

class AddrObject:
    """ Адресный объект, выделяемый из текста (элемент TextAddress)
    
    Адресный объект из текста
    """
    
    def __init__(self, attrs_ : 'BaseAttributes') -> None:
        self.attrs = None;
        self.level = AddrLevel.UNDEFINED
        self.gars = list()
        self.cross_object = None;
        self.rep_object_id = 0
        self.is_reconstructed = False
        self.detail_typ = DetailType.UNDEFINED
        self.detail_param = None;
        self.ext_id = None;
        self._doubt_type = False
        self.tag = None;
        self.attrs = attrs_
    
    def __str__(self) -> str:
        if (self.detail_typ == DetailType.UNDEFINED): 
            return self.to_string_min()
        if (self.detail_typ == DetailType.KMRANGE): 
            return "{0} {1}".format(self.to_string_min(), Utils.ifNotNull(self.detail_param, ""))
        res = None
        if (self.detail_param == "часть"): 
            res = "{0} ".format(AddressHelper.get_detail_part_param_string(self.detail_typ))
        else: 
            res = AddressHelper.get_detail_type_string(self.detail_typ)
            if (self.detail_param is not None): 
                res = "{0} {1}".format(res, self.detail_param)
            res += " от "
        return res + self.to_string_min()
    
    def to_string_min(self) -> str:
        if (self.attrs is None): 
            return "?"
        res = None
        if (self.cross_object is not None): 
            res = "{0} / {1}".format(str(self.attrs), self.cross_object.to_string_min())
        elif (isinstance(self.attrs, AreaAttributes)): 
            res = self.attrs.to_string_ex(self.level, False)
        else: 
            res = str(self.attrs)
        return res
    
    def _find_gar_by_ids(self, ids : typing.List[str]) -> 'GarObject':
        if (ids is None): 
            return None
        for g in self.gars: 
            if (g.id0_ in ids): 
                return g
        return None
    
    def find_gar_by_id(self, id0_ : str) -> 'GarObject':
        for g in self.gars: 
            if (g.id0_ == id0_): 
                return g
        return None
    
    def find_gar_by_level(self, level_ : 'GarLevel') -> 'GarObject':
        for g in self.gars: 
            if (g.level == level_): 
                return g
        return None
    
    def _sort_gars(self) -> None:
        k = 0
        while k < len(self.gars): 
            i = 0
            while i < (len(self.gars) - 1): 
                if (self.gars[i].expired and not self.gars[i + 1].expired): 
                    g = self.gars[i]
                    self.gars[i] = self.gars[i + 1]
                    self.gars[i + 1] = g
                i += 1
            k += 1
    
    def out_info(self, res : io.StringIO) -> None:
        """ Вывести подробную текстовую информацию об объекте (для отладки)
        
        Args:
            res(io.StringIO): 
        """
        self.attrs.out_info(res)
        if (self.detail_typ != DetailType.UNDEFINED): 
            if (self.detail_param == "часть"): 
                print("Детализация: {0}\r\n".format(AddressHelper.get_detail_part_param_string(self.detail_typ)), end="", file=res, flush=True)
            else: 
                print("Детализация: {0}".format(AddressHelper.get_detail_type_string(self.detail_typ)), end="", file=res, flush=True)
                if (self.detail_param is not None): 
                    print(" {0}".format(self.detail_param), end="", file=res, flush=True)
                print("\r\n", end="", file=res)
        print("\r\nУровень: {0}".format(AddressHelper.get_addr_level_string(self.level)), end="", file=res, flush=True)
        if (self.rep_object_id > 0): 
            print("\r\nОбъект адрессария: ID={0}".format(self.rep_object_id), end="", file=res, flush=True)
        if (self.ext_id is not None): 
            print("\r\nВнешний идентификатор: {0}".format(self.ext_id), end="", file=res, flush=True)
        print("\r\nПривязка к ГАР: ".format(), end="", file=res, flush=True)
        if (len(self.gars) == 0): 
            print("НЕТ", end="", file=res)
        else: 
            i = 0
            while i < len(self.gars): 
                if (i > 0): 
                    print("; ", end="", file=res)
                print(str(self.gars[i]), end="", file=res)
                i += 1
        if (self.cross_object is not None): 
            print("\r\n\r\nОбъект пересечения:\r\n".format(), end="", file=res, flush=True)
            self.cross_object.out_info(res)
    
    def can_be_parent_for(self, child : 'AddrObject', par_for_parent : 'AddrObject'=None) -> bool:
        if (child is None): 
            return False
        if (not AddressHelper.can_be_parent(child.level, self.level)): 
            if (self.level == AddrLevel.BUILDING and child.level == AddrLevel.BUILDING): 
                if (child.attrs.typ == HouseType.GARAGE and self.attrs.typ != HouseType.GARAGE): 
                    return True
            if (child.level == AddrLevel.CITY and self.level == AddrLevel.CITY): 
                cha = Utils.asObjectOrNull(child.attrs, AreaAttributes)
                a = Utils.asObjectOrNull(self.attrs, AreaAttributes)
                if (len(a.names) > 0 and a.names[0] in cha.names): 
                    if (a.number is None and cha.number is not None): 
                        return True
            if (self.level == AddrLevel.STREET and child.level == AddrLevel.STREET): 
                if ("километр" in child.attrs.types): 
                    return True
                if ("километр" in self.attrs.types): 
                    return True
            if (self.level == AddrLevel.COUNTRY): 
                if (AddressHelper.compare_levels(child.level, AddrLevel.LOCALITY) <= 0): 
                    return True
            return False
        if (child.level == AddrLevel.STREET or child.level == AddrLevel.TERRITORY): 
            if (self.level == AddrLevel.DISTRICT): 
                if (par_for_parent is None): 
                    return False
                if (par_for_parent.level != AddrLevel.CITY and par_for_parent.level != AddrLevel.REGIONCITY and par_for_parent.level != AddrLevel.REGIONAREA): 
                    return False
        if (child.level == AddrLevel.BUILDING and len(self.gars) == 1): 
            pass
        return True
    
    def can_be_equals_level(self, obj : 'AddrObject') -> bool:
        if (self.level == obj.level): 
            return True
        if (self.level == AddrLevel.STREET and obj.level == AddrLevel.TERRITORY): 
            return "дорога" in obj.attrs.miscs
        if (self.level == AddrLevel.TERRITORY and obj.level == AddrLevel.STREET): 
            return "дорога" in self.attrs.miscs
        return False
    
    def can_be_equals_glevel(self, gar : 'GarObject') -> bool:
        a = Utils.asObjectOrNull(self.attrs, AreaAttributes)
        ga = Utils.asObjectOrNull(gar.attrs, AreaAttributes)
        if (((self.level == AddrLevel.LOCALITY and gar.level == GarLevel.AREA)) or ((self.level == AddrLevel.TERRITORY and ((gar.level == GarLevel.LOCALITY or gar.level == GarLevel.STREET))))): 
            for mi in a.miscs: 
                if (mi in ga.miscs): 
                    return True
                if (mi == "дорога" and ((gar.level == GarLevel.STREET or gar.level == GarLevel.AREA))): 
                    return True
            for ty in a.types: 
                if (ty in ga.types): 
                    return True
            if (self.level == AddrLevel.LOCALITY and gar.level == GarLevel.AREA): 
                if ("микрорайон" in ga.types): 
                    return True
                if (len(a.types) > 0): 
                    if (a.types[0] in str(ga).lower()): 
                        return True
            if (self.level == AddrLevel.TERRITORY and gar.level == GarLevel.LOCALITY): 
                if ("совхоз" in a.miscs or "колхоз" in a.miscs): 
                    return True
            if (self.level == AddrLevel.TERRITORY and gar.level == GarLevel.STREET): 
                if ("микрорайон" in a.types): 
                    if ("МИКРОРАЙОН" in str(ga).upper()): 
                        return True
            return False
        if (self.level == AddrLevel.CITY and ((gar.level == GarLevel.MUNICIPALAREA or gar.level == GarLevel.ADMINAREA))): 
            if ("город" in ga.types): 
                return True
        if (self.level == AddrLevel.DISTRICT and gar.level == GarLevel.SETTLEMENT): 
            for ty in gar.attrs.types: 
                if ("район" in ty): 
                    return True
            for nam in gar.attrs.names: 
                if ("район" in nam): 
                    return True
        if (AddressHelper.can_be_equal_levels(self.level, gar.level)): 
            return True
        if (self.level == AddrLevel.LOCALITY and ((gar.level == GarLevel.SETTLEMENT or gar.level == GarLevel.CITY))): 
            return True
        if (self.level == AddrLevel.SETTLEMENT and gar.level == GarLevel.LOCALITY): 
            return True
        if (self.level == AddrLevel.DISTRICT and gar.level == GarLevel.LOCALITY): 
            if ("улус" in a.types): 
                return True
        if (self.level == AddrLevel.DISTRICT and gar.level == GarLevel.AREA): 
            return True
        if (self.level == AddrLevel.CITYDISTRICT): 
            if (len(ga.types) > 0 and "район" in ga.types[0]): 
                return True
        if (self.level == AddrLevel.STREET and gar.level == GarLevel.AREA): 
            if (len(a.types) == 1 and a.types[0] == "улица"): 
                return True
        if (((self.level == AddrLevel.SETTLEMENT or self.level == AddrLevel.LOCALITY)) and gar.level == GarLevel.CITY): 
            if ("поселок" in str(gar)): 
                return True
            if ("поселок" in str(self)): 
                return True
            for ty in a.types: 
                if (ty in ga.types): 
                    return True
        if (self.level == AddrLevel.CITY and gar.level == GarLevel.LOCALITY): 
            if (not "город" in a.types): 
                return True
        return False
    
    def serialize(self, xml0_ : XmlWriter) -> None:
        xml0_.write_start_element("textobj")
        self.attrs.serialize(xml0_)
        xml0_.write_element_string("level", Utils.enumToString(self.level).lower())
        if (self.is_reconstructed): 
            xml0_.write_element_string("reconstr", "true")
        if (self.ext_id is not None): 
            xml0_.write_element_string("extid", self.ext_id)
        for g in self.gars: 
            g.serialize(xml0_)
        if (self.cross_object is not None): 
            xml0_.write_start_element("cross")
            self.cross_object.serialize(xml0_)
            xml0_.write_end_element()
        if (self.detail_typ != DetailType.UNDEFINED): 
            xml0_.write_element_string("detailtyp", Utils.enumToString(self.detail_typ).lower())
            if (self.detail_param is not None): 
                xml0_.write_element_string("detailparame", self.detail_param)
        xml0_.write_end_element()
    
    def deserialize(self, xml0_ : xml.etree.ElementTree.Element) -> None:
        from pullenti.address.GarObject import GarObject
        for x in xml0_: 
            if (Utils.getXmlLocalName(x) == "gar"): 
                g = GarObject(None)
                g.deserialize(x)
                self.gars.append(g)
            elif (Utils.getXmlLocalName(x) == "area"): 
                self.attrs = (AreaAttributes())
                self.attrs.deserialize(x)
            elif (Utils.getXmlLocalName(x) == "house"): 
                self.attrs = (HouseAttributes())
                self.attrs.deserialize(x)
            elif (Utils.getXmlLocalName(x) == "room"): 
                self.attrs = (RoomAttributes())
                self.attrs.deserialize(x)
            elif (Utils.getXmlLocalName(x) == "cross"): 
                for xx in x: 
                    self.cross_object = AddrObject(None)
                    self.cross_object.deserialize(xx)
                    break
            elif (Utils.getXmlLocalName(x) == "level"): 
                try: 
                    self.level = (Utils.valToEnum(Utils.getXmlInnerText(x), AddrLevel))
                except Exception as ex5: 
                    pass
            elif (Utils.getXmlLocalName(x) == "reconstr"): 
                self.is_reconstructed = Utils.getXmlInnerText(x) == "true"
            elif (Utils.getXmlLocalName(x) == "extid"): 
                self.ext_id = Utils.getXmlInnerText(x)
            elif (Utils.getXmlLocalName(x) == "detailtyp"): 
                try: 
                    self.detail_typ = (Utils.valToEnum(Utils.getXmlInnerText(x), DetailType))
                except Exception as ex6: 
                    pass
            elif (Utils.getXmlLocalName(x) == "detailparam"): 
                self.detail_param = Utils.getXmlInnerText(x)
    
    def clone(self) -> 'AddrObject':
        res = AddrObject(self.attrs.clone())
        res.gars.extend(self.gars)
        res.level = self.level
        res.tag = self.tag
        res.rep_object_id = self.rep_object_id
        res._doubt_type = self._doubt_type
        if (self.cross_object is not None): 
            res.cross_object = self.cross_object.clone()
        res.detail_typ = self.detail_typ
        res.detail_param = self.detail_param
        return res