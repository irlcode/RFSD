# SDK Pullenti Address (client version), version 4.28, february 2025.
# Copyright (c) 2013-2025, Pullenti. All rights reserved.
# Non-Commercial Freeware and Commercial Software.
# This class is generated using the converter Unisharping (www.unisharping.ru) from Pullenti C# project.
# The latest version of the code is available on the site www.pullenti.ru

import io
import xml.etree
import typing
from pullenti.unisharp.Utils import Utils
from pullenti.unisharp.Misc import RefOutArgWrapper
from pullenti.unisharp.Xml import XmlWriter

from pullenti.address.BaseAttributes import BaseAttributes
from pullenti.address.AddrLevel import AddrLevel
from pullenti.address.AddressHelper import AddressHelper

class AreaAttributes(BaseAttributes):
    """ Атрибуты города, региона, района, квартала, улиц и т.п. """
    
    def __init__(self) -> None:
        super().__init__()
        self.types = list()
        self.names = list()
        self.number = None;
        self.miscs = list()
        self.number_param = None;
    
    def __str__(self) -> str:
        return self.to_string_ex(AddrLevel.UNDEFINED, False)
    
    def to_string_ex(self, lev : 'AddrLevel', is_gar : bool) -> str:
        res = (self.types[0] if len(self.types) > 0 else "")
        out_num = False
        if ((lev == AddrLevel.STREET and len(self.types) > 1 and self.types[1] != "улица") and res == "улица"): 
            res = self.types[1]
        br = False
        is_terr = res == "территория" or lev == AddrLevel.TERRITORY
        if (is_terr): 
            if (len(self.miscs) > 0 and not is_gar and self.miscs[0] != "дорога"): 
                if (len(self.names) > 0 and self.miscs[0] in self.names[0]): 
                    pass
                elif (len(self.names) > 0 or self.number is not None): 
                    if (res.startswith("территория ")): 
                        words = Utils.splitString(res[11:].strip(), ' ', False)
                        if (len(words) == len(self.miscs[0])): 
                            i = 0
                            i = 0
                            while i < len(words): 
                                if (words[i][0] != self.miscs[0][i]): 
                                    break
                                i += 1
                            if (i >= len(words)): 
                                res = "территория"
                    res = "{0} {1}".format(res, self.miscs[0])
                    i = 1
                    while i < len(self.miscs): 
                        res = "{0} {1}".format(res, self.miscs[i])
                        i += 1
                    if (len(self.names) > 0): 
                        br = True
        if ((self.number is not None and lev == AddrLevel.STREET and not is_terr) and not self.number.endswith("км")): 
            res = "{0} {1}".format(res, self.number)
            out_num = True
        if (len(self.names) > 0): 
            if (res == "километр" and not str.isdigit(self.names[0][0])): 
                res = "{0} километр".format(self.names[0])
            elif (br): 
                if (self.number is not None and not out_num): 
                    res = "{0} \"{1}-{2}\"".format(res, self.names[0], self.number)
                    out_num = True
                else: 
                    res = "{0} \"{1}\"".format(res, self.names[0])
            else: 
                res = "{0} {1}".format(res, self.names[0])
                if (lev == AddrLevel.LOCALITY or lev == AddrLevel.SETTLEMENT): 
                    if (len(self.miscs) > 0 and not self.miscs[0] in self.types): 
                        mi = AddressHelper.convert_first_char_upper_and_other_lower(self.miscs[0])
                        if (not mi in self.names): 
                            res = "{0} {1}".format(res, mi)
        elif (((lev == AddrLevel.STREET or lev == AddrLevel.TERRITORY)) and len(self.types) > 1): 
            if (self.types[1] != "улица"): 
                res = "{0} {1}".format(res, AddressHelper.convert_first_char_upper_and_other_lower(self.types[1]))
            else: 
                res = self.types[1]
                if (self.number is not None and lev == AddrLevel.STREET): 
                    res = "{0} {1}".format(res, self.number)
                    out_num = True
                res = "{0} {1}".format(res, AddressHelper.convert_first_char_upper_and_other_lower(self.types[0]))
        if (self.number is not None and not out_num): 
            if (res == "километр"): 
                res = "{0} километр".format(self.number)
            else: 
                nnn = 0
                if (lev == AddrLevel.TERRITORY): 
                    if (len(res) == 0): 
                        res = self.number
                    else: 
                        res = "{0} №{1}".format(res, self.number)
                else: 
                    wrapnnn7 = RefOutArgWrapper(0)
                    inoutres8 = Utils.tryParseInt(self.number, wrapnnn7)
                    nnn = wrapnnn7.value
                    if (inoutres8): 
                        res = "{0}-{1}".format(res, self.number)
                    else: 
                        res = "{0} {1}".format(res, self.number)
        if (len(self.names) == 0 and self.number is None and len(self.miscs) > 0): 
            res = "{0} {1}".format(res, AddressHelper.convert_first_char_upper_and_other_lower(self.miscs[0]))
        if (self.number_param is not None): 
            res = "{0}, {1}".format(res, self.number_param)
        return res.strip()
    
    def out_info(self, res : io.StringIO) -> None:
        if (len(self.types) > 0): 
            print("Тип: {0}".format(self.types[0]), end="", file=res, flush=True)
            i = 1
            while i < len(self.types): 
                print(" / {0}".format(self.types[i]), end="", file=res, flush=True)
                i += 1
            print("\r\n", end="", file=res)
        if (len(self.names) > 0): 
            print("Наименование: {0}".format(self.names[0]), end="", file=res, flush=True)
            i = 1
            while i < len(self.names): 
                print(" / {0}".format(self.names[i]), end="", file=res, flush=True)
                i += 1
            print("\r\n", end="", file=res)
        if (self.number is not None): 
            print("Номер: {0}\r\n".format(self.number), end="", file=res, flush=True)
        if (len(self.miscs) > 0): 
            print("Дополнительно: {0}".format(self.miscs[0]), end="", file=res, flush=True)
            i = 1
            while i < len(self.miscs): 
                print(" / {0}".format(self.miscs[i]), end="", file=res, flush=True)
                i += 1
            print("\r\n", end="", file=res)
        if (self.number_param is not None): 
            print("Числовой параметр: {0}".format(self.number_param), end="", file=res, flush=True)
    
    def can_be_equals(self, a : 'AreaAttributes') -> bool:
        if (self.number is not None or a.number is not None): 
            if (self.number != a.number): 
                return False
        typ_eq = False
        for ty in self.types: 
            if (ty in a.types): 
                typ_eq = True
        names_eq = False
        for ty in self.names: 
            if (ty in a.names): 
                names_eq = True
        if (names_eq): 
            if (typ_eq): 
                return True
            if (len(self.types) == 0 or len(a.types) == 0): 
                return True
            if (self.number is not None): 
                return True
            return False
        if (len(self.names) == 0 and len(a.names) == 0): 
            if (typ_eq): 
                return True
        return False
    
    def serialize(self, xml0_ : XmlWriter) -> None:
        xml0_.write_start_element("area")
        for ty in self.types: 
            xml0_.write_element_string("type", ty)
        for nam in self.names: 
            xml0_.write_element_string("name", nam)
        for misc in self.miscs: 
            xml0_.write_element_string("misc", misc)
        if (self.number is not None): 
            xml0_.write_element_string("num", self.number)
        if (self.number_param is not None): 
            xml0_.write_element_string("numparam", self.number_param)
        xml0_.write_end_element()
    
    def deserialize(self, xml0_ : xml.etree.ElementTree.Element) -> None:
        for x in xml0_: 
            if (Utils.getXmlLocalName(x) == "type"): 
                self.types.append(Utils.getXmlInnerText(x))
            elif (Utils.getXmlLocalName(x) == "name"): 
                self.names.append(Utils.getXmlInnerText(x))
            elif (Utils.getXmlLocalName(x) == "misc"): 
                self.miscs.append(Utils.getXmlInnerText(x))
            elif (Utils.getXmlLocalName(x) == "num"): 
                self.number = Utils.getXmlInnerText(x)
            elif (Utils.getXmlLocalName(x) == "numparam"): 
                self.number_param = Utils.getXmlInnerText(x)
    
    def has_equal_type(self, typs : typing.List[str]) -> bool:
        if (typs is None): 
            return False
        for ty in self.types: 
            if (ty in typs): 
                return True
            if ("поселок" in ty): 
                for tyy in typs: 
                    if ("поселок" in tyy): 
                        return True
        return False
    
    def find_misc(self, miscs_ : typing.List[str]) -> str:
        if (miscs_ is not None): 
            for m in miscs_: 
                if (m in self.miscs): 
                    return m
        return None
    
    def contains_name(self, sub_name : str) -> bool:
        for n in self.names: 
            if (sub_name in n): 
                return True
            elif (Utils.compareStrings(n, sub_name, True) == 0): 
                return True
            elif (len(sub_name) == (len(n) + 1) and sub_name.startswith(n)): 
                return True
        return False
    
    @property
    def can_not_has_gar(self) -> bool:
        if ((("блок" in self.types or "линия" in self.types or "ряд" in self.types) or "очередь" in self.types or "поле" in self.types) or "куст" in self.types): 
            return True
        return False
    
    def clone(self) -> 'BaseAttributes':
        res = AreaAttributes()
        res.types.extend(self.types)
        res.names.extend(self.names)
        res.miscs.extend(self.miscs)
        res.number = self.number
        res.number_param = self.number_param
        return res