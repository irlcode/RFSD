# SDK Pullenti Address (client version), version 4.28, february 2025.
# Copyright (c) 2013-2025, Pullenti. All rights reserved.
# Non-Commercial Freeware and Commercial Software.
# This class is generated using the converter Unisharping (www.unisharping.ru) from Pullenti C# project.
# The latest version of the code is available on the site www.pullenti.ru

import io
import xml.etree
from pullenti.unisharp.Utils import Utils
from pullenti.unisharp.Xml import XmlWriter

from pullenti.address.BaseAttributes import BaseAttributes
from pullenti.address.HouseType import HouseType
from pullenti.address.StroenType import StroenType
from pullenti.address.AddressHelper import AddressHelper

class HouseAttributes(BaseAttributes):
    """ Атрибуты зданий и участков """
    
    def __init__(self) -> None:
        super().__init__()
        self.typ = HouseType.UNDEFINED
        self.number = None;
        self.build_number = None;
        self.stroen_typ = StroenType.UNDEFINED
        self.stroen_number = None;
        self.plot_number = None;
    
    def __str__(self) -> str:
        res = io.StringIO()
        if (self.plot_number is not None): 
            print("уч.{0}".format(self.plot_number), end="", file=res, flush=True)
        if (self.number is not None or self.typ != HouseType.UNDEFINED): 
            if (res.tell() > 0): 
                print(" ", end="", file=res)
            if (self.typ != HouseType.UNDEFINED): 
                ty = AddressHelper.get_house_type_string(self.typ, self.number is not None)
                if (self.number is not None and self.number.startswith(ty)): 
                    print(self.number, end="", file=res)
                else: 
                    print("{0}{1}".format(ty, Utils.ifNotNull(self.number, " б/н")), end="", file=res, flush=True)
            else: 
                print(self.number, end="", file=res)
        if (self.build_number is not None): 
            if (res.tell() > 0): 
                print(" ", end="", file=res)
            print("корп.{0}".format(self.build_number), end="", file=res, flush=True)
        if (self.stroen_number is not None): 
            if (res.tell() > 0): 
                print(" ", end="", file=res)
            print("{0}{1}".format(AddressHelper.get_stroen_type_string(self.stroen_typ, True), self.stroen_number), end="", file=res, flush=True)
        return Utils.toStringStringIO(res)
    
    def out_info(self, res : io.StringIO) -> None:
        if (self.plot_number is not None): 
            print("Участок: {0}\r\n".format(self.plot_number), end="", file=res, flush=True)
        if (self.number is not None or self.typ != HouseType.UNDEFINED): 
            if (self.typ != HouseType.UNDEFINED): 
                typ_ = AddressHelper.get_house_type_string(self.typ, False)
                print("{0}{1}: {2}\r\n".format(str.upper(typ_[0]), typ_[1:], Utils.ifNotNull(self.number, "б/н")), end="", file=res, flush=True)
            else: 
                print("Номер: {0}\r\n".format(self.number), end="", file=res, flush=True)
        if (self.build_number is not None): 
            print("Корпус: {0}\r\n".format(self.build_number), end="", file=res, flush=True)
        if (self.stroen_number is not None): 
            typ_ = AddressHelper.get_stroen_type_string(self.stroen_typ, False)
            print("{0}{1}: {2}\r\n".format(str.upper(typ_[0]), typ_[1:], self.stroen_number), end="", file=res, flush=True)
    
    def serialize(self, xml0_ : XmlWriter) -> None:
        xml0_.write_start_element("house")
        if (self.typ != HouseType.UNDEFINED): 
            xml0_.write_element_string("type", Utils.enumToString(self.typ).lower())
        if (self.number is not None): 
            xml0_.write_element_string("num", self.number)
        if (self.build_number is not None): 
            xml0_.write_element_string("bnum", self.build_number)
        if (self.plot_number is not None): 
            xml0_.write_element_string("pnum", self.plot_number)
        if (self.stroen_number is not None): 
            xml0_.write_element_string("stype", Utils.enumToString(self.stroen_typ).lower())
            xml0_.write_element_string("snum", self.stroen_number)
        xml0_.write_end_element()
    
    def deserialize(self, xml0_ : xml.etree.ElementTree.Element) -> None:
        for x in xml0_: 
            if (Utils.getXmlLocalName(x) == "type"): 
                try: 
                    self.typ = (Utils.valToEnum(Utils.getXmlInnerText(x), HouseType))
                except Exception as ex29: 
                    pass
            elif (Utils.getXmlLocalName(x) == "num"): 
                self.number = Utils.getXmlInnerText(x)
            elif (Utils.getXmlLocalName(x) == "stype"): 
                try: 
                    self.stroen_typ = (Utils.valToEnum(Utils.getXmlInnerText(x), StroenType))
                except Exception as ex30: 
                    pass
            elif (Utils.getXmlLocalName(x) == "snum"): 
                self.stroen_number = Utils.getXmlInnerText(x)
            elif (Utils.getXmlLocalName(x) == "bnum"): 
                self.build_number = Utils.getXmlInnerText(x)
            elif (Utils.getXmlLocalName(x) == "pnum"): 
                self.plot_number = Utils.getXmlInnerText(x)
    
    def clone(self) -> 'BaseAttributes':
        res = HouseAttributes()
        res.typ = self.typ
        res.number = self.number
        res.build_number = self.build_number
        res.plot_number = self.plot_number
        res.stroen_typ = self.stroen_typ
        res.stroen_number = self.stroen_number
        return res
    
    def calc_house_pure_number(self) -> int:
        if (self.number is None): 
            return 0
        n = 0
        i = 0
        while i < len(self.number): 
            if (not str.isdigit(self.number[i])): 
                break
            else: 
                n = ((n * 10) + (((ord(self.number[i])) - (ord('0')))))
            i += 1
        return n