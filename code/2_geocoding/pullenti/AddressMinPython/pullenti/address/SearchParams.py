# SDK Pullenti Address (client version), version 4.28, february 2025.
# Copyright (c) 2013-2025, Pullenti. All rights reserved.
# Non-Commercial Freeware and Commercial Software.
# This class is generated using the converter Unisharping (www.unisharping.ru) from Pullenti C# project.
# The latest version of the code is available on the site www.pullenti.ru

import io
import xml.etree
from pullenti.unisharp.Utils import Utils
from pullenti.unisharp.Xml import XmlWriter

from pullenti.address.GarParam import GarParam

class SearchParams:
    """ Параметры для поиска """
    
    def __init__(self) -> None:
        self.free_text = None;
        self.region = 0
        self.area = None;
        self.city = None;
        self.street = None;
        self.param_typ = GarParam.UNDEFINED
        self.param_value = None;
        self.max_count = 100
    
    def __str__(self) -> str:
        res = io.StringIO()
        if (not Utils.isNullOrEmpty(self.free_text)): 
            print("Text:'{0}' ".format(self.free_text), end="", file=res, flush=True)
        if (self.region > 0): 
            print("Region:{0} ".format(), end="", file=res, flush=True)
        if (not Utils.isNullOrEmpty(self.area)): 
            print("Area:'{0}' ".format(self.area), end="", file=res, flush=True)
        if (not Utils.isNullOrEmpty(self.city)): 
            print("City:'{0}' ".format(self.city), end="", file=res, flush=True)
        if (not Utils.isNullOrEmpty(self.street)): 
            print("Street:'{0}' ".format(self.street), end="", file=res, flush=True)
        if (self.param_typ != GarParam.UNDEFINED): 
            print("{0}:'{1}'".format(Utils.enumToString(self.param_typ), Utils.ifNotNull(self.param_value, "")), end="", file=res, flush=True)
        return Utils.toStringStringIO(res)
    
    def serialize(self, xml0_ : XmlWriter) -> None:
        xml0_.write_start_element("searchparams")
        if (not Utils.isNullOrEmpty(self.free_text)): 
            xml0_.write_element_string("text", self.free_text)
        if (self.region > 0): 
            xml0_.write_element_string("region", str(self.region))
        if (self.area is not None): 
            xml0_.write_element_string("area", self.area)
        if (self.city is not None): 
            xml0_.write_element_string("city", self.city)
        if (self.street is not None): 
            xml0_.write_element_string("street", self.street)
        if (self.param_typ != GarParam.UNDEFINED): 
            xml0_.write_element_string("paramtype", Utils.enumToString(self.param_typ).lower())
        if (self.param_value is not None): 
            xml0_.write_element_string("paramvalue", self.param_value)
        if (self.max_count > 0): 
            xml0_.write_element_string("maxcount", str(self.max_count))
        xml0_.write_end_element()
    
    def deserialize(self, xml0_ : xml.etree.ElementTree.Element) -> None:
        for x in xml0_: 
            if (Utils.getXmlLocalName(x) == "region"): 
                self.region = int(Utils.getXmlInnerText(x))
            elif (Utils.getXmlLocalName(x) == "text"): 
                self.free_text = Utils.getXmlInnerText(x)
            elif (Utils.getXmlLocalName(x) == "area"): 
                self.area = Utils.getXmlInnerText(x)
            elif (Utils.getXmlLocalName(x) == "city"): 
                self.city = Utils.getXmlInnerText(x)
            elif (Utils.getXmlLocalName(x) == "street"): 
                self.street = Utils.getXmlInnerText(x)
            elif (Utils.getXmlLocalName(x) == "paramtype"): 
                try: 
                    self.param_typ = (Utils.valToEnum(Utils.getXmlInnerText(x), GarParam))
                except Exception as ex34: 
                    pass
            elif (Utils.getXmlLocalName(x) == "paramvalue"): 
                self.param_value = Utils.getXmlInnerText(x)
            elif (Utils.getXmlLocalName(x) == "maxcount"): 
                self.max_count = int(Utils.getXmlInnerText(x))