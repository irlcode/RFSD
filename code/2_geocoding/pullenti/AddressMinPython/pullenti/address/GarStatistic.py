# SDK Pullenti Address (client version), version 4.28, february 2025.
# Copyright (c) 2013-2025, Pullenti. All rights reserved.
# Non-Commercial Freeware and Commercial Software.
# This class is generated using the converter Unisharping (www.unisharping.ru) from Pullenti C# project.
# The latest version of the code is available on the site www.pullenti.ru

import xml.etree
from pullenti.unisharp.Utils import Utils
from pullenti.unisharp.Xml import XmlWriter

class GarStatistic:
    """ Статистика по объектам ГАР """
    
    def __init__(self) -> None:
        self.index_path = None;
        self.version = None;
        self.area_count = 0
        self.house_count = 0
        self.room_count = 0
    
    def __str__(self) -> str:
        return "IndexPath: {0}, Version: {1}, AddrObjs: {2}, Houses: {3}, Rooms: {4}".format(self.index_path, self.version, self.area_count, self.house_count, self.room_count)
    
    def serialize(self, xml0_ : XmlWriter) -> None:
        xml0_.write_start_element("GarStatistic")
        if (self.index_path is not None): 
            xml0_.write_element_string("path", self.index_path)
        if (self.version is not None): 
            xml0_.write_element_string("version", self.version)
        xml0_.write_element_string("areas", str(self.area_count))
        xml0_.write_element_string("houses", str(self.house_count))
        xml0_.write_element_string("rooms", str(self.room_count))
        xml0_.write_end_element()
    
    def deserialize(self, xml0_ : xml.etree.ElementTree.Element) -> None:
        for x in xml0_: 
            if (Utils.getXmlLocalName(x) == "path"): 
                self.index_path = Utils.getXmlInnerText(x)
            elif (Utils.getXmlLocalName(x) == "version"): 
                self.version = Utils.getXmlInnerText(x)
            elif (Utils.getXmlLocalName(x) == "areas"): 
                self.area_count = int(Utils.getXmlInnerText(x))
            elif (Utils.getXmlLocalName(x) == "houses"): 
                self.house_count = int(Utils.getXmlInnerText(x))
            elif (Utils.getXmlLocalName(x) == "rooms"): 
                self.room_count = int(Utils.getXmlInnerText(x))