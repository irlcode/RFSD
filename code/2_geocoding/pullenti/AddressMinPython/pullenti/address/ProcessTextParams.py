# SDK Pullenti Address (client version), version 4.28, february 2025.
# Copyright (c) 2013-2025, Pullenti. All rights reserved.
# Non-Commercial Freeware and Commercial Software.
# This class is generated using the converter Unisharping (www.unisharping.ru) from Pullenti C# project.
# The latest version of the code is available on the site www.pullenti.ru

import io
import xml.etree
from pullenti.unisharp.Utils import Utils
from pullenti.unisharp.Misc import RefOutArgWrapper
from pullenti.unisharp.Xml import XmlWriter

from pullenti.address.AddressService import AddressService
from pullenti.address.AddrLevel import AddrLevel

class ProcessTextParams:
    """ Параметры обработки текста """
    
    def __init__(self) -> None:
        self.default_regions = list()
        self.default_object = None
        self.is_plot = False
        self.strict_house_number = False
        self.no_flats = False
        self.search_regime = False
        self.dont_correct_text = False
        self.prev_address = None
        self.classic_address_model_records = False
        self.text_model = 0
    
    def __str__(self) -> str:
        tmp = io.StringIO()
        if (len(self.default_regions) == 1): 
            print("Регион: {0}".format(self.default_regions[0]), end="", file=tmp, flush=True)
        elif (len(self.default_regions) > 0): 
            print("Регионы: {0}".format(self.default_regions[0]), end="", file=tmp, flush=True)
            i = 1
            while i < len(self.default_regions): 
                print(",{0}".format(self.default_regions[i]), end="", file=tmp, flush=True)
                i += 1
        if (self.default_object is not None): 
            print(" Объект: {0}".format(str(self.default_object)), end="", file=tmp, flush=True)
        if (self.is_plot): 
            print(" Участок: да", end="", file=tmp)
        if (self.search_regime): 
            print(" Режим поисковый: да".format(), end="", file=tmp, flush=True)
        if (self.strict_house_number): 
            print(" Точные номера домов: да", end="", file=tmp)
        if (self.no_flats): 
            print(" Квартиры: отсутствуют", end="", file=tmp)
        if (self.dont_correct_text): 
            print(" Корректировать текст: нет".format(), end="", file=tmp, flush=True)
        if (self.prev_address is not None): 
            print(" Пред.адрес: '{0}'".format(self.prev_address.get_full_path(", ", False, AddrLevel.UNDEFINED)), end="", file=tmp, flush=True)
        if (self.text_model != 0): 
            print(" Модель текста: {0}".format(self.text_model), end="", file=tmp, flush=True)
        if (tmp.tell() == 0): 
            print("Нет", end="", file=tmp)
        return Utils.toStringStringIO(tmp)
    
    def serialize(self, xml0_ : XmlWriter) -> None:
        if (len(self.default_regions) > 0): 
            val = str(self.default_regions[0])
            i = 1
            while i < len(self.default_regions): 
                val = "{0} {1}".format(val, self.default_regions)
                i += 1
            xml0_.write_attribute_string("regs", val)
        if (self.default_object is not None): 
            xml0_.write_attribute_string("defobj", self.default_object.id0_)
        if (self.is_plot): 
            xml0_.write_attribute_string("plot", "true")
        if (self.strict_house_number): 
            xml0_.write_attribute_string("stricthousenumber", "true")
        if (self.no_flats): 
            xml0_.write_attribute_string("noflats", "true")
        if (self.dont_correct_text): 
            xml0_.write_attribute_string("dontcorrect", "true")
        if (self.classic_address_model_records): 
            xml0_.write_attribute_string("classicmodelrecord", "true")
        if (self.text_model > 0): 
            xml0_.write_attribute_string("textmodel", str(self.text_model))
    
    def deserialize(self, xml0_ : xml.etree.ElementTree.Element) -> None:
        if (xml0_.attrib is not None): 
            for x in xml0_.attrib.items(): 
                if (Utils.getXmlAttrLocalName(x) == "regs"): 
                    for v in Utils.splitString(x[1], ' ', False): 
                        n = 0
                        wrapn31 = RefOutArgWrapper(0)
                        inoutres32 = Utils.tryParseInt(v, wrapn31)
                        n = wrapn31.value
                        if (inoutres32): 
                            self.default_regions.append(n)
                elif (Utils.getXmlAttrLocalName(x) == "defobj"): 
                    self.default_object = AddressService.get_object(x[1])
                elif (Utils.getXmlAttrLocalName(x) == "plot" and x[1] == "true"): 
                    self.is_plot = True
                elif (Utils.getXmlAttrLocalName(x) == "stricthousenumber" and x[1] == "true"): 
                    self.strict_house_number = True
                elif (Utils.getXmlAttrLocalName(x) == "dontcorrect" and x[1] == "true"): 
                    self.dont_correct_text = True
                elif (Utils.getXmlAttrLocalName(x) == "noflats" and x[1] == "true"): 
                    self.no_flats = True
                elif (Utils.getXmlAttrLocalName(x) == "classicmodelrecord" and x[1] == "true"): 
                    self.classic_address_model_records = True
                elif (Utils.getXmlAttrLocalName(x) == "textmodel"): 
                    self.text_model = int(x[1])