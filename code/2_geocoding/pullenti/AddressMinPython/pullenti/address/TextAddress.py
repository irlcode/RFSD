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

from pullenti.address.MessageCode import MessageCode
from pullenti.address.Message import Message
from pullenti.address.ParamType import ParamType
from pullenti.address.AddrLevel import AddrLevel
from pullenti.address.AddressHelper import AddressHelper
from pullenti.address.AreaAttributes import AreaAttributes
from pullenti.address.AddrObject import AddrObject

class TextAddress:
    """ Полный адрес, выделяемый из текста
    
    """
    
    def __init__(self) -> None:
        self.items = list()
        self.additional_items = None
        self.second_address = None
        self.params = dict()
        self.alpha2 = None;
        self.begin_char = 0
        self.end_char = 0
        self.coef = 0
        self.messages = list()
        self.milliseconds = 0
        self.read_count = 0
        self.text = None;
    
    @property
    def last_item(self) -> 'AddrObject':
        """ Последний (самый низкоуровневый) элемент адреса """
        if (len(self.items) == 0): 
            return None
        return self.items[len(self.items) - 1]
    
    @property
    def last_item_with_gar(self) -> 'AddrObject':
        """ Самый низкоуровневый объект, который удалось привязать к ГАР """
        for i in range(len(self.items) - 1, -1, -1):
            if (len(self.items[i].gars) > 0): 
                return self.items[i]
        return None
    
    def find_item_by_level(self, lev : 'AddrLevel') -> 'AddrObject':
        """ Найти элемент конкретного уровня
        
        Args:
            lev(AddrLevel): 
        
        """
        res = None
        for it in self.items: 
            if (it.level == lev or ((lev == AddrLevel.REGIONAREA and it.level == AddrLevel.REGIONCITY)) or ((lev == AddrLevel.REGIONCITY and it.level == AddrLevel.REGIONAREA))): 
                if (res is None or len(it.gars) > 0): 
                    res = it
        return res
    
    def find_item_by_gar_level(self, lev : 'GarLevel') -> 'AddrObject':
        res = None
        for it in self.items: 
            if (AddressHelper.can_be_equal_levels(it.level, lev)): 
                if (res is None or len(it.gars) > 0): 
                    res = it
        return res
    
    def find_gar_by_ids(self, ids : typing.List[str]) -> 'GarObject':
        if (ids is None): 
            return None
        for it in self.items: 
            if (it is None): 
                continue
            g = it._find_gar_by_ids(ids)
            if (g is not None): 
                return g
        return None
    
    def sort_items(self) -> None:
        k = 0
        while k < len(self.items): 
            ch = False
            i = 0
            while i < (len(self.items) - 1): 
                if (AddressHelper.compare_levels(self.items[i].level, self.items[i + 1].level) > 0): 
                    it = self.items[i]
                    self.items[i] = self.items[i + 1]
                    self.items[i + 1] = it
                    ch = True
                i += 1
            if (not ch): 
                break
            k += 1
    
    @property
    def error_message(self) -> str:
        """ Сообщение, описывающее ошибки (если null, то замечаний и ошибок нет).
        Теперь формируется из списка Messages, у которых Error = true. """
        res = None
        for m in self.messages: 
            if (m.error): 
                if (res is None): 
                    res = m.text
                else: 
                    res = "{0} {1}".format(res, m.text)
        return res
    
    @property
    def info_message(self) -> str:
        """ Несущественные информационные сообщения
        Теперь формируется из списка Messages, у которых Error = false. """
        res = None
        for m in self.messages: 
            if (not m.error): 
                if (res is None): 
                    res = m.text
                else: 
                    res = "{0} {1}".format(res, m.text)
        return res
    
    def __str__(self) -> str:
        res = io.StringIO()
        print("Coef={0}".format(self.coef), end="", file=res, flush=True)
        i = 0
        while i < len(self.items): 
            print((", " if i > 0 else ": "), end="", file=res)
            print(str(self.items[i]), end="", file=res)
            i += 1
        for kp in self.params.items(): 
            if (kp[0] != ParamType.ZIP): 
                print(", ", end="", file=res)
                if (kp[0] != ParamType.ORGANIZATION): 
                    print("{0} ".format(AddressHelper.get_param_type_string(kp[0])), end="", file=res, flush=True)
                print(Utils.ifNotNull(kp[1], ""), end="", file=res)
        if (self.second_address is not None): 
            print("; ", end="", file=res)
            print(str(self.second_address), end="", file=res)
            if (self.second_address.second_address is not None): 
                print("; ", end="", file=res)
                print(str(self.second_address.second_address), end="", file=res)
        return Utils.toStringStringIO(res)
    
    def get_full_path(self, delim : str=", ", ignore_params : bool=False, last_level : 'AddrLevel'=AddrLevel.UNDEFINED) -> str:
        """ Вывести полный путь
        
        Args:
            delim(str): разделитель, запятая по умолчанию
            ignore_params(bool): не выводить дополнительные параметры (этажи, генпланы и пр.)
            last_level(AddrLevel): если задан, то элементы выше этого уровня будут игнорироваться
        
        """
        tmp = io.StringIO()
        i = 0
        first_pass48 = True
        while True:
            if first_pass48: first_pass48 = False
            else: i += 1
            if (not (i < len(self.items))): break
            it = self.items[i]
            if (last_level != AddrLevel.UNDEFINED): 
                if (AddressHelper.compare_levels(it.level, last_level) > 0): 
                    continue
            if (((ignore_params and it.level == AddrLevel.TERRITORY and len(it.gars) == 0) and ((i + 1) < len(self.items)) and self.items[i + 1].level == AddrLevel.STREET) and len(self.items[i + 1].gars) > 0): 
                continue
            if ((isinstance(it.attrs, AreaAttributes)) and ((i + 1) < len(self.items)) and (isinstance(self.items[i + 1].attrs, AreaAttributes))): 
                a = Utils.asObjectOrNull(it.attrs, AreaAttributes)
                a1 = Utils.asObjectOrNull(self.items[i + 1].attrs, AreaAttributes)
                if (("городской округ" in a.types and "город" in a1.types and len(a.names) > 0) and a.names[0] in a1.names): 
                    continue
            if (i > 0): 
                print(delim, end="", file=tmp)
            print(str(self.items[i]), end="", file=tmp)
        if (not ignore_params): 
            for kp in self.params.items(): 
                if (kp[0] != ParamType.ZIP): 
                    print(delim, end="", file=tmp)
                    if (kp[0] != ParamType.ORGANIZATION): 
                        print("{0} ".format(AddressHelper.get_param_type_string(kp[0])), end="", file=tmp, flush=True)
                    print(Utils.ifNotNull(kp[1], ""), end="", file=tmp)
        return Utils.toStringStringIO(tmp)
    
    def out_info(self, res : io.StringIO) -> None:
        """ Вывести подробную текстовую информацию об объекте (для отладки)
        
        Args:
            res(io.StringIO): 
        """
        print("Позиция в тексте: [{0}..{1}]\r\n".format(self.begin_char, self.end_char), end="", file=res, flush=True)
        print("Коэффициент качества: {0}\r\n".format(self.coef), end="", file=res, flush=True)
        for m in self.messages: 
            print("{0}: {1}\r\n".format(("Ошибка" if m.error else "Информация"), m.text), end="", file=res, flush=True)
        for i in range(len(self.items) - 1, -1, -1):
            print("\r\n", end="", file=res)
            self.items[i].out_info(res)
            if (self.additional_items is not None and i == (len(self.items) - 1)): 
                for it in self.additional_items: 
                    print("\r\n", end="", file=res)
                    it.out_info(res)
        for kp in self.params.items(): 
            print("\r\n{0}: {1}".format(AddressHelper.get_param_type_string(kp[0]), Utils.ifNotNull(kp[1], "")), end="", file=res, flush=True)
        if (self.second_address is not None): 
            print("\r\nВторой адрес:\r\n", end="", file=res)
            self.second_address.out_info(res)
            if (self.second_address.second_address is not None): 
                print("\r\nТретий адрес:\r\n", end="", file=res)
                self.second_address.second_address.out_info(res)
    
    def serialize(self, xml0_ : XmlWriter, tag : str=None) -> None:
        xml0_.write_start_element("textaddr")
        xml0_.write_element_string("coef", str(self.coef))
        if (self.error_message is not None): 
            xml0_.write_element_string("message", self.error_message)
        if (self.info_message is not None): 
            xml0_.write_element_string("info", self.info_message)
        for m in self.messages: 
            xml0_.write_start_element("msg")
            xml0_.write_element_string("code", Utils.enumToString(m.code).upper())
            xml0_.write_element_string("text", AddressHelper.correct_xml_value(Utils.ifNotNull(m.text, "")))
            if (m.error): 
                xml0_.write_element_string("error", "true")
            xml0_.write_end_element()
        xml0_.write_element_string("text", AddressHelper.correct_xml_value(Utils.ifNotNull(self.text, "")))
        xml0_.write_element_string("ms", str(self.milliseconds))
        xml0_.write_element_string("rd", str(self.read_count))
        xml0_.write_element_string("begin", str(self.begin_char))
        xml0_.write_element_string("end", str(self.end_char))
        if (self.alpha2 is not None): 
            xml0_.write_element_string("alpha2", self.alpha2)
        for o in self.items: 
            o.serialize(xml0_)
        if (self.additional_items is not None): 
            xml0_.write_start_element("additional")
            for it in self.additional_items: 
                it.serialize(xml0_)
            xml0_.write_end_element()
        for kp in self.params.items(): 
            xml0_.write_start_element("param")
            xml0_.write_attribute_string("typ", Utils.enumToString(kp[0]).lower())
            if (kp[1] is not None): 
                xml0_.write_attribute_string("val", kp[1])
            xml0_.write_end_element()
        if (self.second_address is not None): 
            self.second_address.serialize(xml0_, None)
        xml0_.write_end_element()
    
    def deserialize(self, xml0_ : xml.etree.ElementTree.Element) -> None:
        for x in xml0_: 
            if (Utils.getXmlLocalName(x) == "coef"): 
                self.coef = int(Utils.getXmlInnerText(x))
            elif (Utils.getXmlLocalName(x) == "ms"): 
                self.milliseconds = int(Utils.getXmlInnerText(x))
            elif (Utils.getXmlLocalName(x) == "rd"): 
                self.read_count = int(Utils.getXmlInnerText(x))
            elif (Utils.getXmlLocalName(x) == "text"): 
                self.text = Utils.getXmlInnerText(x)
            elif (Utils.getXmlLocalName(x) == "begin"): 
                self.begin_char = int(Utils.getXmlInnerText(x))
            elif (Utils.getXmlLocalName(x) == "end"): 
                self.end_char = int(Utils.getXmlInnerText(x))
            elif (Utils.getXmlLocalName(x) == "alpha2"): 
                self.alpha2 = Utils.getXmlInnerText(x)
            elif (Utils.getXmlLocalName(x) == "textobj"): 
                to = AddrObject(None)
                to.deserialize(x)
                self.items.append(to)
            elif (Utils.getXmlLocalName(x) == "additional"): 
                self.additional_items = list()
                for xx in x: 
                    it = AddrObject(None)
                    it.deserialize(xx)
                    self.additional_items.append(it)
            elif (Utils.getXmlLocalName(x) == "param"): 
                ty = ParamType.UNDEFINED
                val = None
                for a in x.attrib.items(): 
                    if (Utils.getXmlAttrLocalName(a) == "typ"): 
                        try: 
                            ty = (Utils.valToEnum(a[1], ParamType))
                        except Exception as ex41: 
                            pass
                    elif (Utils.getXmlAttrLocalName(a) == "val"): 
                        val = a[1]
                if (ty != ParamType.UNDEFINED): 
                    self.params[ty] = val
            elif (Utils.getXmlLocalName(x) == "textaddr"): 
                self.second_address = TextAddress()
                self.second_address.deserialize(x)
            elif (Utils.getXmlLocalName(x) == "msg"): 
                m = Message(MessageCode.NOADDRESS, None, False)
                self.messages.append(m)
                for xx in x: 
                    if (Utils.getXmlLocalName(xx) == "code"): 
                        try: 
                            m.code = (Utils.valToEnum(Utils.getXmlInnerText(xx), MessageCode))
                        except Exception as ex42: 
                            pass
                    elif (Utils.getXmlLocalName(xx) == "text"): 
                        m.text = Utils.getXmlInnerText(xx)
                    elif (Utils.getXmlLocalName(xx) == "error"): 
                        m.error = Utils.getXmlInnerText(xx) == "true"
    
    def clone(self) -> 'TextAddress':
        res = TextAddress()
        for it in self.items: 
            res.items.append(it.clone())
        if (self.additional_items is not None): 
            res.additional_items = list()
            for it in self.additional_items: 
                res.additional_items.append(it.clone())
        res.begin_char = self.begin_char
        res.end_char = self.end_char
        res.coef = self.coef
        res.milliseconds = self.milliseconds
        res.text = self.text
        res.alpha2 = self.alpha2
        for kp in self.params.items(): 
            res.params[kp[0]] = kp[1]
        if (self.second_address is not None): 
            res.second_address = self.second_address.clone()
        for m in self.messages: 
            res.messages.append(Message(m.code, m.text, m.error))
        return res
    
    @staticmethod
    def _new2(_arg1 : str) -> 'TextAddress':
        res = TextAddress()
        res.text = _arg1
        return res