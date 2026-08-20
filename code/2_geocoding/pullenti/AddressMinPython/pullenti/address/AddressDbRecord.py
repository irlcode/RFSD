# SDK Pullenti Address (client version), version 4.28, february 2025.
# Copyright (c) 2013-2025, Pullenti. All rights reserved.
# Non-Commercial Freeware and Commercial Software.
# This class is generated using the converter Unisharping (www.unisharping.ru) from Pullenti C# project.
# The latest version of the code is available on the site www.pullenti.ru

import io
import xml.etree
import typing
from pullenti.unisharp.Utils import Utils
from pullenti.unisharp.Xml import XmlWriter

from pullenti.address.ParamType import ParamType
from pullenti.address.RoomAttributes import RoomAttributes
from pullenti.address.GarParam import GarParam
from pullenti.address.RoomType import RoomType
from pullenti.address.HouseType import HouseType
from pullenti.address.AddressHelper import AddressHelper
from pullenti.address.GpsObject import GpsObject
from pullenti.address.AddressDbLevel import AddressDbLevel
from pullenti.address.SqlAddressHelper import SqlAddressHelper
from pullenti.address.HouseAttributes import HouseAttributes
from pullenti.address.AddrLevel import AddrLevel
from pullenti.address.AreaAttributes import AreaAttributes

class AddressDbRecord:
    """ Элементы адреса для заполнения БД """
    
    def __init__(self) -> None:
        self.source_string = None;
        self.coef = 0
        self.normal_string = None;
        self.message = None;
        self.level = AddressDbLevel.UNDEFINED
        self.country_code = None;
        self.region_mnem = None;
        self.region_guid = None;
        self.city_mnem = None;
        self.city_guid = None;
        self.district_mnem = None;
        self.district_guid = None;
        self.location_mnem = None;
        self.location_guid = None;
        self.territory_mnem = None;
        self.territory_types = None;
        self.territory_guid = None;
        self.street_mnem = None;
        self.street_types = None;
        self.street_guid = None;
        self.house_mnem = None;
        self.house_guid = None;
        self.apartment_mnem = None;
        self.apartment_guid = None;
        self.gps_lat = 0
        self.gps_lon = 0
        self.gps_level = AddressDbLevel.UNDEFINED
        self.miscs = None;
        self.id0_ = 0
    
    def __str__(self) -> str:
        return "{0}: {1}".format(self.coef, self.normal_string)
    
    def out_info(self, res : io.StringIO, out_coef : bool=False) -> None:
        """ Вывод информации (для отладки)
        
        Args:
            res(io.StringIO): 
        """
        if (out_coef): 
            print("Коэффициент: {0}".format(self.coef), end="", file=res, flush=True)
            print("\r\nНормализация: {0}".format(self.normal_string), end="", file=res, flush=True)
            print("\r\nУровень: {0}".format(Utils.enumToString(self.level)), end="", file=res, flush=True)
            if (self.message is not None): 
                print("\r\nСообщение: {0}".format(self.message), end="", file=res, flush=True)
        if (self.country_code is not None): 
            print("\r\nСтрана: {0}".format(self.country_code), end="", file=res, flush=True)
        if (self.region_mnem is not None): 
            print("\r\nРегион: {0}".format(self.region_mnem), end="", file=res, flush=True)
            if (self.region_guid is not None): 
                print(" ({0})".format(self.region_guid), end="", file=res, flush=True)
        if (self.city_mnem is not None): 
            print("\r\nГород: {0}".format(self.city_mnem), end="", file=res, flush=True)
            if (self.city_guid is not None): 
                print(" ({0})".format(self.city_guid), end="", file=res, flush=True)
        if (self.district_mnem is not None): 
            print("\r\nРайон: {0}".format(self.district_mnem), end="", file=res, flush=True)
            if (self.district_guid is not None): 
                print(" ({0})".format(self.district_guid), end="", file=res, flush=True)
        if (self.location_mnem is not None): 
            print("\r\nНас.пункт: {0}".format(self.location_mnem), end="", file=res, flush=True)
            if (self.location_guid is not None): 
                print(" ({0})".format(self.location_guid), end="", file=res, flush=True)
        if (self.territory_mnem is not None): 
            print("\r\nТерритория: {0} {1}".format(self.territory_mnem, Utils.ifNotNull(self.territory_types, "")), end="", file=res, flush=True)
            if (self.territory_guid is not None): 
                print(" ({0})".format(self.territory_guid), end="", file=res, flush=True)
        if (self.street_mnem is not None): 
            print("\r\nУлица: {0} {1}".format(self.street_mnem, Utils.ifNotNull(self.street_types, "")), end="", file=res, flush=True)
            if (self.street_guid is not None): 
                print(" ({0})".format(self.street_guid), end="", file=res, flush=True)
        if (self.house_mnem is not None): 
            print("\r\nЗдание: {0}".format(self.house_mnem), end="", file=res, flush=True)
            if (self.house_guid is not None): 
                print(" ({0})".format(self.house_guid), end="", file=res, flush=True)
        if (self.apartment_mnem is not None): 
            print("\r\nПомещение: {0}".format(self.apartment_mnem), end="", file=res, flush=True)
            if (self.apartment_guid is not None): 
                print(" ({0})".format(self.apartment_guid), end="", file=res, flush=True)
        if (self.gps_lat > 0 and self.gps_lon > 0): 
            print("\r\nGPS: {0} {1}".format(GpsObject.out_double(self.gps_lat), GpsObject.out_double(self.gps_lon)), end="", file=res, flush=True)
            if (self.gps_level != AddressDbLevel.UNDEFINED): 
                print(" (уровень {0})".format(Utils.enumToString(self.gps_level)), end="", file=res, flush=True)
        if (self.miscs is not None): 
            print("\r\nРазное: {0}".format(self.miscs), end="", file=res, flush=True)
    
    def serialize(self, xml0_ : XmlWriter) -> None:
        xml0_.write_start_element("addr_rec")
        if (self.id0_ > 0): 
            xml0_.write_element_string("id", str(self.id0_))
        for fi in SqlAddressHelper.COLUMNS: 
            val = SqlAddressHelper.GLOBAL.get_column_value(self, fi)
            if (not Utils.isNullOrEmpty(val)): 
                xml0_.write_element_string(fi, val)
        xml0_.write_end_element()
    
    def deserialize(self, xml0_ : xml.etree.ElementTree.Element) -> None:
        for x in xml0_: 
            if (Utils.getXmlLocalName(x) == "id"): 
                self.id0_ = int(Utils.getXmlInnerText(x))
            else: 
                SqlAddressHelper.GLOBAL.set_column_value(self, Utils.getXmlLocalName(x), Utils.getXmlInnerText(x))
    
    @staticmethod
    def create_from_address(addr : 'TextAddress', classic_model_record : bool=False) -> 'AddressDbRecord':
        """ Создать запись для БД из проанализированного объекта
        
        Args:
            addr(TextAddress): проанализированный адрес
            classic_model_record(bool): имена в полях типа MNEM выводить в ообычном строковом виде, не обрезая стеммингом
        
        Returns:
            AddressDbRecord: результат
        """
        res = AddressDbRecord()
        res.coef = addr.coef
        res.message = addr.error_message
        res.normal_string = addr.get_full_path(", ", True, AddrLevel.UNDEFINED)
        res.country_code = addr.alpha2
        if (res.country_code is not None): 
            res.level = AddressDbLevel.COUNTRY
        need_district = False
        i = 0
        first_pass43 = True
        while True:
            if first_pass43: first_pass43 = False
            else: i += 1
            if (not (i < len(addr.items))): break
            it = addr.items[i]
            aa = Utils.asObjectOrNull(it.attrs, AreaAttributes)
            if (it.level == AddrLevel.REGIONAREA or it.level == AddrLevel.REGIONCITY): 
                res.region_mnem = AddressDbRecord.__create_area_mnem(aa, classic_model_record)
                res.level = AddressDbLevel.REGION
                res.region_guid = AddressDbRecord.__create_guids(it.gars)
                if (it.level == AddrLevel.REGIONCITY): 
                    res.city_mnem = res.region_mnem
                    res.city_guid = res.region_guid
            elif ((it.level == AddrLevel.DISTRICT and res.country_code is not None and res.country_code != "RU") and res.region_mnem is None and len(addr.items) == 2): 
                res.region_mnem = AddressDbRecord.__create_area_mnem(aa, classic_model_record)
                res.level = AddressDbLevel.REGION
            elif (it.level == AddrLevel.CITY): 
                res.city_mnem = AddressDbRecord.__create_area_mnem(aa, classic_model_record)
                res.level = AddressDbLevel.CITY
                if (len(it.gars) > 0): 
                    res.city_guid = AddressDbRecord.__create_guids(it.gars)
                else: 
                    res.city_guid = (None)
                    if (not "город" in aa.types): 
                        has_loc = False
                        j = i + 1
                        while j < len(addr.items): 
                            if (addr.items[j].level == AddrLevel.LOCALITY): 
                                has_loc = True
                            j += 1
                        if (not has_loc): 
                            res.location_mnem = res.city_mnem
                            res.location_guid = (None)
            elif (it.level == AddrLevel.LOCALITY): 
                res.location_mnem = AddressDbRecord.__create_area_mnem(aa, classic_model_record)
                res.level = AddressDbLevel.LOCALITY
                need_district = True
                if (len(it.gars) > 0): 
                    res.location_guid = AddressDbRecord.__create_guids(it.gars)
                else: 
                    res.location_guid = (None)
                    if (len(aa.types) == 1 and aa.types[0] == "населенный пункт" and res.city_mnem is None): 
                        if (i > 0 and addr.items[i - 1].level == AddrLevel.DISTRICT): 
                            pass
                        else: 
                            res.city_mnem = res.location_mnem
                            res.city_guid = (None)
                if (len(it.gars) == 1): 
                    gps = GpsObject.create_point_from_gar_object(it.gars[0])
                    if (gps is not None): 
                        res.gps_lat = gps.min_lat
                        res.gps_lon = gps.min_lon
                        res.gps_level = AddressDbLevel.LOCALITY
            elif (it.level == AddrLevel.TERRITORY): 
                if (res.territory_mnem is not None): 
                    if (len(it.gars) != 1): 
                        continue
                res.territory_mnem = AddressDbRecord.__create_area_mnem(aa, classic_model_record)
                res.level = AddressDbLevel.TERRITORY
                need_district = True
                res.territory_types = AddressDbRecord.__create_area_types(aa, True)
                if (len(it.gars) > 0): 
                    res.territory_guid = AddressDbRecord.__create_guids(it.gars)
                if (len(it.gars) == 1): 
                    gps = GpsObject.create_point_from_gar_object(it.gars[0])
                    if (gps is not None): 
                        res.gps_lat = gps.min_lat
                        res.gps_lon = gps.min_lon
                        res.gps_level = AddressDbLevel.TERRITORY
            elif (it.level == AddrLevel.STREET): 
                if (res.street_mnem is not None): 
                    if (len(it.gars) != 1): 
                        continue
                res.street_mnem = AddressDbRecord.__create_area_mnem(aa, classic_model_record)
                res.level = AddressDbLevel.STREET
                res.street_types = AddressDbRecord.__create_area_types(aa, False)
                if (len(it.gars) > 0): 
                    res.street_guid = AddressDbRecord.__create_guids(it.gars)
                if (len(it.gars) == 1): 
                    gps = GpsObject.create_point_from_gar_object(it.gars[0])
                    if (gps is not None): 
                        res.gps_lat = gps.min_lat
                        res.gps_lon = gps.min_lon
                        res.gps_level = AddressDbLevel.STREET
            elif (it.level == AddrLevel.PLOT or it.level == AddrLevel.BUILDING): 
                res.house_mnem = AddressDbRecord.__create_house_mnem(Utils.asObjectOrNull(it.attrs, HouseAttributes), it.level == AddrLevel.PLOT, classic_model_record)
                res.level = AddressDbLevel.HOUSE
                res.house_guid = AddressDbRecord.__create_guids(it.gars)
                if (len(it.gars) == 1): 
                    gps = GpsObject.create_point_from_gar_object(it.gars[0])
                    if (gps is not None): 
                        res.gps_lat = gps.min_lat
                        res.gps_lon = gps.min_lon
                        res.gps_level = AddressDbLevel.HOUSE
            elif (it.level == AddrLevel.APARTMENT or it.level == AddrLevel.ROOM): 
                ro = Utils.asObjectOrNull(it.attrs, RoomAttributes)
                if (ro.number is None or ro.number == "0"): 
                    if (len(it.gars) == 0): 
                        continue
                if (res.apartment_mnem is not None): 
                    continue
                res.apartment_mnem = AddressDbRecord.__create_apart_mnem(Utils.asObjectOrNull(it.attrs, RoomAttributes), classic_model_record)
                res.level = AddressDbLevel.APARTMENT
                res.apartment_guid = AddressDbRecord.__create_guids(it.gars)
        if (need_district): 
            distr = None
            for i in range(len(addr.items) - 1, 0, -1):
                it = addr.items[i]
                if (it.level == AddrLevel.CITY): 
                    break
                if (it.level == AddrLevel.SETTLEMENT or it.level == AddrLevel.DISTRICT or it.level == AddrLevel.CITYDISTRICT): 
                    pass
                else: 
                    continue
                if (it.level == AddrLevel.SETTLEMENT and i > 0): 
                    if (addr.items[i - 1].level == AddrLevel.DISTRICT): 
                        continue
                mnem = AddressDbRecord.__create_area_mnem(Utils.asObjectOrNull(it.attrs, AreaAttributes), classic_model_record)
                if (mnem == res.location_mnem and res.location_mnem is not None): 
                    distr = it
                    continue
                res.district_mnem = mnem
                res.district_guid = AddressDbRecord.__create_guids(it.gars)
                break
            if (res.district_mnem is None and distr is not None): 
                res.district_mnem = AddressDbRecord.__create_area_mnem(Utils.asObjectOrNull(distr.attrs, AreaAttributes), classic_model_record)
                res.district_guid = AddressDbRecord.__create_guids(distr.gars)
        if (len(addr.params) > 0): 
            tmp = io.StringIO()
            for kp in addr.params.items(): 
                if (tmp.tell() > 0): 
                    print(", ", end="", file=tmp)
                if (kp[0] != ParamType.ORGANIZATION): 
                    print("{0} ".format(AddressHelper.get_param_type_string(kp[0])), end="", file=tmp, flush=True)
                print(Utils.ifNotNull(kp[1], ""), end="", file=tmp)
            res.miscs = Utils.toStringStringIO(tmp)
        return res
    
    @staticmethod
    def __create_guids(gars : typing.List['GarObject']) -> str:
        if (len(gars) == 0): 
            return None
        res = None
        i = 0
        first_pass44 = True
        while True:
            if first_pass44: first_pass44 = False
            else: i += 1
            if (not (i < len(gars))): break
            gui = gars[i].get_param_value(GarParam.GUID)
            if (gui is None): 
                continue
            if (res is None): 
                res = str(gui)
            else: 
                res = "{0}|{1}".format(res, str(gui))
        return res
    
    @staticmethod
    def __create_area_mnem(aa : 'AreaAttributes', classic_model_record : bool) -> str:
        if (classic_model_record): 
            return str(aa)
        if (len(aa.names) == 0): 
            if (aa.number is not None): 
                return "{0}".format(aa.number)
            if (len(aa.types) > 0): 
                return "{0}".format(aa.types[0].upper())
            return None
        nam = aa.names[0].upper()
        words = list()
        tmp = io.StringIO()
        i = 0
        first_pass45 = True
        while True:
            if first_pass45: first_pass45 = False
            else: i += 1
            if (not (i < len(nam))): break
            if (not str.isalpha(nam[i])): 
                continue
            j = 0
            j = (i + 1)
            while j < len(nam): 
                if (not str.isalpha(nam[j])): 
                    break
                j += 1
            j0 = 0
            j0 = (j - 1)
            while j0 >= i: 
                ch = nam[j0]
                if ("АЕЁИЙОУЫЮЯ".find(ch) < 0): 
                    break
                if ((ch == 'О' and j0 > (i + 4) and nam[j0 - 1] == 'Г') and nam[j0 - 2] == 'О'): 
                    j0 -= 2
                j0 -= 1
            if (j0 <= (i + 2)): 
                j0 = (j - 1)
            Utils.setLengthStringIO(tmp, 0)
            k = i
            first_pass46 = True
            while True:
                if first_pass46: first_pass46 = False
                else: k += 1
                if (not (k <= j0)): break
                ch = nam[k]
                if (ch == 'Ъ' or ch == 'Ь'): 
                    continue
                if (ch == 'Ё'): 
                    ch = 'Е'
                elif (ch == 'Щ'): 
                    ch = 'Ш'
                if (tmp.tell() > 0 and Utils.getCharAtStringIO(tmp, tmp.tell() - 1) == ch): 
                    continue
                print(ch, end="", file=tmp)
            w = Utils.toStringStringIO(tmp)
            if (not w in words): 
                words.append(w)
            i = j
        if (len(words) > 0): 
            words.sort()
        if (len(words) == 1 and aa.number is None): 
            return words[0]
        Utils.setLengthStringIO(tmp, 0)
        for w in words: 
            if (tmp.tell() > 0): 
                print(' ', end="", file=tmp)
            print(w, end="", file=tmp)
        if (aa.number is not None): 
            if (tmp.tell() > 0): 
                print(' ', end="", file=tmp)
            print(aa.number, end="", file=tmp)
        return Utils.toStringStringIO(tmp)
    
    @staticmethod
    def __create_area_types(aa : 'AreaAttributes', terr : bool) -> str:
        if (len(aa.types) == 0): 
            return None
        res = io.StringIO()
        for ty in aa.types: 
            if (ty != "территория"): 
                if (res.tell() > 0): 
                    print('|', end="", file=res)
                print(ty, end="", file=res)
        if (res.tell() == 0): 
            for ty in aa.miscs: 
                if (res.tell() > 0): 
                    print('|', end="", file=res)
                print(ty, end="", file=res)
        if (res.tell() == 0): 
            return None
        return Utils.toStringStringIO(res).upper()
    
    @staticmethod
    def __create_house_mnem(ha : 'HouseAttributes', plot : bool, classic_model_record : bool) -> str:
        if (classic_model_record): 
            return str(ha)
        res = io.StringIO()
        if (plot): 
            print('У', end="", file=res)
        elif (ha.typ == HouseType.BOILER or ha.typ == HouseType.MINE or ha.typ == HouseType.WELL): 
            print('Х', end="", file=res)
        elif (ha.typ == HouseType.GARAGE): 
            print("Г", end="", file=res)
        elif (ha.number is None and ha.build_number is not None): 
            print('К', end="", file=res)
        elif (ha.number is None and ha.build_number is None and ha.stroen_number is not None): 
            print('С', end="", file=res)
        else: 
            print('Д', end="", file=res)
        if (ha.plot_number is not None): 
            AddressDbRecord.__add_num(res, ha.plot_number)
        if (ha.number is not None): 
            AddressDbRecord.__add_num(res, ha.number)
        if (ha.build_number is not None): 
            AddressDbRecord.__add_num(res, ha.build_number)
        if (ha.stroen_number is not None): 
            AddressDbRecord.__add_num(res, ha.stroen_number)
        if (res.tell() == 1): 
            print('0', end="", file=res)
        return Utils.toStringStringIO(res)
    
    @staticmethod
    def __create_apart_mnem(ra : 'RoomAttributes', classic_model_record : bool) -> str:
        if (classic_model_record): 
            return str(ra)
        res = io.StringIO()
        if (ra.typ == RoomType.FLAT): 
            print("К", end="", file=res)
        elif (ra.typ == RoomType.CARPLACE): 
            print("М", end="", file=res)
        else: 
            print("П", end="", file=res)
        if (ra.number is not None): 
            AddressDbRecord.__add_num(res, ra.number)
        if (res.tell() == 1): 
            print('0', end="", file=res)
        return Utils.toStringStringIO(res)
    
    @staticmethod
    def __add_num(res : io.StringIO, val : str) -> None:
        if (Utils.isNullOrEmpty(val) or val == "б/н"): 
            return
        i = 0
        first_pass47 = True
        while True:
            if first_pass47: first_pass47 = False
            else: i += 1
            if (not (i < len(val))): break
            if (not str.isalnum(val[i])): 
                continue
            j = 0
            j = (i + 1)
            while j < len(val): 
                if (str.isalpha(val[i])): 
                    if (not str.isalpha(val[j])): 
                        break
                elif (str.isdigit(val[i])): 
                    if (not str.isdigit(val[j])): 
                        break
                else: 
                    break
                j += 1
            if (res.tell() > 1): 
                print(' ', end="", file=res)
            print(val[i:i+j - i], end="", file=res)
            i = (j - 1)
    
    def get_guid_value(self, lev : 'AddressDbLevel') -> str:
        if (lev == AddressDbLevel.COUNTRY): 
            return None
        if (lev == AddressDbLevel.REGION): 
            return self.region_guid
        if (lev == AddressDbLevel.CITY): 
            return self.city_guid
        if (lev == AddressDbLevel.DISTRICT): 
            return self.district_guid
        if (lev == AddressDbLevel.LOCALITY): 
            return self.location_guid
        if (lev == AddressDbLevel.TERRITORY): 
            return self.territory_guid
        if (lev == AddressDbLevel.STREET): 
            return self.street_guid
        if (lev == AddressDbLevel.HOUSE): 
            return self.house_guid
        if (lev == AddressDbLevel.APARTMENT): 
            return self.apartment_guid
        return None
    
    def get_mnem_value(self, lev : 'AddressDbLevel') -> str:
        if (lev == AddressDbLevel.COUNTRY): 
            return None
        if (lev == AddressDbLevel.REGION): 
            return self.region_mnem
        if (lev == AddressDbLevel.CITY): 
            return self.city_mnem
        if (lev == AddressDbLevel.DISTRICT): 
            return self.district_mnem
        if (lev == AddressDbLevel.LOCALITY): 
            return self.location_mnem
        if (lev == AddressDbLevel.TERRITORY): 
            return self.territory_mnem
        if (lev == AddressDbLevel.STREET): 
            return self.street_mnem
        if (lev == AddressDbLevel.HOUSE): 
            return self.house_mnem
        if (lev == AddressDbLevel.APARTMENT): 
            return self.apartment_mnem
        return None
    
    def get_types_value(self, lev : 'AddressDbLevel') -> str:
        if (lev == AddressDbLevel.TERRITORY): 
            return self.territory_types
        if (lev == AddressDbLevel.STREET): 
            return self.street_types
        return None
    
    def is_best_than_old(self, old : 'AddressDbRecord') -> bool:
        """ Проверить, что текущий адрес лучше, чем эквивалентный старый, так что
        в БД старый нужно перезаписать. Например, улучшилос качество выделения,
        появились координаты GPS и пр.
        
        Args:
            old(AddressDbRecord): эквивалентный адрес в базе
        
        """
        if (old.level != self.level): 
            return False
        if (self.gps_lon != 0 and old.gps_lon == 0): 
            return True
        if (self.coef > old.coef): 
            return True
        if (self.coef < old.coef): 
            return False
        if (self.apartment_guid is not None and old.apartment_guid is None): 
            return True
        if (self.apartment_guid is None and old.apartment_guid is not None): 
            return False
        if (self.house_guid is not None and old.house_guid is None): 
            return True
        if (self.house_guid is None and old.house_guid is not None): 
            return False
        if (self.street_guid is not None and old.street_guid is None): 
            return True
        if (self.street_guid is None and old.street_guid is not None): 
            return False
        if (self.territory_guid is not None and old.territory_guid is None): 
            return True
        if (self.territory_guid is None and old.territory_guid is not None): 
            return False
        if (self.location_guid is not None and old.location_guid is None): 
            return True
        if (self.location_guid is None and old.location_guid is not None): 
            return False
        if (self.city_guid is not None and old.city_guid is None): 
            return True
        if (self.city_guid is None and old.city_guid is not None): 
            return False
        if (self.region_guid is not None and old.region_guid is None): 
            return True
        if (self.region_guid is None and old.region_guid is not None): 
            return False
        return False