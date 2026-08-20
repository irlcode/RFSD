# SDK Pullenti Address (client version), version 4.28, february 2025.
# Copyright (c) 2013-2025, Pullenti. All rights reserved.
# Non-Commercial Freeware and Commercial Software.
# This class is generated using the converter Unisharping (www.unisharping.ru) from Pullenti C# project.
# The latest version of the code is available on the site www.pullenti.ru

import typing
import io
from pullenti.unisharp.Utils import Utils
from pullenti.unisharp.Misc import RefOutArgWrapper

from pullenti.address.AddressDbLevel import AddressDbLevel
from pullenti.address.GpsObject import GpsObject

class SqlAddressHelper:
    """ Поддержка работы с СУБД """
    
    def __init__(self) -> None:
        self.address_source_name = "TEXT"
        self.address_normalize_name = "NORMAL"
        self.address_coef_name = "COEF"
        self.address_level_name = "LEVEL"
        self.address_message_name = "MESSAGE"
        self.country_code_name = "COUNTRY_CODE"
        self.region_mnem_name = "REGION_MNEM"
        self.region_guid_name = "REGION_GUID"
        self.city_mnem_name = "CITY_MNEM"
        self.city_guid_name = "CITY_GUID"
        self.district_mnem_name = "DISTRICT_MNEM"
        self.district_guid_name = "DISTRICT_GUID"
        self.location_mnem_name = "LOCATION_MNEM"
        self.location_guid_name = "LOCATION_GUID"
        self.territory_mnem_name = "TERRITORY_MNEM"
        self.territory_type_name = "TERRITORY_TYPE"
        self.territory_guid_name = "TERRITORY_GUID"
        self.street_mnem_name = "STREET_MNEM"
        self.street_type_name = "STREET_TYPE"
        self.street_guid_name = "STREET_GUID"
        self.house_mnem_name = "HOUSE_MNEM"
        self.house_guid_name = "HOUSE_GUID"
        self.apartment_mnem_name = "APARTMENT_MNEM"
        self.apartment_guid_name = "APARTMENT_GUID"
        self.gps_lat_name = "GPS_LAT"
        self.gps_lon_name = "GPS_LON"
        self.gps_level_name = "GPS_LEVEL"
        self.misc_name = "MISC"
    
    GLOBAL = None
    
    COLUMNS = None
    
    @staticmethod
    def initialize() -> None:
        SqlAddressHelper.GLOBAL = SqlAddressHelper()
        SqlAddressHelper.COLUMNS = SqlAddressHelper.GLOBAL.get_all_column_names()
    
    def get_all_column_names(self) -> typing.List[str]:
        """ Получить полный список имён колонок
        
        """
        res = list()
        if (self.address_source_name is not None): 
            res.append(self.address_source_name)
        if (self.address_normalize_name is not None): 
            res.append(self.address_normalize_name)
        if (self.address_coef_name is not None): 
            res.append(self.address_coef_name)
        if (self.address_level_name is not None): 
            res.append(self.address_level_name)
        if (self.address_message_name is not None): 
            res.append(self.address_message_name)
        if (self.country_code_name is not None): 
            res.append(self.country_code_name)
        if (self.region_mnem_name is not None): 
            res.append(self.region_mnem_name)
        if (self.region_guid_name is not None): 
            res.append(self.region_guid_name)
        if (self.city_mnem_name is not None): 
            res.append(self.city_mnem_name)
        if (self.city_guid_name is not None): 
            res.append(self.city_guid_name)
        if (self.district_mnem_name is not None): 
            res.append(self.district_mnem_name)
        if (self.district_guid_name is not None): 
            res.append(self.district_guid_name)
        if (self.location_mnem_name is not None): 
            res.append(self.location_mnem_name)
        if (self.location_guid_name is not None): 
            res.append(self.location_guid_name)
        if (self.territory_mnem_name is not None): 
            res.append(self.territory_mnem_name)
        if (self.territory_type_name is not None): 
            res.append(self.territory_type_name)
        if (self.territory_guid_name is not None): 
            res.append(self.territory_guid_name)
        if (self.street_mnem_name is not None): 
            res.append(self.street_mnem_name)
        if (self.street_type_name is not None): 
            res.append(self.street_type_name)
        if (self.street_guid_name is not None): 
            res.append(self.street_guid_name)
        if (self.house_mnem_name is not None): 
            res.append(self.house_mnem_name)
        if (self.house_guid_name is not None): 
            res.append(self.house_guid_name)
        if (self.apartment_mnem_name is not None): 
            res.append(self.apartment_mnem_name)
        if (self.apartment_guid_name is not None): 
            res.append(self.apartment_guid_name)
        if (self.gps_lat_name is not None): 
            res.append(self.gps_lat_name)
        if (self.gps_lon_name is not None): 
            res.append(self.gps_lon_name)
        if (self.gps_level_name is not None): 
            res.append(self.gps_level_name)
        if (self.misc_name is not None): 
            res.append(self.misc_name)
        return res
    
    def get_column_value(self, rec : 'AddressDbRecord', column_name : str) -> str:
        """ Получить значение колонки по её названию
        
        Args:
            rec(AddressDbRecord): запись с адресом
            column_name(str): наименование колонки
        
        Returns:
            str: значение
        """
        if (column_name == self.address_source_name): 
            return rec.source_string
        if (column_name == self.address_normalize_name): 
            return rec.normal_string
        if (column_name == self.address_coef_name): 
            return str(rec.coef)
        if (column_name == self.address_level_name): 
            return str(rec.level)
        if (column_name == self.address_message_name): 
            return rec.message
        if (column_name == self.country_code_name): 
            return rec.country_code
        if (column_name == self.region_mnem_name): 
            return rec.region_mnem
        if (column_name == self.region_guid_name): 
            return rec.region_guid
        if (column_name == self.city_mnem_name): 
            return rec.city_mnem
        if (column_name == self.city_guid_name): 
            return rec.city_guid
        if (column_name == self.district_mnem_name): 
            return rec.district_mnem
        if (column_name == self.district_guid_name): 
            return rec.district_guid
        if (column_name == self.location_mnem_name): 
            return rec.location_mnem
        if (column_name == self.location_guid_name): 
            return rec.location_guid
        if (column_name == self.territory_mnem_name): 
            return rec.territory_mnem
        if (column_name == self.territory_type_name): 
            return rec.territory_types
        if (column_name == self.territory_guid_name): 
            return rec.territory_guid
        if (column_name == self.street_mnem_name): 
            return rec.street_mnem
        if (column_name == self.street_type_name): 
            return rec.street_types
        if (column_name == self.street_guid_name): 
            return rec.street_guid
        if (column_name == self.house_mnem_name): 
            return rec.house_mnem
        if (column_name == self.house_guid_name): 
            return rec.house_guid
        if (column_name == self.apartment_mnem_name): 
            return rec.apartment_mnem
        if (column_name == self.apartment_guid_name): 
            return rec.apartment_guid
        if (column_name == self.gps_lat_name): 
            return GpsObject.out_double(rec.gps_lat)
        if (column_name == self.gps_lon_name): 
            return GpsObject.out_double(rec.gps_lon)
        if (column_name == self.gps_level_name): 
            return str(rec.gps_level)
        if (column_name == self.misc_name): 
            return rec.miscs
        return None
    
    @staticmethod
    def __get_val_array(val : object) -> str:
        if (isinstance(val, str)): 
            return Utils.asObjectOrNull(val, str)
        li = Utils.asObjectOrNull(val, list)
        if (li is not None): 
            if (len(li) == 0): 
                return None
            res = str(li[0])
            i = 1
            while i < len(li): 
                res = "{0}|{1}".format(res, str(li[i]))
                i += 1
            return res
        sli = Utils.asObjectOrNull(val, list)
        if (sli is not None): 
            if (len(sli) == 0): 
                return None
            res = str(sli[0])
            i = 1
            while i < len(sli): 
                res = "{0}|{1}".format(res, str(sli[i]))
                i += 1
            return res
        return None
    
    def set_column_value(self, rec : 'AddressDbRecord', column_name : str, value : object) -> None:
        """ Установить значение колонки
        
        Args:
            rec(AddressDbRecord): запись с адресом
            column_name(str): имя колонки
            value(object): значение
        """
        if (value is None): 
            return
        if (column_name == self.address_source_name): 
            rec.source_string = (Utils.asObjectOrNull(value, str))
        elif (column_name == self.address_normalize_name): 
            rec.normal_string = (Utils.asObjectOrNull(value, str))
        elif (column_name == self.address_coef_name): 
            rec.coef = int(str(value))
        elif (column_name == self.address_level_name): 
            rec.level = ((int(str(value))))
        elif (column_name == self.address_message_name): 
            rec.message = (Utils.asObjectOrNull(value, str))
        elif (column_name == self.country_code_name): 
            rec.country_code = (Utils.asObjectOrNull(value, str))
        elif (column_name == self.region_mnem_name): 
            rec.region_mnem = (Utils.asObjectOrNull(value, str))
        elif (column_name == self.region_guid_name): 
            rec.region_guid = SqlAddressHelper.__get_val_array(value)
        elif (column_name == self.city_mnem_name): 
            rec.city_mnem = (Utils.asObjectOrNull(value, str))
        elif (column_name == self.city_guid_name): 
            rec.city_guid = SqlAddressHelper.__get_val_array(value)
        elif (column_name == self.district_mnem_name): 
            rec.district_mnem = (Utils.asObjectOrNull(value, str))
        elif (column_name == self.district_guid_name): 
            rec.district_guid = SqlAddressHelper.__get_val_array(value)
        elif (column_name == self.location_mnem_name): 
            rec.location_mnem = (Utils.asObjectOrNull(value, str))
        elif (column_name == self.location_guid_name): 
            rec.location_guid = SqlAddressHelper.__get_val_array(value)
        elif (column_name == self.territory_mnem_name): 
            rec.territory_mnem = (Utils.asObjectOrNull(value, str))
        elif (column_name == self.territory_type_name): 
            rec.territory_types = SqlAddressHelper.__get_val_array(value)
        elif (column_name == self.territory_guid_name): 
            rec.territory_guid = SqlAddressHelper.__get_val_array(value)
        elif (column_name == self.street_mnem_name): 
            rec.street_mnem = (Utils.asObjectOrNull(value, str))
        elif (column_name == self.street_type_name): 
            rec.street_types = SqlAddressHelper.__get_val_array(value)
        elif (column_name == self.street_guid_name): 
            rec.street_guid = SqlAddressHelper.__get_val_array(value)
        elif (column_name == self.house_mnem_name): 
            rec.house_mnem = (Utils.asObjectOrNull(value, str))
        elif (column_name == self.house_guid_name): 
            rec.house_guid = SqlAddressHelper.__get_val_array(value)
        elif (column_name == self.apartment_mnem_name): 
            rec.apartment_mnem = (Utils.asObjectOrNull(value, str))
        elif (column_name == self.apartment_guid_name): 
            rec.apartment_guid = SqlAddressHelper.__get_val_array(value)
        elif (column_name == self.gps_lat_name): 
            d = 0
            wrapd35 = RefOutArgWrapper(0)
            inoutres36 = GpsObject.try_parse_double(str(value), wrapd35)
            d = wrapd35.value
            if (inoutres36): 
                rec.gps_lat = d
        elif (column_name == self.gps_lon_name): 
            d = 0
            wrapd37 = RefOutArgWrapper(0)
            inoutres38 = GpsObject.try_parse_double(str(value), wrapd37)
            d = wrapd37.value
            if (inoutres38): 
                rec.gps_lon = d
        elif (column_name == self.gps_level_name): 
            n = 0
            wrapn39 = RefOutArgWrapper(0)
            inoutres40 = Utils.tryParseInt(Utils.ifNotNull(str(value), ""), wrapn39)
            n = wrapn39.value
            if (inoutres40): 
                rec.gps_level = (Utils.valToEnum(n, AddressDbLevel))
        elif (column_name == self.misc_name): 
            rec.miscs = (Utils.asObjectOrNull(value, str))
    
    def get_column_guid_name_by_level(self, lev : 'AddressDbLevel') -> str:
        if (lev == AddressDbLevel.REGION): 
            return self.region_guid_name
        if (lev == AddressDbLevel.CITY): 
            return self.city_guid_name
        if (lev == AddressDbLevel.DISTRICT): 
            return self.district_guid_name
        if (lev == AddressDbLevel.LOCALITY): 
            return self.location_guid_name
        if (lev == AddressDbLevel.TERRITORY): 
            return self.territory_guid_name
        if (lev == AddressDbLevel.STREET): 
            return self.street_guid_name
        if (lev == AddressDbLevel.HOUSE): 
            return self.house_guid_name
        if (lev == AddressDbLevel.APARTMENT): 
            return self.apartment_guid_name
        return None
    
    def get_column_mnem_name_by_level(self, lev : 'AddressDbLevel') -> str:
        if (lev == AddressDbLevel.REGION): 
            return self.region_mnem_name
        if (lev == AddressDbLevel.CITY): 
            return self.city_mnem_name
        if (lev == AddressDbLevel.DISTRICT): 
            return self.district_mnem_name
        if (lev == AddressDbLevel.LOCALITY): 
            return self.location_mnem_name
        if (lev == AddressDbLevel.TERRITORY): 
            return self.territory_mnem_name
        if (lev == AddressDbLevel.STREET): 
            return self.street_mnem_name
        if (lev == AddressDbLevel.HOUSE): 
            return self.house_mnem_name
        if (lev == AddressDbLevel.APARTMENT): 
            return self.apartment_mnem_name
        return None
    
    def get_column_types_name_by_level(self, lev : 'AddressDbLevel') -> str:
        if (lev == AddressDbLevel.TERRITORY): 
            return self.territory_type_name
        if (lev == AddressDbLevel.STREET): 
            return self.street_type_name
        return None
    
    def generate_where_equal(self, addr : 'AddressDbRecord', pars : typing.List[tuple]) -> str:
        """ Сгенерировать and-условия для поиска эквивалентного адреса
        
        Args:
            addr(AddressDbRecord): запись адреса, аналог котороо нужно найти
            pars(typing.List[tuple]): сюда будут записаны параметры (а в SQL будут вставлены мнемоники @...)
        
        Returns:
            str: вернёт SQL-условие, которое нужно добавить в текст Where
        """
        sql = io.StringIO()
        for ilev in range(addr.level, 0, -1):
            lev = Utils.valToEnum(ilev, AddressDbLevel)
            if (lev == AddressDbLevel.COUNTRY): 
                pars["@" + self.country_code_name] = addr.country_code
                if (sql.tell() > 0): 
                    print(" and ", end="", file=sql)
                print("{0} = @{0}".format(self.country_code_name), end="", file=sql, flush=True)
                continue
            gname = self.get_column_guid_name_by_level(lev)
            guid = None
            if (gname is not None): 
                guid = self.get_column_value(addr, gname)
            if (guid is not None): 
                if (sql.tell() > 0): 
                    print(" and ", end="", file=sql)
                if (guid.find('|') < 0): 
                    pars["@" + gname] = guid
                    print("@{0} = any({0})".format(gname), end="", file=sql, flush=True)
                else: 
                    guids = Utils.splitString(guid, '|', False)
                    print("(", end="", file=sql)
                    i = 0
                    while i < len(guids): 
                        if (i > 0): 
                            print(" or ", end="", file=sql)
                        pars["@" + gname + str(i)] = guids[i]
                        print("@{0}{1} = any({0})".format(gname, i), end="", file=sql, flush=True)
                        i += 1
                    print(")", end="", file=sql)
                break
            mname = self.get_column_mnem_name_by_level(lev)
            tname = self.get_column_types_name_by_level(lev)
            mval = None
            if (mname is not None): 
                mval = self.get_column_value(addr, mname)
            tval = None
            if (tname is not None): 
                tval = self.get_column_value(addr, tname)
            if (mval is None): 
                continue
            if (sql.tell() > 0): 
                print(" and ", end="", file=sql)
            pars["@" + mname] = mval
            print("@{0} = {0}".format(mname), end="", file=sql, flush=True)
            if (tval is None): 
                continue
            print(" and ", end="", file=sql)
            typs = Utils.splitString(tval, '|', False)
            if (len(typs) > 1): 
                print("(", end="", file=sql)
            i = 0
            while i < len(typs): 
                pars["@" + tname + str(i)] = typs[i]
                if (i > 0): 
                    print(" or ", end="", file=sql)
                print("@{0}{1} = any({0})".format(tname, i), end="", file=sql, flush=True)
                i += 1
            if (len(typs) > 1): 
                print(")", end="", file=sql)
        if (sql.tell() == 0): 
            return None
        if (addr.level != AddressDbLevel.APARTMENT): 
            pars["@" + self.address_level_name] = addr.level
            print(" and {0} = @{0}".format(self.address_level_name), end="", file=sql, flush=True)
        return Utils.toStringStringIO(sql)