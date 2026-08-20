# SDK Pullenti Address (client version), version 4.28, february 2025.
# Copyright (c) 2013-2025, Pullenti. All rights reserved.
# Non-Commercial Freeware and Commercial Software.
# This class is generated using the converter Unisharping (www.unisharping.ru) from Pullenti C# project.
# The latest version of the code is available on the site www.pullenti.ru

import io
from pullenti.unisharp.Utils import Utils

from pullenti.address.RoomType import RoomType
from pullenti.address.StroenType import StroenType
from pullenti.address.ParamType import ParamType
from pullenti.address.DetailType import DetailType
from pullenti.address.AddrLevel import AddrLevel
from pullenti.address.GarLevel import GarLevel
from pullenti.address.HouseType import HouseType
from pullenti.address.GarParam import GarParam

class AddressHelper:
    """ Разные полезные функции """
    
    @staticmethod
    def get_gar_level_string(level : 'GarLevel') -> str:
        """ Получить описание уровня ГАР
        
        Args:
            level(GarLevel): 
        
        """
        if (level == GarLevel.REGION): 
            return "регион"
        if (level == GarLevel.ADMINAREA): 
            return "административный район"
        if (level == GarLevel.MUNICIPALAREA): 
            return "муниципальный район"
        if (level == GarLevel.SETTLEMENT): 
            return "сельское/городское поселение"
        if (level == GarLevel.CITY): 
            return "город"
        if (level == GarLevel.LOCALITY): 
            return "населенный пункт"
        if (level == GarLevel.DISTRICT): 
            return "район города"
        if (level == GarLevel.AREA): 
            return "элемент планировочной структуры"
        if (level == GarLevel.STREET): 
            return "элемент улично-дорожной сети"
        if (level == GarLevel.PLOT): 
            return "земельный участок"
        if (level == GarLevel.BUILDING): 
            return "здание (сооружение)"
        if (level == GarLevel.ROOM): 
            return "помещение"
        if (level == GarLevel.CARPLACE): 
            return "машино-место"
        return Utils.enumToString(level)
    
    @staticmethod
    def get_addr_level_string(level : 'AddrLevel') -> str:
        """ Получить описание алресного уровня
        
        Args:
            level(AddrLevel): 
        
        """
        if (level == AddrLevel.COUNTRY): 
            return "страна"
        if (level == AddrLevel.REGIONAREA): 
            return "регион"
        if (level == AddrLevel.REGIONCITY): 
            return "город-регион"
        if (level == AddrLevel.DISTRICT): 
            return "район"
        if (level == AddrLevel.SETTLEMENT): 
            return "поселение"
        if (level == AddrLevel.CITY): 
            return "город"
        if (level == AddrLevel.CITYDISTRICT): 
            return "городской район"
        if (level == AddrLevel.LOCALITY): 
            return "населенный пункт"
        if (level == AddrLevel.TERRITORY): 
            return "элемент планировочной структуры"
        if (level == AddrLevel.STREET): 
            return "элемент улично-дорожной сети"
        if (level == AddrLevel.PLOT): 
            return "земельный участок"
        if (level == AddrLevel.BUILDING): 
            return "здание (сооружение)"
        if (level == AddrLevel.APARTMENT): 
            return "помещение"
        if (level == AddrLevel.ROOM): 
            return "комната"
        if (level == AddrLevel.UNDEFINED): 
            return "непонятно что"
        return Utils.enumToString(level)
    
    @staticmethod
    def get_gar_level_image_name(level : 'GarLevel') -> str:
        """ Получить мнемонику картинки для уровня (по мнемонике саму картинку можно получить функцией FindImage)
        
        Args:
            level(GarLevel): 
        
        """
        if (level == GarLevel.REGION): 
            return "region"
        if (level == GarLevel.ADMINAREA): 
            return "admin"
        if (level == GarLevel.MUNICIPALAREA): 
            return "municipal"
        if (level == GarLevel.SETTLEMENT): 
            return "settlement"
        if (level == GarLevel.CITY): 
            return "city"
        if (level == GarLevel.LOCALITY): 
            return "locality"
        if (level == GarLevel.DISTRICT): 
            return "district"
        if (level == GarLevel.AREA): 
            return "area"
        if (level == GarLevel.STREET): 
            return "street"
        if (level == GarLevel.PLOT): 
            return "plot"
        if (level == GarLevel.BUILDING): 
            return "building"
        if (level == GarLevel.ROOM): 
            return "room"
        if (level == GarLevel.CARPLACE): 
            return "carplace"
        return "undefined"
    
    @staticmethod
    def get_addr_level_image_name(level : 'AddrLevel') -> str:
        """ Получить мнемонику картинки для уровня (по мнемонике саму картинку можно получить функцией FindImage)
        
        Args:
            level(AddrLevel): 
        
        """
        if (level == AddrLevel.COUNTRY): 
            return "country"
        if (level == AddrLevel.REGIONAREA): 
            return "region"
        if (level == AddrLevel.REGIONCITY): 
            return "city"
        if (level == AddrLevel.DISTRICT): 
            return "municipal"
        if (level == AddrLevel.SETTLEMENT): 
            return "settlement"
        if (level == AddrLevel.CITY): 
            return "city"
        if (level == AddrLevel.CITYDISTRICT): 
            return "municipal"
        if (level == AddrLevel.LOCALITY): 
            return "locality"
        if (level == AddrLevel.TERRITORY): 
            return "area"
        if (level == AddrLevel.STREET): 
            return "street"
        if (level == AddrLevel.PLOT): 
            return "plot"
        if (level == AddrLevel.BUILDING): 
            return "building"
        if (level == AddrLevel.APARTMENT): 
            return "room"
        if (level == AddrLevel.ROOM): 
            return "room"
        return "undefined"
    
    @staticmethod
    def get_gar_param_string(p : 'GarParam') -> str:
        if (p == GarParam.GUID): 
            return "код Guid"
        if (p == GarParam.KLADRCODE): 
            return "код КЛАДР"
        if (p == GarParam.POSTINDEX): 
            return "почтовый индекс"
        if (p == GarParam.OKATO): 
            return "код ОКАТО"
        if (p == GarParam.OKTMO): 
            return "код ОКТМО"
        if (p == GarParam.KADASTERNUMBER): 
            return "кадастровый номер"
        if (p == GarParam.REESTERNUMBER): 
            return "реестровый номер"
        if (p == GarParam.OBJECTID): 
            return "внутренний числовой идентификатор ГАР"
        if (p == GarParam.GPSPOINT): 
            return "точка GPS"
        if (p == GarParam.GPSRECTANGLE): 
            return "прямоугольник GPS"
        if (p == GarParam.PURPOSE): 
            return "назначение сооружения"
        if (p == GarParam.FLOORS): 
            return "этажность"
        if (p == GarParam.YEAR): 
            return "год ввода в эксплуатацию"
        return Utils.enumToString(p)
    
    @staticmethod
    def compare_levels(lev1 : 'AddrLevel', lev2 : 'AddrLevel') -> int:
        """ Сравнение уровней для сортировки
        
        Args:
            lev1(AddrLevel): первый уровень
            lev2(AddrLevel): второй уровень
        
        Returns:
            int: -1 первый меньше, +1 первый больше, 0 равны
        """
        r1 = lev1
        r2 = lev2
        if (r1 < r2): 
            return -1
        if (r1 > r2): 
            return 1
        return 0
    
    @staticmethod
    def can_be_equal_levels(a : 'AddrLevel', g : 'GarLevel') -> bool:
        if (a == AddrLevel.COUNTRY): 
            return False
        if (a == AddrLevel.REGIONCITY or a == AddrLevel.REGIONAREA): 
            return g == GarLevel.REGION
        if (a == AddrLevel.DISTRICT): 
            return g == GarLevel.MUNICIPALAREA or g == GarLevel.ADMINAREA or g == GarLevel.DISTRICT
        if (a == AddrLevel.SETTLEMENT): 
            return g == GarLevel.SETTLEMENT
        if (a == AddrLevel.CITY): 
            return g == GarLevel.CITY
        if (a == AddrLevel.LOCALITY): 
            return g == GarLevel.LOCALITY or g == GarLevel.AREA or g == GarLevel.ADMINAREA
        if (a == AddrLevel.TERRITORY): 
            return g == GarLevel.AREA or g == GarLevel.DISTRICT
        if (a == AddrLevel.STREET): 
            return g == GarLevel.STREET
        if (a == AddrLevel.PLOT): 
            return g == GarLevel.PLOT
        if (a == AddrLevel.BUILDING): 
            return g == GarLevel.BUILDING
        if (a == AddrLevel.APARTMENT or a == AddrLevel.ROOM): 
            return g == GarLevel.ROOM
        return False
    
    @staticmethod
    def can_be_parent(ch : 'AddrLevel', par : 'AddrLevel') -> bool:
        """ Проверка уровней на предмет прямого родителя
        
        Args:
            ch(AddrLevel): прямой потомок
            par(AddrLevel): родитель
        
        Returns:
            bool: может ли быть
        """
        if (ch == AddrLevel.COUNTRY): 
            return False
        if (ch == AddrLevel.REGIONCITY or ch == AddrLevel.REGIONAREA): 
            return par == AddrLevel.COUNTRY
        if (ch == AddrLevel.DISTRICT): 
            if (par == AddrLevel.COUNTRY or par == AddrLevel.REGIONCITY or par == AddrLevel.REGIONAREA): 
                return True
            if (par == AddrLevel.DISTRICT): 
                return True
        if (ch == AddrLevel.SETTLEMENT): 
            return par == AddrLevel.REGIONCITY or par == AddrLevel.REGIONAREA or par == AddrLevel.DISTRICT
        if (ch == AddrLevel.CITY): 
            return (par == AddrLevel.COUNTRY or par == AddrLevel.REGIONCITY or par == AddrLevel.REGIONAREA) or par == AddrLevel.DISTRICT or par == AddrLevel.SETTLEMENT
        if (ch == AddrLevel.CITYDISTRICT): 
            return par == AddrLevel.CITY
        if (ch == AddrLevel.LOCALITY): 
            if ((par == AddrLevel.DISTRICT or par == AddrLevel.SETTLEMENT or par == AddrLevel.CITY) or par == AddrLevel.REGIONCITY): 
                return True
            if (par == AddrLevel.CITYDISTRICT): 
                return True
            if (par == AddrLevel.LOCALITY): 
                return True
            if (par == AddrLevel.REGIONAREA): 
                return True
            return False
        if (ch == AddrLevel.TERRITORY): 
            if (par == AddrLevel.REGIONCITY): 
                return True
            if ((par == AddrLevel.LOCALITY or par == AddrLevel.CITY or par == AddrLevel.DISTRICT) or par == AddrLevel.CITYDISTRICT or par == AddrLevel.SETTLEMENT): 
                return True
            if (par == AddrLevel.TERRITORY): 
                return True
            return False
        if (ch == AddrLevel.STREET): 
            if ((par == AddrLevel.REGIONCITY or par == AddrLevel.LOCALITY or par == AddrLevel.CITY) or par == AddrLevel.TERRITORY or par == AddrLevel.CITYDISTRICT): 
                return True
            if (par == AddrLevel.DISTRICT): 
                return True
            return False
        if (ch == AddrLevel.BUILDING or ch == AddrLevel.PLOT): 
            if (par == AddrLevel.LOCALITY or par == AddrLevel.TERRITORY or par == AddrLevel.STREET): 
                return True
            if (par == AddrLevel.CITY and ch == AddrLevel.BUILDING): 
                return True
            if (par == AddrLevel.PLOT and ch == AddrLevel.BUILDING): 
                return True
            return False
        if (ch == AddrLevel.APARTMENT): 
            if (par == AddrLevel.BUILDING): 
                return True
            return False
        if (ch == AddrLevel.ROOM): 
            return par == AddrLevel.APARTMENT or par == AddrLevel.BUILDING
        return False
    
    @staticmethod
    def get_house_type_string(ty : 'HouseType', short_val : bool) -> str:
        """ Получить описание для типа дома
        
        Args:
            ty(HouseType): тип
            short_val(bool): в короткой форме
        
        """
        if (ty == HouseType.ESTATE): 
            return ("влад." if short_val else "владение")
        if (ty == HouseType.HOUSEESTATE): 
            return ("дмвлд." if short_val else "домовладение")
        if (ty == HouseType.HOUSE): 
            return ("д." if short_val else "дом")
        if (ty == HouseType.GARAGE): 
            return ("гар." if short_val else "гараж")
        if (ty == HouseType.SPECIAL): 
            return ("спец.строен." if short_val else "специальное строение")
        if (ty == HouseType.WELL): 
            return ("скваж." if short_val else "скважина")
        if (ty == HouseType.MINE): 
            return "шахта"
        if (ty == HouseType.BOILER): 
            return ("котел." if short_val else "котельная")
        if (ty == HouseType.UNFINISHED): 
            return "ОНС"
        return "?"
    
    @staticmethod
    def get_stroen_type_string(ty : 'StroenType', short_val : bool) -> str:
        """ Получить описание для типа строения
        
        Args:
            ty(StroenType): тип
            short_val(bool): в короткой форме
        
        """
        if (ty == StroenType.CONSTRUCTION): 
            return ("сооруж." if short_val else "сооружение")
        if (ty == StroenType.LITER): 
            return ("лит." if short_val else "литера")
        return ("стр." if short_val else "строение")
    
    @staticmethod
    def get_room_type_string(ty : 'RoomType', short_val : bool) -> str:
        """ Получить описание для типа помещения
        
        Args:
            ty(RoomType): тип
            short_val(bool): в короткой форме
        
        """
        if (ty == RoomType.FLAT): 
            return ("кв." if short_val else "квартира")
        if (ty == RoomType.OFFICE): 
            return ("оф." if short_val else "офис")
        if (ty == RoomType.ROOM): 
            return ("комн." if short_val else "комната")
        if (ty == RoomType.SPACE or ty == RoomType.UNDEFINED): 
            return ("помещ." if short_val else "помещение")
        if (ty == RoomType.GARAGE): 
            return ("гар." if short_val else "гараж")
        if (ty == RoomType.CARPLACE): 
            return ("маш.м." if short_val else "машиноместо")
        if (ty == RoomType.PAVILION): 
            return ("пав." if short_val else "павильон")
        return "?"
    
    @staticmethod
    def is_spec_type_direction(typ : 'DetailType') -> bool:
        """ Проверка, что спецтип является направлением
        
        Args:
            typ(DetailType): 
        
        """
        if ((typ == DetailType.NORTH or typ == DetailType.EAST or typ == DetailType.WEST) or typ == DetailType.SOUTH): 
            return True
        if ((typ == DetailType.NORTHEAST or typ == DetailType.NORTHWEST or typ == DetailType.SOUTHEAST) or typ == DetailType.SOUTHWEST): 
            return True
        return False
    
    @staticmethod
    def get_detail_type_string(typ : 'DetailType') -> str:
        """ Получить описание для типа дополнительного параметра
        
        Args:
            typ(DetailType): тип
        
        Returns:
            str: строковое описание
        """
        if (typ == DetailType.NEAR): 
            return "вблизи"
        if (typ == DetailType.CENTRAL): 
            return "центр"
        if (typ == DetailType.LEFT): 
            return "левее"
        if (typ == DetailType.RIGHT): 
            return "правее"
        if (typ == DetailType.NORTH): 
            return "на север"
        if (typ == DetailType.WEST): 
            return "на запад"
        if (typ == DetailType.SOUTH): 
            return "на юг"
        if (typ == DetailType.EAST): 
            return "на восток"
        if (typ == DetailType.NORTHEAST): 
            return "на северо-восток"
        if (typ == DetailType.NORTHWEST): 
            return "на северо-запад"
        if (typ == DetailType.SOUTHEAST): 
            return "на юго-восток"
        if (typ == DetailType.SOUTHWEST): 
            return "на юго-запад"
        if (typ == DetailType.KMRANGE): 
            return "диапазон"
        return Utils.enumToString(typ)
    
    @staticmethod
    def get_detail_part_param_string(typ : 'DetailType') -> str:
        if (typ == DetailType.CENTRAL): 
            return "центральная часть"
        if (typ == DetailType.NORTH): 
            return "северная часть"
        if (typ == DetailType.WEST): 
            return "западная часть"
        if (typ == DetailType.SOUTH): 
            return "южная часть"
        if (typ == DetailType.EAST): 
            return "восточная часть"
        if (typ == DetailType.NORTHEAST): 
            return "северо-восточная часть"
        if (typ == DetailType.NORTHWEST): 
            return "северо-западная часть"
        if (typ == DetailType.SOUTHEAST): 
            return "юго-восточная часть"
        if (typ == DetailType.SOUTHWEST): 
            return "юго-западная часть"
        if (typ == DetailType.LEFT): 
            return "левая часть"
        if (typ == DetailType.RIGHT): 
            return "правая часть"
        return Utils.enumToString(typ)
    
    @staticmethod
    def is_detail_param_direction(typ : 'DetailType') -> bool:
        """ Проверка, является ли тип доп.параметра направлением (на сервер, на юг и т.д.)
        
        Args:
            typ(DetailType): 
        
        """
        if ((((((typ == DetailType.NEAR or typ == DetailType.CENTRAL or typ == DetailType.NORTH) or typ == DetailType.WEST or typ == DetailType.SOUTH) or typ == DetailType.EAST or typ == DetailType.NORTHEAST) or typ == DetailType.NORTHWEST or typ == DetailType.SOUTHEAST) or typ == DetailType.SOUTHWEST or typ == DetailType.LEFT) or typ == DetailType.RIGHT): 
            return True
        return False
    
    @staticmethod
    def get_param_type_string(typ : 'ParamType') -> str:
        """ Получить описание для типа дополнительного параметра
        
        Args:
            typ(ParamType): тип
        
        Returns:
            str: строковое описание
        """
        if (typ == ParamType.ORDER): 
            return "очередь"
        if (typ == ParamType.PART): 
            return "часть"
        if (typ == ParamType.FLOOR): 
            return "этаж"
        if (typ == ParamType.DELIVERYAREA): 
            return "доставочный участок"
        if (typ == ParamType.ZIP): 
            return "индекс"
        if (typ == ParamType.SUBSCRIBERBOX): 
            return "а/я"
        if (typ == ParamType.ORGANIZATION): 
            return "организация"
        if (typ == ParamType.METRO): 
            return "метро"
        return Utils.enumToString(typ)
    
    @staticmethod
    def create_gar_object_official_url(obj : 'GarObject') -> str:
        """ Создать url-ссылку на объект на сайте nalog.ru
        
        Args:
            obj(GarObject): ГАР-объект
        
        Returns:
            str: url-ссылка на сайт с карточкой объекта или null при ошибке
        """
        if (obj is None): 
            return None
        obj_id = obj.get_param_value(GarParam.OBJECTID)
        if (obj_id is None): 
            return None
        return "https://fias.nalog.ru/Search/IndexWithPath?objectId={0}&addressType=1".format(obj_id)
    
    @staticmethod
    def correct_xml_value(txt : str) -> str:
        if (txt is None): 
            return ""
        corr = False
        i = 0
        while i < len(txt): 
            cod = ord(txt[i])
            if (((cod < 0x20) and cod != 0xD and cod != 0xA) and cod != 9): 
                corr = True
                break
            elif (cod >= 0xD800 and cod <= 0xDBFF): 
                if ((i + 1) >= len(txt)): 
                    corr = True
                    break
                i += 1
                cod = (ord(txt[i]))
                if ((cod < 0xDC00) or cod > 0xDFFF): 
                    corr = True
                    break
            elif (cod >= 0xDC00 and cod <= 0xDFFF): 
                corr = True
                break
            i += 1
        if (not corr): 
            return txt
        tmp = Utils.newStringIO(txt)
        i = 0
        while i < tmp.tell(): 
            ch = Utils.getCharAtStringIO(tmp, i)
            if ((((ord(ch)) < 0x20) and ch != '\r' and ch != '\n') and ch != '\t'): 
                Utils.setCharAtStringIO(tmp, i, ' ')
            elif ((ord(ch)) >= 0xD800 and (ord(ch)) <= 0xDBFF): 
                if ((i + 1) >= tmp.tell()): 
                    Utils.setCharAtStringIO(tmp, i, ' ')
                    break
                ch1 = Utils.getCharAtStringIO(tmp, i + 1)
                if ((ord(ch1)) >= 0xDC00 and (ord(ch1)) <= 0xDFFF): 
                    i += 1
                else: 
                    Utils.setCharAtStringIO(tmp, i, ' ')
            elif ((ord(ch)) >= 0xDC00 and (ord(ch)) <= 0xDFFF): 
                Utils.setCharAtStringIO(tmp, i, '?')
            i += 1
        return Utils.toStringStringIO(tmp)
    
    @staticmethod
    def convert_first_char_upper_and_other_lower(str0_ : str) -> str:
        if (Utils.isNullOrEmpty(str0_)): 
            return str0_
        tmp = io.StringIO()
        print(str0_.lower(), end="", file=tmp)
        i = 0
        up = True
        Utils.replaceStringIO(tmp, " .", ".")
        i = 0
        while i < tmp.tell(): 
            if (str.isalpha(Utils.getCharAtStringIO(tmp, i))): 
                if (up): 
                    if (((i + 1) >= tmp.tell() or str.isalpha(Utils.getCharAtStringIO(tmp, i + 1)) or ((Utils.getCharAtStringIO(tmp, i + 1) == '.' or Utils.getCharAtStringIO(tmp, i + 1) == '-'))) or i == 0): 
                        Utils.setCharAtStringIO(tmp, i, str.upper(Utils.getCharAtStringIO(tmp, i)))
                up = False
            elif (not str.isdigit(Utils.getCharAtStringIO(tmp, i))): 
                up = True
            i += 1
        Utils.replaceStringIO(tmp, " - ", "-")
        return Utils.toStringStringIO(tmp)