# SDK Pullenti Address (client version), version 4.28, february 2025.
# Copyright (c) 2013-2025, Pullenti. All rights reserved.
# Non-Commercial Freeware and Commercial Software.
# This class is generated using the converter Unisharping (www.unisharping.ru) from Pullenti C# project.
# The latest version of the code is available on the site www.pullenti.ru

import math
from pullenti.unisharp.Utils import Utils
from pullenti.unisharp.Misc import RefOutArgWrapper

from pullenti.address.GarParam import GarParam

class GpsObject:
    """ Объект GPS (прямоугольник или точка) """
    
    def __str__(self) -> str:
        slat = (GpsObject.out_double(self.min_lat) if self.min_lat == self.max_lat else "{0}-{1}".format(GpsObject.out_double(self.min_lat), GpsObject.out_double(self.max_lat)))
        slon = (GpsObject.out_double(self.min_lon) if self.min_lon == self.max_lon else "{0}-{1}".format(GpsObject.out_double(self.min_lon), GpsObject.out_double(self.max_lon)))
        return "{0} {1}".format(slat, slon)
    
    @staticmethod
    def try_parse_double(str0_ : str, res : float) -> bool:
        res.value = (0)
        if (Utils.isNullOrEmpty(str0_)): 
            return False
        inoutres16 = Utils.tryParseFloat(str0_, res)
        if (inoutres16): 
            return True
        inoutres15 = Utils.tryParseFloat(str0_.replace(',', '.'), res)
        if (str0_.find(',') >= 0 and inoutres15): 
            return True
        inoutres14 = Utils.tryParseFloat(str0_.replace('.', ','), res)
        if (str0_.find('.') >= 0 and inoutres14): 
            return True
        return False
    
    @staticmethod
    def out_double(val : float) -> str:
        return str(val).replace(',', '.')
    
    def clone(self) -> 'GpsObject':
        res = GpsObject()
        res.min_lat = self.min_lat
        res.max_lat = self.max_lat
        res.min_lon = self.min_lon
        res.max_lon = self.max_lon
        return res
    
    def __init__(self, val : str=None) -> None:
        """ Конструктор
        
        Args:
            val(str): формат строки: 'minLat-maxLat minLon-maxLon' (для прямоугольника) или 'lat lon' (для точки)
        """
        self.min_lat = 0
        self.max_lat = 0
        self.min_lon = 0
        self.max_lon = 0
        self.tag = None;
        if (val is None): 
            return
        i = val.find(' ')
        if (i < 0): 
            return
        val1 = val[0:0+i]
        val2 = val[i + 1:]
        d = 0
        i = val1.find('-')
        if (((i)) > 0): 
            wrapd19 = RefOutArgWrapper(0)
            inoutres20 = GpsObject.try_parse_double(val1[0:0+i], wrapd19)
            d = wrapd19.value
            if (inoutres20): 
                self.min_lat = d
            wrapd17 = RefOutArgWrapper(0)
            inoutres18 = GpsObject.try_parse_double(val1[i + 1:], wrapd17)
            d = wrapd17.value
            if (inoutres18): 
                self.max_lat = d
        else: 
            wrapd21 = RefOutArgWrapper(0)
            inoutres22 = GpsObject.try_parse_double(val1, wrapd21)
            d = wrapd21.value
            if (inoutres22): 
                self.max_lat = d
                self.min_lat = self.max_lat
        i = val2.find('-')
        if (((i)) > 0): 
            wrapd25 = RefOutArgWrapper(0)
            inoutres26 = GpsObject.try_parse_double(val2[0:0+i], wrapd25)
            d = wrapd25.value
            if (inoutres26): 
                self.min_lon = d
            wrapd23 = RefOutArgWrapper(0)
            inoutres24 = GpsObject.try_parse_double(val2[i + 1:], wrapd23)
            d = wrapd23.value
            if (inoutres24): 
                self.max_lon = d
        else: 
            wrapd27 = RefOutArgWrapper(0)
            inoutres28 = GpsObject.try_parse_double(val2, wrapd27)
            d = wrapd27.value
            if (inoutres28): 
                self.max_lon = d
                self.min_lon = self.max_lon
    
    @staticmethod
    def create_point_from_gar_object(gobj : 'GarObject') -> 'GpsObject':
        """ Создать объект из параметров ГАР-объекта
        
        Args:
            gobj(GarObject): ГАР-объект
        
        Returns:
            GpsObject: результат или null, если нет информации
        """
        if (gobj is None): 
            return None
        res = None
        s = None
        s = gobj.get_param_value(GarParam.GPSPOINT)
        if ((s) is not None): 
            return GpsObject(s)
        s = gobj.get_param_value(GarParam.GPSRECTANGLE)
        if ((s) is not None): 
            res = GpsObject(s)
            d = ((res.min_lat + res.max_lat)) / (2)
            res.max_lat = d
            res.min_lat = res.max_lat
            d = (((res.min_lon + res.max_lon)) / (2))
            res.max_lon = d
            res.min_lon = res.max_lon
        return res
    
    def calc_lat_km(self) -> float:
        """ Вычислить широту в км
        
        """
        return ((self.max_lat - self.min_lat)) * 111.13
    
    def calc_lon_km(self) -> float:
        """ Вычислить долготу в км
        
        """
        if (self.min_lon == self.max_lon): 
            return 0
        rad = (math.pi * ((self.max_lat + self.min_lat))) / ((2 * 180))
        return math.cos(rad) * ((self.max_lon - self.min_lon)) * 111.32
    
    def calc_square(self) -> float:
        """ Вычислить площадь в кв.км
        
        """
        sq = self.calc_lat_km() * self.calc_lon_km()
        if (sq > 100): 
            sq = round(sq, 0)
        elif (sq > 10): 
            sq = round(sq, 1)
        elif (sq > 1): 
            sq = round(sq, 2)
        elif (sq > 0.1): 
            sq = round(sq, 3)
        else: 
            sq = round(sq, 4)
        return sq
    
    def calc_dist(self, go : 'GpsObject') -> float:
        """ Вычислить расстояние в км между объектами.
        Расстояние равно 0, если есть пересечение между объектами.
        Иначе расстояние между ближайшими друг к другу точками
        
        Args:
            go(GpsObject): другой объект
        
        Returns:
            float: расстояние в км или 0 при пересечении
        """
        lat1 = 0
        lat2 = 0
        if (self.max_lat <= go.min_lat): 
            lat1 = self.max_lat
            lat2 = go.min_lat
        elif (go.max_lat <= self.min_lat): 
            lat1 = go.max_lat
            lat2 = self.min_lat
        elif (self.max_lat <= go.max_lat): 
            lat2 = self.max_lat
            lat1 = lat2
        elif (go.max_lat <= self.max_lat): 
            lat2 = go.max_lat
            lat1 = lat2
        else: 
            pass
        lon1 = 0
        lon2 = 0
        if (self.max_lon <= go.min_lon): 
            lon1 = self.max_lon
            lon2 = go.min_lon
        elif (go.max_lon <= self.min_lon): 
            lon1 = go.max_lon
            lon2 = self.min_lon
        elif (self.max_lon <= go.max_lon): 
            lon2 = self.max_lon
            lon1 = lon2
        elif (go.max_lon <= self.max_lon): 
            lon2 = go.max_lon
            lon1 = lon2
        else: 
            pass
        x = ((lat1 - lat2)) * 111.13
        x = (x * x)
        rad = (math.pi * ((lat1 + lat2))) / ((2 * 180))
        y = math.cos(rad) * ((lon1 - lon2)) * 111.32
        y = (y * y)
        return math.sqrt(x + y)
    
    def add_object(self, r : 'GpsObject') -> bool:
        """ Добавить другой объект, расширив границы текущего при необходимости
        
        Args:
            r(GpsObject): добавляемый объект
        
        Returns:
            bool: true, если границы расширились
        """
        ret = False
        if (r.min_lat < self.min_lat): 
            self.min_lat = r.min_lat
            ret = True
        if (r.max_lat > self.max_lat): 
            self.max_lat = r.max_lat
            ret = True
        if (r.min_lon < self.min_lon): 
            self.min_lon = r.min_lon
            ret = True
        if (r.max_lon > self.max_lon): 
            self.max_lon = r.max_lon
            ret = True
        return ret