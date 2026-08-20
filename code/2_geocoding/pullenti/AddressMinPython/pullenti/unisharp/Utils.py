import io
import math
import pkg_resources
from pathlib import Path
from pathlib import PurePath
import datetime
import time
import xml.etree.ElementTree
import uuid
from pullenti.unisharp.Streams import Stream

class Utils:    
    @staticmethod
    def asObjectOrNull(obj, clas):
        if(obj == None): return None
        if(isinstance(obj, clas)): return obj
        return None

    @staticmethod
    def enumToString(e):
        s = str(e)
        i = s.find('.')
        if(i > 0):
            s = s[i + 1:]
        return s.upper()

    @staticmethod
    def valToEnum(v, e):
        try:
            if(isinstance(v, e)):
                return v
            if(isinstance(v, str)):
                return e.__dict__[v.upper()]
            return e(v)
        except:
            if(isinstance(v, str)):
                return e(int(v))
            return v

    @staticmethod
    def newException(msg, ie):
        e = Exception()
        e.message = msg
        if(ie is not None): e.inner_exception = ie
        return e

    @staticmethod
    def newArray(cou, ini):
        res = list()
        for i in range(cou):
            res.append(ini)
        return res

    @staticmethod
    def copyArray(src, sind, tgt, tind, cou):
        for i in range(cou):
            tgt[i + tind] = src[i + sind]

    @staticmethod
    def newArrayOfBytes(cou, ini):
        res = bytearray()
        for i in range(cou):
            res.append(ini)
        return res

    @staticmethod
    def indexOfList(li, it, i):
        for ii in range(i, len(li)):
            if(li[ii] == it): return ii
        return -1
    @staticmethod
    def indexOfAny(li, sany, i, le):
        if(le <= 0):
            le = len(li)
        for ii in range(i, le):
            if(Utils.indexOfList(sany, li[ii], 0) >= 0): return ii
        return -1
    @staticmethod
    def lastIndexOfList(li, it, i):
        if(i < 0): i = len(li) - 1
        for ii in range(i, -1, -1):
            if(li[ii] == it): return ii
        return -1

    @staticmethod
    def tryGetValue(d, key, res):
        if(d is None):
            return False
        if(key in d): 
            res.value = d[key]
            return True
        return False

    @staticmethod
    def tryGetValue0(d, key, res):
        res.value = 0
        if(d is None):
            return False
        if(key in d): 
            res.value = d[key]
            return True
        return False

    @staticmethod
    def tryParseInt(s, res):
        res.value = 0
        if(s is None): return False
        try: 
            res.value = int(s)
            return True
        except:
            return False

    @staticmethod
    def tryParseFloat(s, res):
        res.value = 0
        if(s is None): return False
        try: 
            res.value = float(s)
            return True
        except:
            return False

    @staticmethod
    def mathTruncate(f):
        if(f < 0): return math.ceil(f)
        return math.floor(f)

    @staticmethod
    def getHashtabVal(dic, ind):
        if(ind in dic): return dic[ind]
        return None
    @staticmethod
    def setHashtabVal(dic, ind, val):
        dic[ind] = val

    @staticmethod
    def ifNotNull(v, altv):
        if(v is None): return altv
        return v

    @staticmethod
    def isNullOrEmpty(v):
        if(v is None): return True
        try:
            if(len(v) == 0): return True
        except:
            pass
        return False

    @staticmethod
    def isNullOrWhiteSpace(v):
        if(v is None): return True
        try:
            if(len(v) == 0): return True
            for ss in v: 
                if(not Utils.isWhitespace(ss)): return False
        except:
            pass
        return True

    wsChars = [0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x20, 0x85, 0xA0, 0x1680, 0x2000, 0x2001, 0x2002, 0x2003, 0x2004, 0x2005, 0x2006, 0x2007, 0x2008, 0x2009, 0x200A, 0x2028, 0x2029, 0x202F, 0x205F, 0x3000]
   
    @staticmethod
    def isWhitespace(ch):
        co = ord(ch[0])
        return co in Utils.wsChars

    ptChars = [0x21, 0x22, 0x23, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A, 0x2C, 0x2D, 0x2E, 0x2F, 0x3A, 0x3B, 0x3F, 0x40, 0x5B, 0x5C, 0x5D, 0x5F, 0x7B, 0x7D, 0xA1, 0xAB, 0xAD, 0xB7, 0xBB, 0xBF, 0x37E, 0x387, 0x55A, 0x55B, 0x55C, 0x55D, 0x55E, 0x55F, 0x589, 0x58A, 0x5BE, 0x5C0, 0x5C3, 0x5C6, 0x5F3, 0x5F4, 0x609, 0x60A, 0x60C, 0x60D, 0x61B, 0x61E, 0x61F, 0x66A, 0x66B, 0x66C, 0x66D, 0x6D4, 0x700, 0x701, 0x702, 0x703, 0x704, 0x705, 0x706, 0x707, 0x708, 0x709, 0x70A, 0x70B, 0x70C, 0x70D, 0x7F7, 0x7F8, 0x7F9, 0x830, 0x831, 0x832, 0x833, 0x834, 0x835, 0x836, 0x837, 0x838, 0x839, 0x83A, 0x83B, 0x83C, 0x83D, 0x83E, 0x85E, 0x964, 0x965, 0x970, 0xAF0, 0xDF4, 0xE4F, 0xE5A, 0xE5B, 0xF04, 0xF05, 0xF06, 0xF07, 0xF08, 0xF09, 0xF0A, 0xF0B, 0xF0C, 0xF0D, 0xF0E, 0xF0F, 0xF10, 0xF11, 0xF12, 0xF14, 0xF3A, 0xF3B, 0xF3C, 0xF3D, 0xF85, 0xFD0, 0xFD1, 0xFD2, 0xFD3, 0xFD4, 0xFD9, 0xFDA, 0x104A, 0x104B, 0x104C, 0x104D, 0x104E, 0x104F, 0x10FB, 0x1360, 0x1361, 0x1362, 0x1363, 0x1364, 0x1365, 0x1366, 0x1367, 0x1368, 0x1400, 0x166D, 0x166E, 0x169B, 0x169C, 0x16EB, 0x16EC, 0x16ED, 0x1735, 0x1736, 0x17D4, 0x17D5, 0x17D6, 0x17D8, 0x17D9, 0x17DA, 0x1800, 0x1801, 0x1802, 0x1803, 0x1804, 0x1805, 0x1806, 0x1807, 0x1808, 0x1809, 0x180A, 0x1944, 0x1945, 0x1A1E, 0x1A1F, 0x1AA0, 0x1AA1, 0x1AA2, 0x1AA3, 0x1AA4, 0x1AA5, 0x1AA6, 0x1AA8, 0x1AA9, 0x1AAA, 0x1AAB, 0x1AAC, 0x1AAD, 0x1B5A, 0x1B5B, 0x1B5C, 0x1B5D, 0x1B5E, 0x1B5F, 0x1B60, 0x1BFC, 0x1BFD, 0x1BFE, 0x1BFF, 0x1C3B, 0x1C3C, 0x1C3D, 0x1C3E, 0x1C3F, 0x1C7E, 0x1C7F, 0x1CC0, 0x1CC1, 0x1CC2, 0x1CC3, 0x1CC4, 0x1CC5, 0x1CC6, 0x1CC7, 0x1CD3, 0x2010, 0x2011, 0x2012, 0x2013, 0x2014, 0x2015, 0x2016, 0x2017, 0x2018, 0x2019, 0x201A, 0x201B, 0x201C, 0x201D, 0x201E, 0x201F, 0x2020, 0x2021, 0x2022, 0x2023, 0x2024, 0x2025, 0x2026, 0x2027, 0x2030, 0x2031, 0x2032, 0x2033, 0x2034, 0x2035, 0x2036, 0x2037, 0x2038, 0x2039, 0x203A, 0x203B, 0x203C, 0x203D, 0x203E, 0x203F, 0x2040, 0x2041, 0x2042, 0x2043, 0x2045, 0x2046, 0x2047, 0x2048, 0x2049, 0x204A, 0x204B, 0x204C, 0x204D, 0x204E, 0x204F, 0x2050, 0x2051, 0x2053, 0x2054, 0x2055, 0x2056, 0x2057, 0x2058, 0x2059, 0x205A, 0x205B, 0x205C, 0x205D, 0x205E, 0x207D, 0x207E, 0x208D, 0x208E, 0x2308, 0x2309, 0x230A, 0x230B, 0x2329, 0x232A, 0x2768, 0x2769, 0x276A, 0x276B, 0x276C, 0x276D, 0x276E, 0x276F, 0x2770, 0x2771, 0x2772, 0x2773, 0x2774, 0x2775, 0x27C5, 0x27C6, 0x27E6, 0x27E7, 0x27E8, 0x27E9, 0x27EA, 0x27EB, 0x27EC, 0x27ED, 0x27EE, 0x27EF, 0x2983, 0x2984, 0x2985, 0x2986, 0x2987, 0x2988, 0x2989, 0x298A, 0x298B, 0x298C, 0x298D, 0x298E, 0x298F, 0x2990, 0x2991, 0x2992, 0x2993, 0x2994, 0x2995, 0x2996, 0x2997, 0x2998, 0x29D8, 0x29D9, 0x29DA, 0x29DB, 0x29FC, 0x29FD, 0x2CF9, 0x2CFA, 0x2CFB, 0x2CFC, 0x2CFE, 0x2CFF, 0x2D70, 0x2E00, 0x2E01, 0x2E02, 0x2E03, 0x2E04, 0x2E05, 0x2E06, 0x2E07, 0x2E08, 0x2E09, 0x2E0A, 0x2E0B, 0x2E0C, 0x2E0D, 0x2E0E, 0x2E0F, 0x2E10, 0x2E11, 0x2E12, 0x2E13, 0x2E14, 0x2E15, 0x2E16, 0x2E17, 0x2E18, 0x2E19, 0x2E1A, 0x2E1B, 0x2E1C, 0x2E1D, 0x2E1E, 0x2E1F, 0x2E20, 0x2E21, 0x2E22, 0x2E23, 0x2E24, 0x2E25, 0x2E26, 0x2E27, 0x2E28, 0x2E29, 0x2E2A, 0x2E2B, 0x2E2C, 0x2E2D, 0x2E2E, 0x2E30, 0x2E31, 0x2E32, 0x2E33, 0x2E34, 0x2E35, 0x2E36, 0x2E37, 0x2E38, 0x2E39, 0x2E3A, 0x2E3B, 0x2E3C, 0x2E3D, 0x2E3E, 0x2E3F, 0x2E40, 0x2E41, 0x2E42, 0x3001, 0x3002, 0x3003, 0x3008, 0x3009, 0x300A, 0x300B, 0x300C, 0x300D, 0x300E, 0x300F, 0x3010, 0x3011, 0x3014, 0x3015, 0x3016, 0x3017, 0x3018, 0x3019, 0x301A, 0x301B, 0x301C, 0x301D, 0x301E, 0x301F, 0x3030, 0x303D, 0x30A0, 0x30FB, 0xA4FE, 0xA4FF, 0xA60D, 0xA60E, 0xA60F, 0xA673, 0xA67E, 0xA6F2, 0xA6F3, 0xA6F4, 0xA6F5, 0xA6F6, 0xA6F7, 0xA874, 0xA875, 0xA876, 0xA877, 0xA8CE, 0xA8CF, 0xA8F8, 0xA8F9, 0xA8FA, 0xA8FC, 0xA92E, 0xA92F, 0xA95F, 0xA9C1, 0xA9C2, 0xA9C3, 0xA9C4, 0xA9C5, 0xA9C6, 0xA9C7, 0xA9C8, 0xA9C9, 0xA9CA, 0xA9CB, 0xA9CC, 0xA9CD, 0xA9DE, 0xA9DF, 0xAA5C, 0xAA5D, 0xAA5E, 0xAA5F, 0xAADE, 0xAADF, 0xAAF0, 0xAAF1, 0xABEB, 0xFD3E, 0xFD3F, 0xFE10, 0xFE11, 0xFE12, 0xFE13, 0xFE14, 0xFE15, 0xFE16, 0xFE17, 0xFE18, 0xFE19, 0xFE30, 0xFE31, 0xFE32, 0xFE33, 0xFE34, 0xFE35, 0xFE36, 0xFE37, 0xFE38, 0xFE39, 0xFE3A, 0xFE3B, 0xFE3C, 0xFE3D, 0xFE3E, 0xFE3F, 0xFE40, 0xFE41, 0xFE42, 0xFE43, 0xFE44, 0xFE45, 0xFE46, 0xFE47, 0xFE48, 0xFE49, 0xFE4A, 0xFE4B, 0xFE4C, 0xFE4D, 0xFE4E, 0xFE4F, 0xFE50, 0xFE51, 0xFE52, 0xFE54, 0xFE55, 0xFE56, 0xFE57, 0xFE58, 0xFE59, 0xFE5A, 0xFE5B, 0xFE5C, 0xFE5D, 0xFE5E, 0xFE5F, 0xFE60, 0xFE61, 0xFE63, 0xFE68, 0xFE6A, 0xFE6B, 0xFF01, 0xFF02, 0xFF03, 0xFF05, 0xFF06, 0xFF07, 0xFF08, 0xFF09, 0xFF0A, 0xFF0C, 0xFF0D, 0xFF0E, 0xFF0F, 0xFF1A, 0xFF1B, 0xFF1F, 0xFF20, 0xFF3B, 0xFF3C, 0xFF3D, 0xFF3F, 0xFF5B, 0xFF5D, 0xFF5F, 0xFF60, 0xFF61, 0xFF62, 0xFF63, 0xFF64, 0xFF65]
   
    @staticmethod
    def isPunctuation(ch):
        co = ord(ch[0])
        return co in Utils.ptChars

    @staticmethod
    def getDate(dt):
        return datetime.datetime(dt.year, dt.month, dt.day)

    @staticmethod
    def getDateTimeFromCtime(cti):
        return datetime.datetime.strptime(time.ctime(cti), "%a %b %d %H:%M:%S %Y")

    @staticmethod
    def getDateShortString(dt):
        return str(datetime.date(dt.year, dt.month, dt.day))

    @staticmethod
    def getTimeShortString(dt):
        return str(datetime.time(dt.hour, dt.minute, dt.second))

    @staticmethod
    def lastDayOfMonth(year, month):
        any_day = datetime.date(year, month, 1) 
        next_month = any_day.replace(day=28) + datetime.timedelta(days=4) 
        dat = next_month - datetime.timedelta(days=next_month.day)
        return dat.day

    @staticmethod
    def compareStrings(s1, s2, ignoreCas = False):
        if(ignoreCas):
            s1 = s1.upper()
            s2 = s2.upper()
        if(s1 == s2): return 0
        if(s1 < s2): return -1
        return 1

    @staticmethod
    def joinStrings(d, s):
        res = ""
        for ss in s:
            if(len(res) > 0): res += d
            res += ss
        return res

    @staticmethod
    def startsWithString(s, sub, ignoreCase = False):
        if(not ignoreCase): return s.startswith(sub)
        return s.upper().startswith(sub.upper())

    @staticmethod
    def endsWithString(s, sub, ignoreCase = False):
        if(not ignoreCase): return s.endswith(sub)
        return s.upper().endswith(sub.upper())

    @staticmethod
    def splitString(s, sep, ignoreEmpty = False):
        if(s is None or sep is None): return None
        res = []
        i0 = 0
        for i in range(len(s)):
            ch = s[i]
            if(isinstance(sep, list)):
                if(not (ch in sep)): continue
            else:
                if(sep.find(ch) < 0): continue
                
            if(i > i0): res.append(s[i0: i])
            else: 
                if((not ignoreEmpty) and i > 0): res.append("")
            i0 = i + 1
        else: i = len(s)
        if(i > i0):
            res.append(s[i0 : i])
        return res

    @staticmethod
    def trimEndString(s):
        for i in range(len(s) - 1, -1, -1):
            if(not s[i].isspace()):
                if(i < len(s) - 1): return s[:i + 1]
                return s
        return ""
    
    @staticmethod
    def trimStartString(s):
        for i in range(len(s)):
            if(not s[i].isspace()):
                if(i > 0): return s[i:]
                return s
        return ""

    @staticmethod
    def getCharAtStringIO(s, i):
        p = s.tell()
        s.seek(i, io.SEEK_SET)
        ret = s.read(1)
        s.seek(p, io.SEEK_SET)
        return ret

    @staticmethod
    def setCharAtStringIO(s, i, ch):
        p = s.tell()
        s.seek(i, io.SEEK_SET)
        s.write(ch)
        s.seek(p, io.SEEK_SET)
        return ch

    @staticmethod
    def setLengthStringIO(s, i):
        s.truncate(i)
        s.seek(0, io.SEEK_END)

    @staticmethod
    def getLengthIO(s):
        p = s.tell()
        s.seek(0, io.SEEK_END)
        le = s.tell()
        s.seek(p, io.SEEK_SET)
        return le

    @staticmethod
    def readTextIO(s):
        ret = s.read(1)
        if(ret is None): return -1
        if(len(ret) != 1): return -1
        return ord(ret[0])

    @staticmethod
    def readLineIO(s):
        ret = s.readline()
        if(ret is None): return None
        if(len(ret) > 0): 
            if(ret[len(ret) - 1] == '\n'): ret = ret[:len(ret) - 1]
        if(len(ret) > 0): 
            if(ret[len(ret) - 1] == '\r'): ret = ret[:len(ret) - 1]
        if(len(ret) > 0): 
            return ret
        if(Utils.getLengthIO(s) >= s.tell()): return None
        return ''

    @staticmethod
    def readByteIO(s):
        ret = s.read(1)
        if(ret is None): return -1
        if(len(ret) != 1): return -1
        return ret[0]

    @staticmethod
    def writeByteIO(s, b):
        arr = bytearray()
        arr.append(b)
        s.write(arr)

    @staticmethod
    def readIO(s, buf, i, le):
        if(i == 0 and len(buf) == le):
            return s.readinto(buf)
        ret = s.read(le)
        if(ret is None): return -1
        le = len(ret)
        if(le == 0): return 0
        for j in range(le):
            buf[i + j] = ret[j]
        return le
    
    @staticmethod
    def writeIO(s, buf, i, le):
        if(i == 0 and len(buf) == le):
            return s.write(buf)
        bb = buf[i:i + le]
        return s.write(bb)

    @staticmethod
    def newStringIO(v):
        res = io.StringIO()
        if(v is not None): print(v, end="", file=res)
        return res

    @staticmethod
    def fileReadAllText(f):
        b = Path(f).read_bytes()
        if(b is None): return None
        if(b[0] == 0xEF and b[1] == 0xBB and b[2] == 0xBF): b = b[3:]
        return b.decode('utf-8', 'ignore')

    @staticmethod
    def fileWriteAllText(f, txt):
        b = bytearray()
        b.append(0xEF)
        b.append(0xBB)
        b.append(0xBF)
        b.extend(txt.encode('utf-8', 'ignore'))
        Path(f).write_bytes(b)

    @staticmethod
    def preambleCharset(e):
        b = bytearray()
        b.append(0xEF)
        b.append(0xBB)
        b.append(0xBF)
        return b

    @staticmethod
    def insertStringIO(s, i, ins):
        v = s.getvalue()
        buf = str(ins) + v[i:]
        s.seek(i, io.SEEK_SET)
        s.write(buf)
        s.seek(0, io.SEEK_END)

    @staticmethod
    def removeStringIO(s, i, l):
        v = s.getvalue()
        vl = len(v)
        if(i + l < vl):
            vv = v[i + l:]
            s.seek(i, io.SEEK_SET)
            s.write(vv)
        s.truncate(vl - l)
        s.seek(0, io.SEEK_END)

    @staticmethod
    def replaceStringIO(s, old, new):
        v = s.getvalue().replace(old, new)
        s.truncate(0)
        s.seek(0, io.SEEK_SET)
        s.write(v)
        s.seek(0, io.SEEK_END)

        
    @staticmethod
    def toStringStringIO(s):
        v = s.getvalue()
        if(len(v) > s.tell()):
            return v[0:s.tell()]
        return v

    @staticmethod
    def getFilenameWithoutExt(f):
        ext = PurePath(f).suffix
        if(ext is None or ext == '.'): return f
        return f[: len(f) - len(ext)]
    
    @staticmethod
    def getResourcesNames(di, exts):
        ee = Utils.splitString(exts, ';', True)
        res = []
        for rn in pkg_resources.ResourceManager().resource_listdir(di, ''):
            for e in ee:
                if(PurePath(rn).suffix == e):
                    res.append(rn)
                    break
        return res
    
    @staticmethod
    def getResourceStream(di, name):
        try:
            for rn in pkg_resources.ResourceManager().resource_listdir(di, ''):
                fname = PurePath(rn).name
                if(Utils.endsWithString(name, fname, True)):
                    return Stream(pkg_resources.ResourceManager().resource_stream(di, fname))
        except:
            return None

    @staticmethod
    def getResourceInfo(di, name):
        try:
            for rn in pkg_resources.ResourceManager().resource_listdir(di, ''):
                fname = PurePath(rn).name
                if(Utils.endsWithString(name, fname, True)):
                    return fname
            return None
        except:
            return None

    @staticmethod
    def parseXmlFromString(txt):
        tree = xml.etree.ElementTree.ElementTree()
        root = xml.etree.ElementTree.fromstring(txt)
        tree._root = root
        return tree
    @staticmethod
    def parseXmlFromStream(stream):
        mem = io.BytesIO()
        blen = stream.length
        if blen <= 0: blen = 10000
        buf = bytearray(blen)
        while True:
            i = stream.read(buf, 0, blen)
            if i <= 0: break
            buf1 = buf
            if i < blen: buf1 = buf[0:i]
            mem.write(buf1)
        mem.seek(0, io.SEEK_SET)
        tree = xml.etree.ElementTree.parse(mem)
        mem.close()
        return tree

    @staticmethod
    def getXmlLocalName(e):
        tag = Utils.getXmlName(e)
        i = tag.find(':')
        if i > 0: tag = tag[i + 1:]
        return tag
    @staticmethod
    def getXmlName(e):
        tag = e.tag
        i = tag.find('}')
        if i > 0: tag = tag[i + 1:]
        return tag

    @staticmethod
    def getXmlAttrLocalName(a):
        tag = Utils.getXmlAttrName(a)
        i = tag.find(':')
        if i > 0: tag = tag[i + 1:]
        return tag

    @staticmethod
    def getXmlAttrName(a):
        tag = a[0]
        i = tag.find('}')
        if i > 0: tag = tag[i + 1:]
        return tag

    @staticmethod
    def getXmlAttrByName(attrs, tag):
        try:
            for a in attrs.items():
                if a[0] == tag or Utils.getXmlAttrLocalName(a) == tag or Utils.getXmlAttrName(a) == tag: 
                    return (tag, a[1])
            return None
        except:
            return None

    @staticmethod
    def getXmlAttrByIndex(attrs, i):
        try:
            for a in attrs.items():
                if(i == 0): return a
                i -= 1
            return None
        except:
            return None

    @staticmethod
    def getXmlInnerText(e):
        res = ""
        for t in e.itertext():
            res += t
        if(len(res) == 0): return None
        return res

    EMPTYUUID = uuid.UUID('{00000000-0000-0000-0000-000000000000}')
