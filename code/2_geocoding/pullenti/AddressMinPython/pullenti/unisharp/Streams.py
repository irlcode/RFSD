import io
import shutil

class Stream:
    def __init__(self, ss=None):
        self.s = ss
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.s != None:
            self.s.close()
    
    @property
    def readable(self):
        if self.s != None: return self.s.readable
        return True
    @property
    def writeable(self):
        if self.s != None: return self.s.writable
        return True
    @property
    def seekable(self):
        if self.s != None: return self.s.seekable
        return True
    @property
    def position(self):
        if self.s != None: return self.s.tell()
        return -1
    @position.setter
    def position(self, value):
        if self.s != None:
            self.s.seek(value, io.SEEK_SET)
            return self.s.tell()
        return -1
    @property
    def length(self):
        if self.s == None: return 0 
        p = self.s.tell()
        self.s.seek(0, io.SEEK_END)
        le = self.s.tell()
        self.s.seek(p, io.SEEK_SET)
        return le
    @length.setter
    def length(self, value):
        if self.s != None: self.s.truncate(value)
    
    def flush(self):
        if self.s != None: self.s.flush()
        
    def readbyte(self):
        if self.s == None: 
            arr = bytearray()
            arr.append(0)
            i = self.read(arr, 0, 1)
            if i < 1: return -1
            return arr[0]
        ret = self.s.read(1)
        if(ret is None): return -1
        if(len(ret) != 1): return -1
        return ret[0]
    def read(self, buf, i, le):
        if self.s == None: return -1
        if(i == 0 and len(buf) == le):
            return self.s.readinto(buf)
        ret = self.s.read(le)
        if(ret is None): return -1
        le = len(ret)
        if(le == 0): return 0
        for j in range(le):
            buf[i + j] = ret[j]
        return le
    def writebyte(self, b):
        arr = bytearray()
        arr.append(b)
        if self.s == None:
            self.write(arr, 0, 1)
        else:
            self.s.write(arr)
    def write(self, buf, i, le):
        if self.s == None: return -1
        if(i == 0 and len(buf) == le):
            return self.s.write(buf)
        bb = buf[i:i + le]
        return self.s.write(bb)
    def seek(self, p, t):
        if self.s == None: return -1
        if t == 1: return self.s.seek(p, io.SEEK_CUR)
        if t == 2: return self.s.seek(p, io.SEEK_END)
        return self.s.seek(p, io.SEEK_SET)
    def getstream(self):
        if self.s != None: return self.s
        return None

class MemoryStream(Stream):
    
    def __init__(self, buf=None):
        self.s = io.BytesIO(buf)
    def close(self):
        self.s.close()
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
         
    def toarray(self):
        return bytearray(self.s.getvalue())
    def writeto(self, sto):
        shutil.copyfileobj(self.s, sto.s) #???

class FileStream(Stream):
    
    def __init__(self, fname, m):
        self.s = open(fname, mode=m)
    def close(self):
        self.s.close()
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def name(self):
        return self.s.name 

