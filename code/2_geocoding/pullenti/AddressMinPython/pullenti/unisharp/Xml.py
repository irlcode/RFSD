import io

class XmlWriterSettings:
    def __init__(self): 
        self.encoding = None
        self.indent = False
        self.indentChars = '\r\n'

class XmlWriter:
    
    def __init__(self) -> None:
        self.settings = None
        self.__m_stream = None
        self.__m_str_build = None
        self.__m_file_name = None
        self.__m_nodes = list()
        self.__m_elem_not_ended = False
        self.__m_elem_has_child = False
    
    def __enter__(self): return self
    def __exit__(self, typ, val, traceback): self.close()
    
    
    @staticmethod
    def create_stream(output : io.IOBase, settings_ : XmlWriterSettings=None) -> 'XmlWriter':
        if (settings_ is None): 
            settings_ = XmlWriterSettings()
        res = XmlWriter()
        res.settings = settings_
        res.__m_stream = output
        return res
    
    @staticmethod
    def create_file(output_file_name : str, settings_ : XmlWriterSettings=None) -> 'XmlWriter':
        if (settings_ is None): 
            settings_ = XmlWriterSettings()
        res = XmlWriter()
        res.settings = settings_
        res.__m_stream = (open(output_file_name, mode="r+b"))
        return res
    
    @staticmethod
    def create_string(output : io.StringIO, settings_ : XmlWriterSettings=None) -> 'XmlWriter':
        if (settings_ is None): 
            settings_ = XmlWriterSettings()
        res = XmlWriter()
        res.settings = settings_
        res.__m_str_build = output
        return res
    
    def close(self) -> None:
        if (self.__m_stream is not None): 
            self.__m_stream.close()
            self.__m_stream = None
    
    def flush(self) -> None:
        if (self.__m_stream is not None): 
            self.__m_stream.flush()
    
    def __out(self, str0_ : str) -> None:
        if (str0_ is None): 
            return
        if (self.__m_str_build is not None): 
            print(str0_, end="", file=self.__m_str_build)
        elif (self.__m_stream is not None): 
            if (self.__m_stream.position == 0): 
                arr = bytearray()
                arr.append(0xEF)
                arr.append(0xBB)
                arr.append(0xBF)
                self.__m_stream.write(arr, 0, 3)
            dat = str0_.encode('utf-8', 'ignore')
            self.__m_stream.write(dat, 0, len(dat))
    
    def write_start_document(self) -> None:
        self.__out("<?xml version=\"1.0\" encoding=\"utf-8\"?>")
    
    def write_end_document(self) -> None:
        pass
    
    def write_start_element(self, local_name : str) -> None:
        if (self.__m_elem_not_ended): 
            self.__out(">")
            self.__m_elem_not_ended = False
        self.__m_nodes.append(local_name)
        if (self.settings.indent): 
            self.__out("\r\n")
            if (self.settings.indentChars is not None): 
                i = 0
                while i < (len(self.__m_nodes) - 1): 
                    self.__out(self.settings.indentChars)
                    i += 1
        self.__out("<{0}".format(local_name))
        self.__m_elem_not_ended = True
        self.__m_elem_has_child = False
    
    def write_start_element2(self, local_name : str, ns : str) -> None:
        if(ns is None):
            self.write_start_element(local_name)
        else:
            self.write_start_element(local_name)
            self.write_attribute_string("xmlns", ns)
    
    def write_start_element3(self, prefix : str, local_name : str, ns : str) -> None:
        if(prefix is None):
            self.write_start_element2(local_name, ns)
        elif(ns is None):
            self.write_start_element("{0}:{1}".format(prefix, local_name))
        else:
            self.write_start_element("{0}:{1}".format(prefix, local_name, ns))
            self.write_attribute_string("xmlns:{0}".format(prefix), ns)
    
    def write_end_element(self) -> None:
        if (self.__m_elem_not_ended): 
            self.__out(" />")
            self.__m_elem_not_ended = False
            del self.__m_nodes[len(self.__m_nodes) - 1]
            self.__m_elem_has_child = True
            return
        if (self.settings.indent and self.__m_elem_has_child): 
            self.__out("\r\n")
            if (self.settings.indentChars is not None): 
                i = 0
                while i < (len(self.__m_nodes) - 1): 
                    self.__out(self.settings.indentChars)
                    i += 1
        if (len(self.__m_nodes) > 0): 
            self.__out("</{0}>".format(self.__m_nodes[len(self.__m_nodes) - 1]))
            del self.__m_nodes[len(self.__m_nodes) - 1]
            self.__m_elem_has_child = True
           
    
    def __correct_value(self, val : str, is_attr : bool) -> str:
        tmp = io.StringIO()
        if (val is not None): 
            for ch in val: 
                o = ord(ch)
                if (ch == '<'): 
                    print("&lt;", end="", file=tmp)
                elif (ch == '&'): 
                    print("&amp;", end="", file=tmp)
                elif (ch == '>'): 
                    print("&gt;", end="", file=tmp)
                elif (is_attr and ch == '"'): 
                    print("&quot;", end="", file=tmp)
                elif (is_attr and ch == '\''): 
                    print("&apos;", end="", file=tmp)
                elif (o < 0x20 and o != 0xA and o != 0xD and o != 9): 
                    print(' ', end="", file=tmp)
                else: 
                    print(ch, end="", file=tmp)
        v = tmp.getvalue()
        if(len(v) > tmp.tell()):
            return v[0:tmp.tell()]
        return v
    
    def write_attribute_string(self, local_name : str, value : str) -> None:
        self.__out(" {0}=\"{1}\"".format(local_name, self.__correct_value(value, True)))
    
    def write_attribute_string2(self, local_name : str, ns : str, value : str) -> None:
        if(ns is None):
            self.write_attribute_string(local_name, value)
        else:
            self.write_attribute_string("{0}:{1}".format("p2", local_name), value)
            self.write_attribute_string("xmlns:p2", ns)
    
    def write_attribute_string3(self, prefix : str, local_name : str, ns : str, value : str) -> None:
        if(prefix is None):
            self.write_attribute_string2(local_name, ns, value)
        elif(ns is None):
            self.write_attribute_string("{0}:{1}".format(prefix, local_name), value)
        else:
            self.write_attribute_string("{0}:{1}".format(prefix, local_name), value)
            self.write_attribute_string("{0}:{1}".format("xmlns", prefix), ns)
    
    def write_element_string(self, local_name : str, value : str) -> None:
        self.write_start_element(local_name)
        self.write_string(value)
        self.write_end_element()
    
    def write_element_string2(self, local_name : str, ns : str, value : str) -> None:
        self.write_start_element2(local_name, ns)
        self.write_string(value)
        self.write_end_element()
    
    def write_element_string3(self, prefix : str, local_name : str, ns : str, value : str) -> None:
        self.write_start_element3(prefix, local_name, ns)
        self.write_string(value)
        self.write_end_element()
    
    def write_string(self, text : str) -> None:
        if (self.__m_elem_not_ended): 
            self.__out(">")
            self.__m_elem_not_ended = False
        self.__out(self.__correct_value(text, False))
    
    def write_value(self, value : object) -> None:
        if (value is None): 
            return
        self.write_string(str(value))

    def write_comment(self, text : str) -> None:
        if (self.__m_elem_not_ended): 
            self.__out(">")
            self.__m_elem_not_ended = False
        self.__out("<!--{0}-->".format(text))
    
    def write_cdata(self, text : str) -> None:
        if (self.__m_elem_not_ended): 
            self.__out(">")
            self.__m_elem_not_ended = False
        self.__out("<![CDATA[{0}]]>".format(text))		

        