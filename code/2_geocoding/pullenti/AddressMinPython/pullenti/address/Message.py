# SDK Pullenti Address (client version), version 4.28, february 2025.
# Copyright (c) 2013-2025, Pullenti. All rights reserved.
# Non-Commercial Freeware and Commercial Software.
# This class is generated using the converter Unisharping (www.unisharping.ru) from Pullenti C# project.
# The latest version of the code is available on the site www.pullenti.ru


from pullenti.address.MessageCode import MessageCode

class Message:
    """ Элемент сообщения """
    
    def __init__(self, code_ : 'MessageCode', txt : str, error_ : bool) -> None:
        self.code = MessageCode.NOADDRESS
        self.text = None;
        self.error = False
        self.code = code_
        self.text = txt
        self.error = error_
    
    def __str__(self) -> str:
        return self.text