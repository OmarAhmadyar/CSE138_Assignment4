from os import getenv
from typing import cast
from copy import deepcopy as copy


class IP:
    """
    This class stores a single ip address

    __init__ will construct the class given an ip address as a string
    __eq__ defined to compare ip addresses
    __str__ defined to turn ip address into string form
    """
    def __init__(self, ip: str = "0.0.0.0") -> None:
        self.__data__ = (-1,-1,-1,-1)

        fail = False
        spl = ip.split('.')
        addrint = (-1,-1,-1,-1)
        try:
            if len(spl) != 4:
                fail = True
            else:
                addrint = cast(tuple[int,int,int,int], tuple(map(int, spl)))
        except:
            fail = True

        for num in addrint:
            if 0 <= num <= 0xFF: pass
            else: fail = True

        if fail: raise Exception("Attempted to construct invalid ip")
        else: self.__data__ = addrint


    def __eq__(self, other:object) -> bool:
        if not isinstance(other, IP):
            return False
        return self.__data__ == other.__data__


    def __str__(self) -> str:
        ret = ""
        first = True
        for field in self.__data__:
            ret = ret + ("" if first else ".") + str(field)
            first = False
        return ret




class Port:
    """
    This class stored a port number

    __init__ constructs the object
    __len__ returns the port number
    __eq__ allows for comparisons
    """
    def __init__(self, num: int = 0) -> None:
        if 0 <= num <= 0xFFFF:
            self.__num__ = num
        else:
            raise Exception("Attempted to construct port outside of 16 bit range")


    def __len__(self) -> int:
        return self.__num__


    def __eq__(self, other) -> bool:
        if isinstance(other, int):
            return self.__num__ == other
        elif not isinstance(other, Port):
            return False
        return self.__num__ == other.__num__




class Address:
    def __init__(self, ip:str = "0.0.0.0", port:int = 0):
        self.__ip__ = IP(ip)
        self.__port__ = Port(port)

    def getPort(self) -> Port:
        return self.__port__


    def getIP(self) -> IP:
        return self.__ip__


    def setPort(self, num: int) -> None:
        self.__port__ = Port(num)


    def setIP(self, ip: str) -> None:
        self.__ip__ = IP(ip)


    def setIPEnv(self, envname: str) -> None:
        a = getenv(envname)
        if a is None:
            raise Exception(f"No environment variable named {envname}")
        else: self.setIP(a)


    def setPortEnv(self, envname: str) -> None:
        p = getenv(envname)
        if p is None:
            raise Exception(f"No environment variable named {envname}")
        else:
            self.setPort(int(p))

    def __str__(self) -> str:
        return str(self.__ip__) + ':' + str(len(self.__port__))


    def __eq__(self, that) -> bool:
        if not isinstance(that, Address):
            return False
        return str(self) == str(that)




def main():
    # Test Main for addr.py
    x = Address(port=1235, ip="1.1.1.1")
    print(x)

if __name__ == "__main__":
    main()
