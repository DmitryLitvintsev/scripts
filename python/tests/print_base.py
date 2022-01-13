"""

"""
from __future__ import print_function
import string
import sys

chars = string.printable

def print_base(number, base):
    if number >= base:
        print_base(number // base, base)
    print(chars[number % base], end="")


def convert(number, base):
    txt=""
    while number:
        txt += chars[number % base]
        number //= base
    return txt[::-1]
    

if __name__ == "__main__":
    #print_base(int(sys.argv[1]), int(sys.argv[2]))
    a = convert(int(sys.argv[1]), int(sys.argv[2]))
    print(a)
