"""
return False if input string has all unique characters
"""

from __future__ import print_function
import sys


def is_all_unique_characters(txt):
    checker = set()
    for i in txt.upper():
        if i not in checker:
            checker.add(i)
        else:
            return False
    return True


def is_all_unique_characters1(txt):
    checker = 0
    base = ord('A')
    for i in txt.upper():
        val = ord(i) - base
        if checker & (1 << val) != 0:
            return False
        else:
            checker |= (1 << val)
    return True


if __name__ == "__main__":
    print (is_all_unique_characters(sys.argv[1]))
    print (is_all_unique_characters1(sys.argv[1]))
