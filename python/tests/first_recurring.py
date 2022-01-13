"""
print first recurring character in string
"""

from __future__ import print_function
import sys

def first_recurring(s):
    checker = 0
    base = ord('A')
    for i in s.upper():
        val = ord(i) - base
        if checker & (1<<val) != 0:
            return i
        else:
            checker |= (1<<val)
    return None

if __name__ == "__main__":
    print(first_recurring(sys.argv[1]))
