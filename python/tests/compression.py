"""
1.4 implement a method to perform basic sctring compression using
    counts of repeated characters. aabccccaaa -> a2b1c5a3. If the compressed
    string is not smaller than the original, return the original
"""

from __future__ import print_function
import sys


def compress(s):
    compressed=""
    previous = s[0]
    count = 1
    for char in s[1:]:
        if previous != char:
            compressed += previous + str(count)
            previous = char
            # reset count
            count = 1
        else:
            count += 1
            
    # to pick last character
    compressed += previous + str(count)

    if len(compressed) > len(s):
        return s
    else:
        return compressed
        

if __name__ == "__main__":
    print(sys.argv[1], " -> ", compress(sys.argv[1]))
