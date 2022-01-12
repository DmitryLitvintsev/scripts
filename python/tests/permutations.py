"""
1.3 Given two strings, write a method to decide if one string is permutation of the other
"""

from __future__ import print_function

import itertools
import sys


def is_permutation(s1, s2):
    if s1 in ["".join(i) for i in list(itertools.permutations(s2))]:
        return True
    else:
        return False


def permutations(s):
    if len(s) <= 1:
        yield s
    else:
        for i in range(len(s)):
            for p in permutations(s[:i]+s[i+1:]):
                yield s[i] + p


def array_permutations(array):
    if len(array) <= 1:
        yield array
    else:
        for i in range(len(array)):
            for p in array_permutations(array[:i]+array[i+1:]):
                yield [array[i]] + p


def is_permutation1(s1, s2):
    sorted_s1 = "".join(sorted(list(s1.upper())))
    sorted_s2 = "".join(sorted(list(s2.upper())))
    return sorted_s1 == sorted_s2


if __name__ == "__main__":
    print(is_permutation(sys.argv[1], sys.argv[2]))
    if sys.argv[1] in list(permutations(sys.argv[2])):
        print("True")
    else:
        print("False")
    print(is_permutation1(sys.argv[1], sys.argv[2]))

