"""
Number of ways to decode a numeric string so that 
1 -> a
...
26 -> z 

"""

from __future__ import print_function
import sys

def num_ways(s):
    if s.startswith("0"):
        return 0
    length = len(s)
    cache = [0] * (length+1)
    cache[0] = 1 
    cache[1] = 1 
    for i in range(2, length+1):
        cache[i] += cache[i-1] + cache[i-2]
    return cache[length]

if __name__ == "__main__":
    print(sys.argv[1], " ", num_ways(sys.argv[1]))

    
