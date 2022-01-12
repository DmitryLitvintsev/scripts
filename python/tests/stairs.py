"""
There's a staircase with N steps, and you can climb 1 or 2 steps 
at a time. Given N, write a function that returns the number of 
unique ways you can climb the staircase. The order of the steps matters.

For example, if N is 4, then there are 5 unique ways:

1, 1, 1, 1
2, 1, 1
1, 2, 1
1, 1, 2
2, 2

What if, instead of being able to climb 1 or 2 steps at a time, 
you could climb any number from a set of positive integers X? 
For example, if X = {1, 3, 5}, you could climb 1, 3, or 5 steps 
at a time. Generalize your function to take in X.

f(n) = f(n-x) + f(n-y) + f(n-z) .... 

"""

from __future__ import print_function


def step_counter(n, steps):
    cache = [0] * (n+1)
    cache[0] = 1 
    for i in range(1, n+1):
        cache[i] += sum(cache[i-x] for x in steps if i-x >= 0)
    return cache[n]


if __name__ == "__main__":
    print("N=4, steps = [1,2],   steps = ", step_counter(4, (1,2)))
    print("N=10, steps = [1,3,5],   steps = ", step_counter(10, (1, 3, 5)))