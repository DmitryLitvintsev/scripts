from __future__ import print_function
import math
#
# https://en.wikipedia.org/wiki/Golden_ratio
#
# a > b > 0
#
# (a + b) / a = a / b = phi
# 
# take b = 1
# ( a + 1 ) / a = a
# a + 1 = a^2
# a^2 - a - 1 = 0
# a = ( 1 +/- sqrt(1 + 4) ) / 2 = (1 +/- sqrt(5)) / 2
# take positive:
# a = (1 + sqrt(5)) / 2 = 1.61803 - Golden Ratio


def fib_recursive(n):
    #
    # recursive O(phi^n)
    #

    if n <= 1:
        return n
    if n > 0:
        return fib_recursive(n-1) + fib_recursive(n-2)
    else:
        raise ValueError("argument must be positive integer")


def fib_loop(n):
    #
    # O(n)
    #

    if n < 0:
        raise ValueError("save must be True if recurse is True")
    
    f2, f1 = 0, 1
    for _ in range(n):
        f = f1 + f2
        f2 = f1
        f1 = f
    return f2


def fib_dp(n):
    #
    # "dynamic programming"
    #

    if n < 0:
        raise ValueError("save must be True if recurse is True")
    
    f = [0, 1]
    for i in range(2, n+1):
        f.append(f[i-1] + f[i-2])
    return f[n]


phi = 0.5 * (1. + math.sqrt(5))
sqrt5 = math.sqrt(5)


def fib_formula(n):
    #
    # just using formula
    #
    if n < 0:
        raise ValueError("save must be True if recurse is True")
    if n <= 1: return n
    return int(round(pow(phi, n)/sqrt5))
    

def print_series(title, func):
    print(title,  end="")
    for i in range(10):
        print(func(i), end=" ")
    print()


if __name__ == "__main__":

    print_series("recursive F_10: ", fib_recursive)
    print_series("loop      F_10: ", fib_loop)
    print_series("dp        F_10: ", fib_dp)
    print_series("formula   F_10: ", fib_formula)
