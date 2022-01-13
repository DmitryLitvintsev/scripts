"""
Various stack problems. Stack - LIFO
Stack that has pop(), push(), min() methods that are O(1) in time 


"""

from __future__ import print_function
import sys

_BIG_NUMBER = 1<<64-1

class Node:
    
    def __init__(self, data, min):
        self.next = None
        self.data = data
        self.min = min

    def __repr__(self):
        return str(self.data)


class Stack:

    def __init__(self):
        self.top = None
        self.size = 0

    def min(self):
        if self.size == 0:
            return  _BIG_NUMBER
        else:
            return self.top.min

    def get_minimum(self):
        return self.min()
        

    def push(self, data):
        minimum = min(self.min(), data)
        n = Node(data, minimum)
        n.next = self.top
        self.top = n
        self.size += 1

    def pop(self):
        if not self.top:
            return None
        top = self.top
        self.top = self.top.next
        self.size -= 1
        return top.data

    def peek(self):
        if not self.top:
            return None
        return self.top.data

    def __repr__(self):
        if self.size == 0:
            return "None"
        else:
            n = self.top
            txt = "("
            txt += str(n)
            while n.next:
                n = n.next
                txt += "," + str(n)
            txt += ")"
        return txt


if __name__ == "__main__":
    s = Stack()
    print(s)
    print("min ", s.get_minimum());
    s.push(1) 
    print("min ", s.get_minimum());
    s.push(2)
    s.push(3)
    s.push(4)
    s.push(0)
    print("min ", s.get_minimum());
    print(s)
    a = s.pop()
    print("popped ", a)
    print(s)
    a = s.peek()
    print("peeked ", a)
    print(s)
