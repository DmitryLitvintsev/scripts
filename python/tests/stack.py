"""
Various stack problems. Stack - LIFO
"""

from __future__ import print_function
import sys


class Node:
    
    def __init__(self, data):
        self.next = None
        self.data = data

    def __repr__(self):
        return str(self.data)


class Stack:

    def __init__(self):
        self.top = None
        self.size = 0
        self.min = 1<<64-1

    def push(self, data):
        n = Node(data)
        n.next = self.top
        self.top = n
        self.size += 1
        if data <  self.min:
            self.min = data

    def pop(self):
        if not self.top:
            return None
        top = self.top
        self.top = self.top.next
        self.size -= 1
        if top.data == min:
            self.get_min()
        return top.data

    def peek(self):
        if not self.top:
            return None
        return self.top.data

    def get_min(self):
        # return minimum element O(N)
        if self.size == 0:
            return None
        min = 1<<64-1
        current = self.top
        while current:
            if current.data < min:
                min = current.data
            current = current.next
        self.min =  min

    def get_minimum(self):
        return self.min

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
    print("min ", s.get_minimum());
    print(s)
    a = s.pop()
    print("popped ", a)
    print(s)
    a = s.peek()
    print("peeked ", a)
    print(s)
