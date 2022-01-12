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

    def push(self, data):
        n = Node(data)
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
    s.push(1)
    s.push(2)
    s.push(3)
    s.push(4)
    print(s)
    a = s.pop()
    print("popped ", a)
    print(s)
    a = s.peek()
    print("peeked ", a)
    print(s)
