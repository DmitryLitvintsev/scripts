"""
Various queue problems. Queue : FIFO
"""

from __future__ import print_function
import sys


class Node:
    
    def __init__(self, data):
        self.next = None
        self.data = data

    def __repr__(self):
        return str(self.data)

    
class Queue:

    def __init__(self):
        self.first = self.last = None
        self.size = 0

    def queue(self, data):
        n = Node(data)
        if self.size == 0:
            self.first = self.last = n
            self.size += 1
            return
        self.last.next = n
        self.last = n
        self.size += 1  

    def dequeue(self):
        if self.size == 0:
            return None

        first = self.first

        if self.size == 1:
            self.first = self.last = None
        else:
            self.first = first.next

        self.size -= 1
        return first.data

    def peek(self):
        if not self.first:
            return None
        return self.first.data

    def __repr__(self):
        if self.size == 0:
            return "None"
        else:
            n = self.first
            txt = "("
            txt += str(n)
            while n.next:
                n = n.next
                txt += "," + str(n)
            txt += ")"
        return txt


if __name__ == "__main__":
    s = Queue()
    print(s)
    s.queue(1)
    s.queue(2)
    s.queue(3)
    s.queue(4)
    print(s)
    a = s.dequeue()
    print("dequeued ", a)
    print(s)

    a = s.peek()
    print("peeked ", a)
    print(s)