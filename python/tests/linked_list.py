"""
Various Linked list problems 
"""

from __future__ import print_function
import sys


class Node:
    
    def __init__(self, data):
        self.next = None
        self.data = data

    def __repr__(self):
        return str(self.data)

    
class LinkedList:

    def __init__(self):
        self.head = None
        self.tail = None
        self.size = 0

    def add_front(self, data):
        #
        # add item to the front
        # 
        self.size += 1 
        node = Node(data)
        node.next = self.head
        self.head = node
        if not self.tail:
            self.tail = node

    def add_back(self, data):
        #
        # add item to the back
        # 
        self.size += 1 
        node = Node(data)
        if not self.head:
            self.head = node
            self.tail = node
            return
        self.tail.next = node
        self.tail = node

    def remove(self, data):
        #
        # remove first occurrence of data
        #
        if self.size == 0:
            return
        current = self.head
        previous = None
        if current:
            if current.data == data:
                self.size -= 1
                self.head = current.next
                return

        while current:
            if current.data == data:
                self.size -= 1 
                break
            previous = current
            current = current.next

        # nothing found 
        if not current:
            return

        previous.next = current.next

        if current == self.tail:
            self.tail = previous

    def remove_duplicates_space(self):
        #
        # time complexity O(N), space complexity O(x)
        #
        if self.size <= 1:
            return
        duplicates = set()
        current = self.head
        previous = None

        while current:
            if current.data in duplicates:
                previous.next = current.next
                self.size -= 1 
            else:
                duplicates.add(current.data)
                previous = current
            current = current.next
        self.tail = previous

    def remove_duplicates_time(self):
        #
        # time complexity O(N*N), space complexity O(1)
        #
        if self.size <= 1:
            return

        current = self.head
        runner = None
        while current:
            runner = current
            while runner.next:
                if current.data == runner.next.data:
                    runner.next = runner.next.next
                    self.size -= 1
                else:
                    runner = runner.next
            current = current.next
            
        self.tail = runner

    def pop(self):
        #
        # returns last element, removes it
        #

        if not self.tail:
            return None

        current = self.head
        previous = None

        #
        # get to the element that has null next
        # that is the the tail.
        #

        while current.next:
            previous = current
            current = current.next

        self.size -= 1

        if not previous:
            self.head = None
            self.tail = None
        else:
            previous.next = None
            self.tail = previous

        return current.data

    def reverse(self):

        if self.size <= 1:
            return

        head = self.head
        tail = self.tail
        
        current = self.head
        the_next = None
        previous = None

        # 1-2-3-4-5-6
        
        while current:
            the_next = current.next
            current.next = previous
            previous = current
            current = the_next
            
        self.head = tail
        self.tail = head 

    def __repr__(self):
        if self.size == 0:
            return "None"
        else:
            n = self.head
            txt = "("
            txt += str(n)
            while n.next:
                n = n.next
                txt += "," + str(n)
            txt += ")"
        return txt

    def get_head(self):
        return self.head

    def get_tail(self):
        return self.tail
        
    def add_loop(self, data):
        #
        # add loop to list
        #
        current = self.head
        while current:
            if current.data == data:
                self.tail.next = current
                break
            current = current.next

    def has_loop(self):
        #
        # cheater method 
        #
        if self.tail.next:
            return self.tail.next.data

    def find_loop(self):
        #
        # assume we do not keep the tail
        #
        hash = set()
        current = self.head
        while current:
            if current in hash:
                return current.data
            hash.add(current)
            current = current.next
        return None


    
if __name__ == "__main__":
    l = LinkedList()
    l.remove(1)
    l.add_back(1) 
    print(l, l.size, l.get_head(), l.get_tail())
    popped = l.pop()
    print ("popped", popped)
    print(l, l.size, l.get_head(), l.get_tail())

    print("found loop ", l.find_loop())
    
    l.add_front(2)
    l.add_front(2)
    l.add_back(5)
    l.add_back(6)
    print(l, l.size, l.get_head(), l.get_tail())
    l.remove(2)
    print(l, l.size, l.get_head(), l.get_tail()) 
    l.remove(6)
    print(l, l.size, l.get_head(), l.get_tail())
    l.remove(1)
    print(l, l.size, l.get_head(), l.get_tail())
    l.add_front(2)
    l.add_front(2)
    l.add_back(5)
    l.add_back(6)
    print(l, l.size, l.get_head(), l.get_tail())
    l.reverse()
    print(l, l.size, l.get_head(), l.get_tail())
    item = l.pop()
    print ("popped", item)
    print(l, l.size, l.get_head(), l.get_tail())
    l.add_front(2)
    l.add_front(2)
    l.add_back(5)
    l.add_back(6)
    print(l, l.size, l.get_head(), l.get_tail())
    l.remove_duplicates_space()
    print(l, l.size, l.get_head(), l.get_tail())
    l.add_front(2)
    l.add_front(2)
    l.add_back(5)
    l.add_back(6)
    print(l, l.size, l.get_head(), l.get_tail())
    l.remove_duplicates_time()
    print(l, l.size, l.get_head(), l.get_tail())

    l = LinkedList()
    for i in range(10):
        l.add_back(i)
    print(l, l.size, l.get_head(), l.get_tail())
    l.add_loop(5) # poison pil, tail points at 5
    print("has loop", l.has_loop())
    loop = l.find_loop()
    print("Found loop", loop)
