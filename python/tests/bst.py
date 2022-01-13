"""
Binary search tree. Traversal
"""
from __future__ import print_function
import os

def search(graph, root, key):

    if not root:
        return None

    if root == key:
        return root

    if key < root:
        #
        # for BST if key less than root go left
        #
        left = graph[root][0]
        return search(graph, left, key)
    else:
        #
        # for BST if key is greater than root go right
        #
        right =  graph[root][1]
        return search(graph, right, key)

    
def bf(graph, root):
    #
    # breadth first
    #
    queue = []
    visited = []
    queue.append(root)

    while queue:
        node = queue.pop(0)

        if node in visited:
            continue
        
        visited.append(node)
        
        for i in graph[node]:
            if i:
                queue.append(i)
    return visited


def df(graph, root):
    #
    # depth first
    #
    visited = []

    visited.append(root)

    for i in graph[root]:
        if i:
            visited += df(graph, i)

    return visited


def postorder(graph, root):
    visited = []
    if root:
        left = graph[root][0]
        visited = postorder(graph, left)
        right = graph[root][1]
        visited += postorder(graph, right)
        visited.append(root)
    return visited

        
def inorder(graph, root):
    visited = []
    if root:
        left = graph[root][0]
        visited = inorder(graph, left)
        visited.append(root)
        right = graph[root][1]
        visited += inorder(graph, right)
    return visited


def preorder(graph, root):
    visited = []
    if root:
        visited.append(root)
        left = graph[root][0]
        visited += preorder(graph, left)
        right = graph[root][1]
        visited += preorder(graph, right)
    return visited


def check_height(graph, root):

    if not root:
        return 0;

    left = graph[root][0]
    right = graph[root][1]

    left_height = check_height(graph, left)
    if left_height == -1:
        return -1

    right_height = check_height(graph, right)
    if right_height == -1:
        return -1

    diff = left_height - right_height

    if abs(diff) > 1:
        return -1

    return max(left_height, right_height) + 1 


def is_balanced(graph, root):
    if check_height(graph, root) == -1:
        return False
    else:
        return True
    
class TreeNode:
    
    def __init__(self, key):
        self.left = None
        self.right = None
        self.key = key
        
    def inorder(self, root):
        visited = []
        if root:
            visited = self.inorder(root.left)
            visited.append(root.key)
            visited += self.inorder(root.right)
        return visited
        
        
def array_to_bst(array, start, end):
    if end < start:
        return None
    middle = (start + end) // 2
    node = TreeNode(array[middle])
    node.left = array_to_bst(array, start, middle - 1)
    node.right = array_to_bst(array, middle + 1, end)
    return node
                

def printTree(node, level=0):
    if node != None:
        printTree(node.right, level + 1)
        print(' ' * 4 * level + '->', node.key)
        printTree(node.left, level + 1)
        

if __name__ == "__main__":
    """
           8
         /   \
        3     10
       / \     \
      1   6     14
         / \    /
        4   7 13 

    """
    bst = {
        8 : [3, 10],
        3 : [1, 6],
        10 : [None, 14],
        1 : [None, None],
        6 : [4, 7],
        14 : [13, None],
        4 : [None, None],
        7 : [None, None],
        13 :[None, None],
    }
    print("search(bst, 8, 14) ", search(bst, 8, 14))
    print("search(bst, 8, 15) ", search(bst, 8, 15))
    print("search(bst, 8, 1) ", search(bst, 8, 1))

    print("bf", bf(bst, 8))
    print("df ", df(bst, 8))
    print("preorder", preorder(bst, 8))
    print("inorder", inorder(bst, 8))
    print("postorder", postorder(bst, 8))
    print("is balanced ", is_balanced(bst, 8))

    """
           8
         /   \
        3     10
       / \   / \
      1   6 9   14
         / \    / \
        4   7 13   15

    """
    bst = {
        8 : [3, 10],
        3 : [1, 6],
        10 : [9, 14],
        1 : [None, None],
        6 : [4, 7],
        9 : [None, None],
        14 : [13, 15],
        4 : [None, None],
        7 : [None, None],
        13 :[None, None],
        15 :[None, None],
    }
    
    print("is balanced ", is_balanced(bst, 8))
    array = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160]

    BST =  array_to_bst(array, 0, len(array) - 1)
    visited = BST.inorder(BST)
    print(visited)
    printTree(BST)
