#!/usr/bin/env python

import sys
import os
import getopt


def bfs(graph, s, item):
    #
    # breadth first search
    #
    if not s:
        return None

    if s == item:
        return s
    
    visited = []
    queue = []
    queue.append(s)

    while queue:
        node = queue.pop(0)
        if node in visited:
            continue
        visited.append(node)
        for i in graph[node]:
            if i == item:
                return i
            else: 
                queue.append(i)
    return None


def dfs(graph, s):
    visited = []
    node = s
    visited.append(node)
    for i in graph[node]:
        if i not in visited:
            visited += dfs(graph, i)
    return visited


def is_there_path_between_two_nodes(graph, a, b):

    if a not in graph or b not in graph:
        raise ValueError("no such element")

    queue = []
    visited = []

    queue.append(a)

    while queue:
        node = queue.pop(0)
        if node in visited:
            continue
        visited.append(node)
        if node == b:
            return True
        for i in graph[node]:
            queue.append(i)
    
    return False
            

if __name__ == "__main__":

    """

         1
        / \
       /   \
      /     \
     2       3
    / \      /
   4   5    6
  /       / | \
 7       8 9  10

The correct output should look like this:
preorder:    1 2 4 7 5 3 6 8 9 DFS (recursive)
inorder:     7 4 2 5 1 8 6 9 3
postorder:   7 4 5 2 8 9 6 3 1 
level-order: 1 2 3 4 5 6 7 8 9 BFS

[1, 2, 3, 4, 5, 6, 7, 8, 9] BFS
-----
[1, 2, 4, 7, 5, 3, 6, 8, 9] DFS (recursive)
-----
[1, 3, 6, 9, 8, 2, 5, 4, 7] DFS (iterative)

    """

    graph = {
        1 : [2, 3],
        2 : [4, 5],
        3 : [6],
        4 : [7],
        5 : [],
        6 : [8, 9, 10],
        7 : [],
        8 : [],
        9 : [],
        10 : []
        }

    print(bfs(graph, 1, 10))
    print(bfs(graph, 1, 20))
    print(bfs(graph, 1, 1))
    print(dfs(graph, 1))
    print("is_there_path_between_two_nodes(graph, 2, 3)",
          is_there_path_between_two_nodes(graph, 2, 3))
    print("is_there_path_between_two_nodes(graph, 2, 7)",
          is_there_path_between_two_nodes(graph, 2, 7))
    print("is_there_path_between_two_nodes(graph, 1, 7)",
          is_there_path_between_two_nodes(graph, 1, 7))
    print("is_there_path_between_two_nodes(graph, 1, 10)",
          is_there_path_between_two_nodes(graph, 1, 10))

