"""
Write an algorithm such that
of element in a MxN matrix is 0, its entire row and
column is set to 0
"""

from __future__ import print_function
import sys


def print_matrix(a):
    for i in range(len(a)):
        print ("|", end=" ")
        for j in range(len(a[i])):
            print("%2d" % (a[i][j],), end=" ")
        print ("|")


def zero_matrix(a):
    zero_rows = set()
    zero_columns = set()

    for row in range(len(a)):
        for column in range(len(a[row])):
            if a[row][column] == 0:
                zero_rows.add(row) 
                zero_columns.add(column)

    for row in range(len(a)):
        for column in range(len(a[row])):
            if row in zero_rows or column in zero_columns:
                a[row][column] = 0


if __name__ == "__main__":
    matrix = [[1, 2, 3, 4, 5],
              [6, 7, 8, 9, 10],
              [11, 12, 0, 14, 15],
              [16, 17, 18, 19, 0]]
    print_matrix(matrix)
    print ("---")
    zero_matrix(matrix)
    print_matrix(matrix)
