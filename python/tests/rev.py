from __future__ import print_function
import sys

def print_matrix(a):
    for i in range(len(a)):
        print ("|", end = " ")
        for j in range(len(a[i])):
            print(a[i][j], end=" ")
        print ("|")
        

def reverse(a):
    l = len(a)
    letters = list(a)
    for i in range (l//2):
        tmp = letters[i]
        letters[i] = letters[l-1-i]
        letters[l-1-i] = tmp
    return "".join(letters)

def matrix_zero(a):
    # find positions of zeroes
    rows = {}
    columns = {}
    for row in range(len(a)):
        for column in range(len(a[row])):
            value = a[row][column]
            if value == 0:
                if column not in columns:
                    columns[column] = True
                if row not in rows:
                    rows[row] = True

    for row in range(len(a)):
        for column in range(len(a[row])):
            if column in columns or row in rows:
                a[row][column] = 0

    print (a)

def rotate(a):
    layers = len(a) // 2
    n = len(a)
    for layer in range(layers):
        first = layer
        last = n - layer - 1
        print (layer, first, last)
        for i in range(first, last):
            offset = i - first 
            tmp = a[first][i]
            a[first][i] = a[last-offset][first]
            a[last-offset][first] = a[last][last-offset]
            a[last][last-offset] = a[i][last]
            a[i][last] = tmp
    

if __name__ == "__main__":
    print(sys.argv[1],"->",reverse(sys.argv[1]))
    matrix = [ [1,2,3],
               [4,5,0],
               [7,8,9]]
    #matrix_zero(matrix)
              
    print_matrix(matrix)
    rotate(matrix)
    print()
    print_matrix(matrix)
