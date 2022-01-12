"""
Guests arrive at the folloging hours s = [1, 2, 6, 5, 3]
and leave at  e = [5, 5, 7, 6, 8]. Find minimum number of 
chairs required 

"""

if __name__ == "__main__":

    starts = [1, 2, 6, 5, 3]
    ends = [5, 5, 7, 6, 8]

    all = [(s, 1) for s in starts] + [(e, -1) for e in ends]
    all.sort()

    chairs = 0
    max_count = 0
    for t, count in all:
        chairs += count
        if chairs > max_count: max_count = chairs
    print (max_count)
