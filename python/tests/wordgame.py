"""
word game - supply start and end word. Same length. Arrive from start to end word
by changing one letter at a time
"""
from __future__ import print_function

import string
import sys


def generate_words(word, word_dictionary):
    words = []
    length = len(word)

    for i in range(length):
        letters = list(word)
        for char in string.ascii_uppercase:
            if char == word[i]:
                continue
            letters[i] = char
            new_word = "".join(letters)
            if new_word in word_dictionary:
                words.append(new_word)
    return words


def transform(start, finish, word_dictionary):

    if start == finish:
        return [start]

    queue = []
    visited = set()
    backtrack_map = {}
    the_list = []

    queue.append(start)
    visited.add(start)

    while queue:
        word = queue.pop(0)
        words = generate_words(word, word_dictionary)

        if not words:
            continue

        if finish in words:
            the_list.append(finish)
            while word:
                the_list.insert(0, word)
                word = backtrack_map.get(word, None)
            return the_list

        for w in words:
            if w not in visited:
                visited.add(w)
                queue.append(w)
                backtrack_map[w] = word
    return the_list
    

if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.stderr.write("Please provide equal length start / end word\n")
        sys.stderr.flush()
        sys.exit(1)
    start = sys.argv[1]
    finish = sys.argv[2]
    word_length = len(start)
    
    if len(start) != len(finish):
        sys.stderr.write("words need to be equal length\n")
        sys.stderr.flush()
        sys.exit(1)

    words = set()
    found_finish = False
    with open("/usr/share/dict/words", "r") as f:
        for line in f:
            if not line:
                continue
            word = line.strip()
            if len(word) != word_length:
                continue
            if finish == word:
                found_finish = True
            words.add(word.upper())

    if not words or not found_finish:
        sys.stderr.write("words set is empty or end word not found. aborting\n")
        sys.stderr.flush()
        sys.exit(1)

    result = transform(start.upper(), finish.upper(), words)
    if result:
        print(" -> ".join(result))
    else:
        print(result)