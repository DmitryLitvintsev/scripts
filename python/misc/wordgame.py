#!/usr/bin/env python

"""
Given two words of equal length
wite a method to transform one word into another word by changing one character
at a time

The essense of algoruithm is to traverse the tree:

HOME -> DAME

1) Each tree node is a word obtained from previous by replacing
   one letter.

2) Each node has N-children corresponding to all dictionary
   words that are obtained form the node word by replacing one letter

3) We need to traverse tree, breadh first as we generate it until we hit the
   the end word.

4) As we go we use a map from words on the next level to the parent word in order to
   reconstruct the path from start to end word.


            GONE
            NONE
            MONE
            TONE
            HYNE
            ZONE
            HONK
      HONE  LONE
            HONG
            RONE
            WONE
            DONE
            CONE
            IONE
            PONE
            BONE


            MOSE
            JOSE
            ROSE
            POSE
      HOSE  DOSE
            HOST
            NOSE
            LOSE
            BOSE
            HUSE

            HORY
      HOMY  DOMY
            HOLY

            HEHE
      HOHE  HOHN

            NODE
      NOME  NOTE
            NOMA
            NOPE
            NAME

            POMO
      HOMO  MOMO
            HOBO

            SOCE
            SOMA
            SEME
            SOLE
      SOME  SAME
            SORE
            SIME
            SOPE
            SOKE

HOME*       DOBE
            DOMN
            DEME
            DOTE
            DIME
            DOPE
      DOME* DOGE
            DOLE
            DOKE
            DOZE
            DOVE
            DAME*

      HEME  ...
      HAME
HOME  POME
      ROME
      KOME
      HOPE
      HOWE
      HOVE
      HOLE
      COME
      TOME
      MOME
      HUME



['HOME', 'DOME', 'DAME']


"""

from __future__ import print_function
import logging
import string
import sys
import time

_LOGGER = logging.getLogger()

def generateWords(word, wordDictionary):
    """
    generate words from a given words by changing one letter
    """
    words = set()
    wordLength = len(word)
    for i in range(0, wordLength):
        letters = list(word)
        for c in list(string.ascii_uppercase):
            if c != letters[i]:
                letters[i] = c
                newWord="".join(letters)
                if newWord != word and newWord in wordDictionary:
                    words.add(newWord)
    return words

def transform(startWord, endWord, wordDictionary):
    if startWord == endWord:
        return [startWord]
    queue = []
    visited = set()
    backtrackMap = {}

    queue.append(startWord)
    visited.add(startWord)

    while queue:
        w = queue.pop(0)
        words  =  generateWords(w, wordDictionary)
        _LOGGER.debug( "%s -> %s" % (w, words), )
        for v in words:
            if v == endWord:
                thelist = []
                thelist.append(v)
                while w:
                    thelist.insert(0, w)
                    w = backtrackMap.get(w, None)
                return thelist

            if v not in visited:
                queue.append(v)
                visited.add(v)
                backtrackMap[v] = w
    return None


if __name__ == "__main__" :
    logging.basicConfig(level=logging.ERROR,
                        format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
                        datefmt='%H:%M:%S')
    if len(sys.argv) < 3:
        sys.stderr.write("specify start and and word of the same length\n")
        sys.exit(1)
    startWord = sys.argv[1].upper()
    endWord   = sys.argv[2].upper()
    wordLength = len(startWord)

    if wordLength != len(endWord):
        sys.stderr.write("start word and end word have different lengths\n")
        sys.exit(1)
    """
        fill in the word dictionary
    """
    words = set()
    endword_found = False
    with open("/usr/share/dict/words", "r") as f:
        for line in f:
            if not line : continue
            word = line.strip()
            if len(word) != wordLength:
                continue
            if len(word.split()) > 1:
                continue
            words.add(word.upper())
            if endWord == word.upper():
                endword_found = True

    if not endword_found:
        sys.stderr.write("ERROR '%s' is not found in the dictionary\n" % (endWord.lower(), ))
        sys.exit(1)


    wordgame = transform(startWord, endWord, words)

    if wordgame:
        print (" -> ".join(wordgame))
    else :
        print (wordgame)
