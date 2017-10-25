#!/bin/python
"""
Adapted from https://github.com/Yelp/mrjob
"""

from mrjob.job import MRJob, MRStep
from lxml import etree
import heapq
import re


class top100(MRJob):
    # the splitting method has been applied before mapper
    # the mapper return the word for each line
    def mapper_get_words(self, _, line):
        for word in re.findall("\w+", line):
            yield (word.lower(), 1)

    # the shuffling process has been finished and then
    # same word has been in the same processor
    # then sum all the count together
    def reducer_count_words(self, word, counts):
        yield (word, sum(counts))

    # if this the second mapper, this mapper has been face
    # all word,count pair from the last reducer
    # the tuple is better to find the most common
    # 100 words
    def mapper_word_count_tuple(self, word, count):
        yield (None, (count, word))

    # find the 100 most common words
    def reducer_top100(self, _, pairs):
        top = []
        for (count, word) in pairs:
            if len(top) < 100:
                heapq.heappush(top, (count, word))
            if (count, word) > top[0]:
                heapq.heappushpop(top, (count, word))
        for pair in top:
            yield (pair[1], pair[0])

    # define steps.
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   reducer=self.reducer_count_words),
            MRStep(mapper=self.mapper_word_count_tuple,
                   reducer=self.reducer_top100)]


# boiler plate to turn code into running
if __name__ == '__main__':
    top100.run()
