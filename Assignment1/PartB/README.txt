
Group 23 : Big Data Systems (CS744) Fall 2017

Part B: Developing a MapReduce application

This program groups all the anagrams in a given list of words and then
orders the sorted anagram string in a descending order of size of each.

JOB 1 : Finding the anagrams

Given input words :

abroad
early
natured
unrated
layer
aboard
untread
leary
relay

We need to create strings of anagram pairs as intermediate output of our first
job of the MapReduce application.

Intermediate Output Produced :

abroad,aboard~2
early,leary,layer,relay~4
natured,unrated,untread~3

Explanation :

First we sort every input word alphabetically as part of the first map function.
The map function creates (sorted_word, word) pairs for every input word.

In the reduce function, we use sorted_word key produced by the map operation to
group the anagrams. This is based on the fact that all anagrams have the same
alphabetical representation when sorted. We format the output of this step in the
format shown above, so that it is easier to detect the size of anagram string in
the subsequent steps

JOB 2 : Sorting the anagram groups in descending order of word count.

Given input string :

abroad,aboard~2
early,leary,layer,relay~4
natured,unrated,untread~3

Final Output produced :

early leary layer relay
natured unrated untread
abroad aboard

Explanation :

We process the intermediate output as part of our map operation in the second job.
Split the given input sting using the delimiter used in reduce step previously i.e.
"~". Now the map operation outputs (n, anagram_string) pairs for every anagram
string. Here n is the number of anagrams in the anagram string.

In the reduce function, We output all anagram strings (separated by newline) that
have the same n. We achieve the final ordering required using "DecreasingComparator".
Note that we don't worry about the relative order within a group of anagram strings
having the same n (as mentioned in specifications). This helps in reducing the runtime
of our application.

Run the application using below command after jar creation: 

hadoop jar ac.jar AnagramSorter /input /output
