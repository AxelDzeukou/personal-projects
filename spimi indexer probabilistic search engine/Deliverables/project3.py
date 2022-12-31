# subproject1
# create index using spimi and compare timing between this and initial indexer
# for each article in each sgm file take the text in
# <TITLE>
# <AUTHOR>
# <DATELINE>
# <BODY>

import math
import pandas as pd
import numpy as np
import time
import re
from nltk.tokenize import word_tokenize
from nltk.tokenize import RegexpTokenizer
import pandas as pd
import functools
from bs4 import BeautifulSoup
import random
import string

from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer


def block_reader(path):
    import os
    # print(os.listdir(path))#list directories in reuters folder
    os.chdir(path)  # Change to /reuters21578 folder

    # read content from each sgm file and send for raw text to be extracted
    for file_name in sorted(os.listdir(".")):
        if file_name.endswith(".sgm"):
            # SGM17 has an encoding error -
            f = open(file_name, 'r', errors='ignore')
            # UnicodeDecodeError: 'utf-8' codec can't decode byte 0xfc in position 1519554: invalid start byte
            reuters_file_content = f.read()
            yield reuters_file_content
        else:
            continue


# bigger list-set of (token,articleid)
bigger_tokens_articleid = []

# dic of length of document
doc_lengths = {}

# number of documents
no_of_documents = 0

# term frequency in docs
term_frequency_docs = {}

# for project
reutersfolderpath = "C:/Users/Axel/Desktop/newwine/comp 479/project 2 final/reuters21578 - Copy"  # path to reuters folder containing sgm files
for sgmfiletxt in block_reader(reutersfolderpath):
    soup = BeautifulSoup(sgmfiletxt, 'html.parser')
    #     print(list(map(lambda x: x.string,soup.find_all(''))))

    for link in soup.find_all('reuters'):
        # get article id
        article_id = link.get('newid')

        # return a string which is a concatination of each text in the subtags of <text>
        article_text = link.find('text').get_text()

        # omit punctation
        article_text = article_text.translate(str.maketrans('', '', string.punctuation))

        # list-set of (token,articleid)
        article_tokens = set(word_tokenize(article_text))
        tokens_articleid = list(map(lambda x: [x, article_id], article_tokens))

        # get term frequency of term per doc
        duplicate_tokens = word_tokenize(article_text)

        # term frequency increment
        # {token:{article_id: count}}
        for token in duplicate_tokens:
            if token in term_frequency_docs:
                article_dic = term_frequency_docs[token]
                if article_id in article_dic:
                    article_dic[article_id] += 1
                elif article_id not in article_dic:
                    article_dic[article_id] = 1

            elif token not in term_frequency_docs:
                term_frequency_docs[token] = {article_id: 1}

        # add list-set to bigger list-set
        bigger_tokens_articleid.extend(tokens_articleid)

        # add doc length
        doc_lengths[article_id] = len(article_tokens)

        # increment number of documents
        no_of_documents += 1

        # uncomment to extract text for 1 article instead of all articles in 1 sgm file
        # break

# uncomment to extract text in 1 sgm file instead of all sgm files
#  break
# \for project

# for demo
# #demo.sgm file
# demosgmfilename="./demo.sgm" # path to demo sgm file

# f= open(demosgmfilename, 'r', errors='ignore')
# sgmfiletxt = f.read()
# soup = BeautifulSoup(sgmfiletxt, 'html.parser')
# for link in soup.find_all('reuters'):

#     #get article id
#     article_id=link.get('newid')

#     #return a string which is a concatination of each text in the subtags of <text>
#     article_text=link.find('text').get_text()

#     #omit punctation
#     article_text= article_text.translate(str.maketrans('', '', string.punctuation))

#     #list-set of (token,articleid)
#     article_tokens=set(word_tokenize(article_text))
#     tokens_articleid=list(map(lambda x:[x,article_id],article_tokens))

#     #get term frequency of term per doc
#     duplicate_tokens=word_tokenize(article_text)

#     #term frequency increment
#     #{token:{article_id: count}}
#     for token in duplicate_tokens:
#      if token in term_frequency_docs:
#       article_dic=term_frequency_docs[token]
#       if article_id in article_dic:
#        article_dic[article_id]+=1
#       elif article_id not in article_dic:
#        article_dic[article_id]=1

#      elif token not in term_frequency_docs:
#       term_frequency_docs[token]={article_id:1}


#     #add list-set to bigger list-set
#     bigger_tokens_articleid.extend(tokens_articleid)

#     #add doc length
#     doc_lengths[article_id]=len(article_tokens)

#     #increment number of documents
#     no_of_documents+=1

# \for demo

# mini list-set of (token,articleid)
mini_bigger_tokens_articleid = bigger_tokens_articleid[0:10000]

# average doc length
sum = 0
for length in doc_lengths.values():
    sum += length
average_doclength = sum / len(doc_lengths)

# naive indexer
start_time_naive_indexer = time.time()
print("The start time for naive indexer is", start_time_naive_indexer)

# creating indexer using pandas

dic_term_tokenID = {'terms': list(map(lambda x: x[0], mini_bigger_tokens_articleid)),
                    'tokenid': list(map(lambda x: x[1], mini_bigger_tokens_articleid))}

# Calling DataFrame constructor on list
df = pd.DataFrame(dic_term_tokenID)

# group rows based on terms
grouped_df = df.groupby("terms")

# create a list of tokenids on each grouped term as postings list
grouped_lists = grouped_df["tokenid"].apply(list)

# reset index to ease index formation
grouped_lists = grouped_lists.reset_index()

# display(grouped_lists)
sortdf = grouped_lists.sort_values(by=['terms'])
initial_terms = sortdf.terms.tolist()
listofpostlist = [sorted(list(map(int, pl))) for pl in sortdf.tokenid.tolist()]
naive_indexer = dict(zip(initial_terms, listofpostlist))

end_time_naive_indexer = time.time()
print("The time taken for naive indexer is", (end_time_naive_indexer - start_time_naive_indexer) * 1000)

# spimi indexer
start_time_spimi_indexer = time.time()
print("The start time for spimi indexer is", start_time_spimi_indexer)

spimi_indexer = {}

for token_stream in mini_bigger_tokens_articleid:
    if token_stream[0] in spimi_indexer:
        spimi_indexer[token_stream[0]].append(token_stream[1])


    elif token_stream[0] not in spimi_indexer:
        spimi_indexer[token_stream[0]] = []
        spimi_indexer[token_stream[0]].append(token_stream[1])

print(list(spimi_indexer.items())[0])

end_time_spimi_indexer = time.time()
print("The time taken for spimi indexer is", (end_time_spimi_indexer - start_time_spimi_indexer) * 1000)

# full indexers with all )token,docid) pairs

# naive indexer
start_time_naive_indexer = time.time()
print("The start time for naive indexer is", start_time_naive_indexer)

# creating indexer using pandas

dic_term_tokenID = {'terms': list(map(lambda x: x[0], bigger_tokens_articleid)),
                    'tokenid': list(map(lambda x: x[1], bigger_tokens_articleid))}

# Calling DataFrame constructor on list
df = pd.DataFrame(dic_term_tokenID)

# group rows based on terms
grouped_df = df.groupby("terms")

# create a list of tokenids on each grouped term as postings list
grouped_lists = grouped_df["tokenid"].apply(list)

# reset index to ease index formation
grouped_lists = grouped_lists.reset_index()

# display(grouped_lists)
sortdf = grouped_lists.sort_values(by=['terms'])
initial_terms = sortdf.terms.tolist()
listofpostlist = [sorted(list(map(int, pl))) for pl in sortdf.tokenid.tolist()]
naive_indexer = dict(zip(initial_terms, listofpostlist))

end_time_naive_indexer = time.time()
print("The time taken for naive indexer is", (end_time_naive_indexer - start_time_naive_indexer) * 1000)

# spimi indexer
start_time_spimi_indexer = time.time()
print("The start time for spimi indexer is", start_time_spimi_indexer)

spimi_indexer = {}

for token_stream in bigger_tokens_articleid:
    if token_stream[0] in spimi_indexer:
        spimi_indexer[token_stream[0]].append(token_stream[1])


    elif token_stream[0] not in spimi_indexer:
        spimi_indexer[token_stream[0]] = []
        spimi_indexer[token_stream[0]].append(token_stream[1])

print(list(spimi_indexer.items())[0])

end_time_spimi_indexer = time.time()
print("The time taken for spimi indexer is", (end_time_spimi_indexer - start_time_spimi_indexer) * 1000)

#suproject2
#Convert your indexer into a probabilistic search engine

#incidence matrix
df['present']=1

incidence_matrix = pd.pivot_table(df, values='present', index=['terms'],columns=['tokenid']).fillna(0)

# for each document calculate RSVd and rank in decreasing order and collect
# experiment with different values for the parameters k1 and b as described in the textbook
# k1>=0
k_one = 10

# 0<=b<=1
b = 0.5


# calculate RSVd
def RSVd(terms, doc):
    rsvd = 0.0
    for term in terms:
        rsvd += (math.log(no_of_documents / len(spimi_indexer[term]))) * (
                    ((k_one + 1) * term_frequency_docs[term][doc]) / (
                        k_one * ((1 - b) + (b * (doc_lengths[doc] / average_doclength))) + term_frequency_docs[term][
                    doc]))

    return rsvd


# Get postings list
def query_process(indexer, single_term):
    # should i handle case sensitivity?
    if single_term in indexer:

        # postings list for term
        #         print(single_term,':', indexer[single_term])
        return indexer[single_term]
    else:
        print("term :" + single_term + ": does not exist in index" + '\n')
        return []


# mini intersection
def inters(lst1, lst2):
    lst3 = [value for value in lst1 if value in lst2]
    return lst3


# AND
def intersection(listofpostingslist):
    results = listofpostingslist[0]

    for postings_list in listofpostingslist:
        results = inters(results, postings_list)

    print("intersect results")
    print(len(results))
    return results


# OR
def union(listofpostingslist, query_terms_df):
    results = []

    for postings_list in listofpostingslist:
        results.extend(postings_list)

    results = sorted(set(results), key=lambda x: len(query_terms_df.loc[query_terms_df[x] == 1]), reverse=True)
    return results


# get search results
def search_engine(ranked, query_type, query):
    print("query name: " + query)
    print('query_type: ' + query_type)
    print('BM25? ' + str(ranked))

    query_terms = query.split()
    dict_postlist = {}

    # query vector
    query_vector = list(np.zeros(len(incidence_matrix)))

    incidence_matrix['query'] = query_vector

    # set terms in query vector
    for term in query_terms:
        incidence_matrix.at[term, 'query'] = 1

    # create a df with just the terms in the query as index
    query_terms_df = incidence_matrix.loc[incidence_matrix['query'] == 1]

    for term in query_terms:
        dict_postlist[term] = query_process(spimi_indexer, term)

    if query_type == " OR ":
        results = union(list(dict_postlist.values()), query_terms_df)

    # how to handle multiple terms? prof says use AND
    elif query_type == " AND " or query_type == " ":
        results = intersection(list(dict_postlist.values()))

    # if ranked is 1 rank by bm25 formula
    if ranked == True:
        doc_rsvd = {}
        for doc in results:
            try:
                doc_query_terms = list(query_terms_df.loc[query_terms_df[doc] == query_terms_df['query']].index)

                doc_rsvd[doc] = RSVd(doc_query_terms, doc)

            # what to do? fixed
            except KeyError:
                print("doc is not in 10000 pairs")

        results = list(map(lambda x: x[0], sorted(doc_rsvd.items(), key=lambda x: x[1], reverse=True)))

    print("final results: ")
    print(results)

#4 test cases
#(a) a single keyword query, to compare with Project 2

# #with naive indexer from project 2
# print('single keyword query with naive indexer from project 2')
# print(query_process(naive_indexer,'Democrats'))
#
#
# #with spimi index
# print('with spimi index')
# search_engine(0," ",'Democrats')
#
# #(b) a query consisting of several keywords for BM25
# search_engine(1," ",'war money control')
# search_engine(1," AND ",'BankAmerica david')
# search_engine(1," OR ",'Pillsbury live')
#
# #(c) a multiple keyword query returning documents containing all the keywords (AND), for unranked Boolean retrieval
# search_engine(0," AND ",'credits debt')
#
# #(d) a multiple keywords query returning documents containing at least one keyword (OR), where documents are ordered by how many keywords they contain), for unranked Boolean retrieval
# search_engine(0," OR ",'Pillsbury Stearns')
#
#
# #part 3 deliverables
#
# #(a) Democrats’ welfare and healthcare reform policies
# #BM25 k_one=10 b=0.5
# search_engine(1," ",'Democrats’ welfare and healthcare reform policies')
#
# #AND
# search_engine(0," AND ",'Democrats’ welfare and healthcare reform policies')
#
# # #OR
# search_engine(0," OR ",'Democrats’ welfare and healthcare reform policies')
#
# # (b) Drug company bankruptcies
# #BM25 k_one=10 b=0.5
# search_engine(1," ",'Drug company bankruptcies')
#
# #AND
# search_engine(0," AND ",'Drug company bankruptcies')
#
# #OR
# search_engine(0," OR ",'Drug company bankruptcies')

# (c) George Bus
#BM25 k_one=10 b=0.5
search_engine(1," ",'George Bush')

# AND
search_engine(0," AND ",'George Bush')

# #OR
search_engine(0," OR ",'George Bush')