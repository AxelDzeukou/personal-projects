# scrape html links
# get text with soup
import webspidy as spidy
from nltk.tokenize import word_tokenize
import pandas as pd


# check if the href is a webpage
def check_html(link):
    if ".html" in link:
        return True

    return False


# return a combination of all <p> tags in a web page and a list of <token,webpage> pairs
def combine_texts_per_page(i, page_texts):
    page_tokens = []
    for text in page_texts:
        page_tokens.extend(word_tokenize(text))

    return list(map(lambda x: [x, i], list(set(page_tokens)))), ' '.join(page_tokens)


# list of <token,webpage> pairs
text_page_pairs = []

# dictionary with webpage as key and combined text as value
page_text_dic = {}

# for project
soup = spidy.get('https://www.concordia.ca/ginacody.html')

# to help complete the actual link to webpage
webpage_beginning = 'https://www.concordia.ca'
# /for project

# for demo
# uncomment to set soup for demo webpage'https://www.concordia.ca/campus-life.html'
# soup=spidy.get('https://www.concordia.ca/campus-life.html')
# /for demo


a_lists = soup.css('a')
print(len(list(a_lists)))

# scrape only links to webpages
for i, a_class in enumerate(a_lists):
    try:
        href = a_class.attr('href')
        if check_html(href) == True:
            if 'https' not in href:
                html_link = webpage_beginning + href

            elif 'https' in href:
                html_link = href

            soup = spidy.get(html_link)
            results = combine_texts_per_page(i, soup.css("p").text())
            text_page_pairs.extend(results[0])
            page_text_dic[i] = results[1]

    except KeyError:
        print("href unavailable")


#create incidence matrix
import pandas as pd

dic_term_page = {'term':list(map(lambda x:x[0],text_page_pairs)), 'page':list(map(lambda x:x[1],text_page_pairs))}

# Calling DataFrame constructor on dictionary
df = pd.DataFrame(dic_term_page)

#incidence matrix with term as index, webpage as column, and present as value in order to get vectors
df['present']=1

incidence_matrix = pd.pivot_table(df, values='present', index=['term'],columns=['page']).fillna(0)

print(incidence_matrix)

#run kmeans clustering  for 3 and 6
from sklearn.cluster import KMeans
import numpy as np

#list of <no of clusters,labels after clustering> pairs
k_means_set=[[3,[]],[6,[]]]

X = pd.pivot_table(df, values='present', index=['page'],columns=['term']).fillna(0)

#k=3
kmeans = KMeans(n_clusters=3, random_state=0).fit(X)
print(kmeans.labels_)
k_means_set[0][1]=kmeans.labels_

#k=6
kmeans = KMeans(n_clusters=6, random_state=0).fit(X)
print(kmeans.labels_)
k_means_set[1][1]=kmeans.labels_

# run affinn analysis for each cluster
from afinn import Afinn
import math

afinn = Afinn()


# get index positions of webpages that belong to particular cluster/label to calculate afinn score
def get_index_positions(list_of_elems, element):
    ''' Returns the indexes of all occurrences of give element in
    the list- listOfElements '''
    index_pos_list = []
    index_pos = 0
    while True:
        try:
            # Search for item in list from indexPos to the end of list
            index_pos = list_of_elems.index(element, index_pos)
            # Add the index position in list
            index_pos_list.append(index_pos)
            index_pos += 1
        except ValueError as e:
            break
    return index_pos_list


# for each type of k means with different number of cluster print the afinn score for each cluster
for k_means in k_means_set:
    print("for " + str(k_means[0]) + " clusters" + '\n')
    labels_set = list(set(k_means[1]))

    for label in labels_set:

        label_indices = get_index_positions(list(k_means[1]), label)

        # pages in cluster
        clustered_pages = list(incidence_matrix.iloc[:, label_indices].columns)

        affinn_score = 0

        for page in clustered_pages:
            affinn_score += afinn.score(page_text_dic[page])

        print("for cluster " + str(label) + " the affinn_score is " + str(affinn_score))

        # to measure top 20 terms based on informativeness we need to create indexer for documents in each cluster then rank
        text_page_pairs_forCluster = list(filter(lambda x: x[1] in clustered_pages, text_page_pairs))
        spimi_indexer = {}

        for token_stream in text_page_pairs_forCluster:
            if token_stream[0] in spimi_indexer:
                spimi_indexer[token_stream[0]].append(token_stream[1])


            elif token_stream[0] not in spimi_indexer:
                spimi_indexer[token_stream[0]] = []
                spimi_indexer[token_stream[0]].append(token_stream[1])

        informativeness_results = list(spimi_indexer.items())
        informativeness_results = list(map(lambda x: x[0], sorted(informativeness_results, key=lambda x: math.log(
            len(clustered_pages) / len(x[1])), reverse=True)))
        print("for cluster " + str(label) + " the top 20 terms based on informativeness is ")
        print(informativeness_results[:20])
        print('\n')
