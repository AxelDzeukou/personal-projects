									#to get the soup
#for project
soup=spidy.get('https://www.concordia.ca/ginacody.html')


									#to get all a tags in html
a_lists=soup.css('a')


									#turned text into list of tokens
#return a combination of all <p> tags in a web page and a list of <token,webpage> pairs
def combine_texts_per_page(i,page_texts):
    page_tokens=[]
    for text in page_texts:
        page_tokens.extend(word_tokenize(text))
    
    return list(map(lambda x: [x,i],list(set(page_tokens)))),' '.join(page_tokens)


									#create k_means model 
#k=3
kmeans = KMeans(n_clusters=3, random_state=0).fit(X)

#k=6
kmeans = KMeans(n_clusters=6, random_state=0).fit(X)


									#calculate affinn score
for page in clustered_pages:
            affinn_score+=afinn.score(page_text_dic[page])