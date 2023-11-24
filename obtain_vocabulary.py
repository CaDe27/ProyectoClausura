import sys
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer

# Check if at least six arguments are provided (including the script name)
book_titles = [input() for _ in range(6)]
corpus = []
for book_title in book_titles:
  with open(book_title + ".txt", "r") as file: 
    corpus.append(file.read())

corpus = np.array(corpus)
count_vectorizer = CountVectorizer(min_df=0.0, max_df=1.0)
count_vectorizer.fit_transform(corpus)
vocabulary_list = list(count_vectorizer.get_feature_names_out())
with open('vocabulary.csv','w') as file:
   file.write(f'{len(vocabulary_list)}\n')
   file.write(', '.join(vocabulary_list))

