import sys
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer

# Check if at least six arguments are provided (including the script name)
number_of_books = 6
books_path = 'books/'
book_titles = [input() for _ in range(number_of_books)]
corpus = []
for book_title in book_titles:
  with open(books_path + book_title + ".txt", "r") as file: 
    corpus.append(file.read())

corpus = np.array(corpus)
count_vectorizer = CountVectorizer(min_df=0.0, max_df=1.0)
count_vectorizer.fit_transform(corpus)
vocabulary_list = list(count_vectorizer.get_feature_names_out())
with open('vocabulary.csv','w') as file:
   file.write(','.join(vocabulary_list))

