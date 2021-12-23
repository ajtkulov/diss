from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.pipeline import Pipeline, FeatureUnion
from sklearn.metrics import accuracy_score
import pickle
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer

trainPd = pd.read_csv('pd.txt.R', sep='\t', names=[0, 1])



# word_vectorizer = TfidfVectorizer(
#     analyzer='word',
#     stop_words='english',
#     ngram_range=(1, 3),
#     lowercase=True,
#     min_df=5,
#     max_features=30000)

char_vectorizer = TfidfVectorizer(
    analyzer='char',
#     stop_words='english',
    ngram_range=(3, 4),
    lowercase=True,
    min_df=5,
    max_features=50000)


log_reg = LogisticRegression(solver='liblinear', random_state=42)

# fu = FeatureUnion([('word_vectorizer', word_vectorizer),  ('char_vectorizer', char_vectorizer)])
fu = FeatureUnion([('char_vectorizer', char_vectorizer)])
model = Pipeline([('vectorizers', fu),  ('log_reg', log_reg)])

X, y = trainPd[0], trainPd[1]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

model.fit(X_train, y_train)

pickle.dump( model, open("model.logreg", "wb" ) )



