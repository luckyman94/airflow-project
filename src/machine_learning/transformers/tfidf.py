import pandas as pd
from sklearn.base import TransformerMixin, BaseEstimator
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
import nltk


DATA_DIR = "/Users/ilan/big-data-airflow-project/data/"
class TFIDFTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, column, max_features, n_components):
        self.column = column
        self.max_features = max_features
        self.n_components = n_components
        self.vectorizer = TfidfVectorizer(max_features=max_features, stop_words='english')
        self.svd = TruncatedSVD(n_components=n_components)
        self.tfidf_matrix = None

    def fit(self, X, y=None):
        vectors = self.vectorizer.fit_transform(X[self.column])
        self.svd.fit(vectors)
        return self

    def transform(self, X):
        vectors = self.vectorizer.transform(X[self.column])
        svd_result = self.svd.transform(vectors)
        self.tfidf_matrix = svd_result
        tfidf_df = pd.DataFrame(self.tfidf_matrix,
                                columns=[f"{self.column}_tfidf_{i}" for i in range(self.n_components)])
        X = pd.concat([X.reset_index(drop=True), tfidf_df.reset_index(drop=True)], axis=1)
        return X

    def get_tfidf_matrix(self):
        return self.tfidf_matrix


