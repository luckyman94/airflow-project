import pandas as pd
from sklearn.base import TransformerMixin, BaseEstimator

DATA_DIR = "/Users/ilan/big-data-airflow-project/data"

class GenreTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, column="Genre"):
        self.genres = [
            "Action",
            "Adventure",
            "Animation",
            "Biopic",
            "Comedy",
            "Crime",
            "Documentary",
            "Drama",
            "Horror",
            "Famille",
            "Guerre",
            "Musical"
            "Romance",
            "Sci-Fi",
            "Thriller",
            "Western"
        ]

        self.column = column


    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X_copy = X.copy()
        X_copy[self.column] = X_copy[self.column].str.split(", ")

        for genre in self.genres:
            X_copy[genre] = X_copy[self.column].apply(lambda genres: int(genre in genres if genres else 0))

        return X_copy




from sklearn.base import TransformerMixin, BaseEstimator
import pandas as pd

class GenreRenameTransformer(BaseEstimator, TransformerMixin):
    def __init__(self):
        self.column = "Genre"
        self.map = {
            "Science fiction": "Sci-Fi",
            "Comédie": "Comedy",
            "Drame": "Drama",
            "Comédie dramatique": "Drama",
            "Police": "Crime",
            "Epouvante-horreur": "Horror",
            "Historique": "Documentary",
        }

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        if not isinstance(X, pd.DataFrame):
            raise TypeError("Input must be a pandas DataFrame")

        for column in X.columns:
            if X[column].dtype == 'object':
                X[column] = X[column].map(self.map).fillna(X[column])
        return X


