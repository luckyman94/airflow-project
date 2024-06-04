from sklearn.base import TransformerMixin, BaseEstimator


class MissingValuesTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, columns=None):
        self.columns = columns

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X_copy = X.copy()
        if self.columns is None:
            return X_copy.dropna()
        else:
            return X_copy.dropna(subset=self.columns)