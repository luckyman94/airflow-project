from sklearn.base import TransformerMixin, BaseEstimator


class FilterTypeTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, column="Type", allowed_types=None):
        self.column = column

        if allowed_types is None:
            self.allowed_types = ["Movie", "Series"]
        else:
            self.allowed_types = allowed_types

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X_copy = X.copy()
        X_copy = X_copy[X_copy[self.column].isin(self.allowed_types)]
        return X_copy