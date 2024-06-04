from sklearn.preprocessing import OneHotEncoder
import pandas as pd

class OneHotEncoderTransformer:
    def __init__(self, columns):
        self.columns = columns
        self.encoder = OneHotEncoder(handle_unknown='ignore', sparse_output=False)

    def fit(self, X, y=None):
        self.encoder.fit(X[self.columns])
        return self

    def transform(self, X):
        encoded = self.encoder.transform(X[self.columns])
        new_columns = self.encoder.get_feature_names_out(self.columns)
        encoded_df = pd.DataFrame(encoded, columns=new_columns, index=X.index)
        return pd.concat([X.drop(columns=self.columns), encoded_df], axis=1)

    def fit_transform(self, X, y=None):
        self.fit(X, y)
        return self.transform(X)
