import pandas as pd
from sklearn.base import TransformerMixin, BaseEstimator
from sklearn.pipeline import Pipeline


from src.machine_learning.transformers.drop import DropColumn, DropDuplicates
from src.machine_learning.transformers.filter_type import FilterTypeTransformer
from src.machine_learning.transformers.genre import GenreTransformer, GenreRenameTransformer
from src.machine_learning.transformers.missing_values import MissingValuesTransformer
from src.machine_learning.transformers.ohe import OneHotEncoderTransformer
from src.machine_learning.transformers.runtime import RuntimeTransformer
from src.machine_learning.transformers.tfidf import TFIDFTransformer


class PipelineTransformer(BaseEstimator, TransformerMixin):
    def __init__(self):
        self.steps = [
            ('drop_duplicated', DropDuplicates(columns=["Title"])),
            ('missing_values', MissingValuesTransformer()),
            ('runtime', RuntimeTransformer()),
            ('genre_renamer', GenreRenameTransformer()),
            ('genre', GenreTransformer()),
            ('drop_columns', DropColumn(cols=["Genre"])),
            ('filter_type', FilterTypeTransformer()),
            ('ohe', OneHotEncoderTransformer(columns=["Runtime", "Type"])),
            ('tfidf', TFIDFTransformer(column="Summary", max_features=1000, n_components=50)),

        ]
        self.pipeline = Pipeline(self.steps)

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        return self.pipeline.fit_transform(X)

    def get_tfidf_matrix(self):
        return self.pipeline.named_steps['tfidf'].get_tfidf_matrix()





