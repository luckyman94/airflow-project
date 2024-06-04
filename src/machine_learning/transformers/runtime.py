import pandas as pd
from sklearn.base import TransformerMixin, BaseEstimator

DATA_DIR = "/Users/ilan/big-data-airflow-project/data"


class RuntimeTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, column="Runtime"):
        self.column = column

    def fit(self, X, y=None):
        return self

    def transform(self, X):

        X_copy = X.copy()

        def parse_runtime(runtime):
            if 'h' in runtime:
                hours, minutes = runtime.replace('min', '').split('h')
                try:
                    hours = int(hours.strip())
                    minutes = int(minutes.strip())
                    total_hours = hours + minutes / 60
                    if total_hours > 2:
                        return '> 2 hrs'
                    elif total_hours < 0.5:
                        return '< 30 minutes'
                    elif total_hours < 1 and total_hours >= 0.5:
                        return '30 - 60 mins'
                    else:
                        return '1-2 hours'
                except ValueError:
                    return runtime
            return runtime

        X_copy[self.column] = X_copy[self.column].apply(parse_runtime)
        return X_copy