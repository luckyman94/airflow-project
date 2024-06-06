import pandas as pd
from sklearn.metrics.pairwise import linear_kernel


from src.machine_learning.pipeline import PipelineTransformer
from src.utils.s3_manager import S3Manager



class RecommendationModel:
    def __init__(self, df):
        self.pipeline = PipelineTransformer()
        self.runtime = df["Runtime"]
        self.genre = df["Genre"]
        self.type = df["Type"]
        self.train = self.pipeline.fit_transform(df)
        self.tfidf_matrix = self.pipeline.get_tfidf_matrix()
        self.indices = pd.Series(range(len(self.train)), index=self.train['Title']).drop_duplicates()
        self.cosine_sim = linear_kernel(self.tfidf_matrix, self.tfidf_matrix)
        self.s3_manager = S3Manager()

    def get_recommendations(self, title):
        idx = self.indices.get(title, None)
        if idx is not None:
            sim_scores = list(enumerate(self.cosine_sim[idx]))
            sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
            sim_scores = sim_scores[
                         1:11]
            movie_indices = [i[0] for i in sim_scores]
            recommendations = self.train.iloc[movie_indices][['Title', 'Actors', 'Rating', 'Summary']]

            recommendations['Genre'] = self.genre.iloc[movie_indices].values
            recommendations['Runtime'] = self.runtime.iloc[movie_indices].values
            recommendations['Type'] = self.type.iloc[movie_indices].values

            return recommendations.to_dict('records')

    def generate_recommendations_df(self, titles, export=True):
        recommendations = []
        for title in titles:
            recommended_titles = self.get_recommendations(title)
            for rec in recommended_titles:
                recommendations.append({
                    'input_title': title,
                    'recommended_title': rec
                })

        if export:
            df = pd.DataFrame(recommendations)
            data_folder = "/opt/airflow/src/indexing/"
            df.to_parquet(data_folder +"recommendations.parquet", index=False)
            self.s3_manager.upload_file('recommendations.parquet', data_folder +"recommendations.parquet")

        return pd.DataFrame(recommendations)


