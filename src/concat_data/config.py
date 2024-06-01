import os

HOME = "/opt/airflow"
DATA_DIR = os.path.join(HOME, "data")

GENRES = ["Action",
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

# Allociné dictionnary to store the genre mapping

ALLOCINE_GENRE_MAPPING = {
    "Science fiction": "Sci-Fi",
    "Comédie": "Comedy",
    "Drame": "Drama",
    "Comédie dramatique": "Drama",
    "Police": "Crime",
    "Epouvante-horreur": "Horror",
    "Historique": "Documentary",

}

