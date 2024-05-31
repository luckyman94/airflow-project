import logging
import os

import requests
from bs4 import BeautifulSoup
import pandas as pd
from . import config


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

def scrap_allocine(genre_name, genre_code, num_pages):

    titles = []
    durations = []
    genres = []
    release_dates = []
    directors = []
    actors = []
    press_ratings = []
    spectator_ratings = []
    synopses = []

    df = pd.DataFrame()

    for page in range(1, num_pages + 1):
        url = f"https://www.allocine.fr/film/meilleurs/genre-{genre_code}/?page={page}"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            print(f"Request ok {genre_name}, page {page}")
        else:
            print(f"Request failed for {genre_name}, page {page}")
            continue

        soup = BeautifulSoup(response.content, 'html.parser')
        if soup is not None:
            movie_containers = soup.find_all('div', class_='card entity-card entity-card-list cf')
            for container in movie_containers:


                title = container.h2.a.text.strip()
                titles.append(title)

                duration = container.find('div', class_='meta-body-item meta-body-info').contents[0].strip()
                durations.append(duration)

                genres.append(genre_name)

                release_date = container.find('span', class_='date').text if container.find('span',class_='date') else 'N/A'
                release_dates.append(release_date)

                try :
                    director = container.find('div', class_='meta-body-item meta-body-direction').find('span',class_='dark-grey-link').text
                except:
                    director = None

                directors.append(director)

                try:
                    actor_list = container.find('div', class_='meta-body-item meta-body-actor').find_all('a')
                    actor_names = ', '.join([actor.text for actor in actor_list])
                except:
                    actor_names = []

                actors.append(actor_names)

                try :
                    press_rating = container.find('div', class_='rating-item').find('span',class_='stareval-note').text if container.find(
                        'div', class_='rating-item').find('span', class_='stareval-note') else 'N/A'
                except:
                    press_rating = ""

                press_ratings.append(press_rating)

                try :
                    spectator_rating = container.find_all('div', class_='rating-item')[1].find('span', class_='stareval-note').text \
                        if len(container.find_all('div', class_='rating-item')) > 1 and container.find_all('div', class_='rating-item')[1].find('span', class_='stareval-note') else 'N/A'
                except:
                    spectator_rating = ""

                spectator_ratings.append(spectator_rating)

                synopsis = container.find('div', class_='content-txt').text.strip() if container.find('div',class_='content-txt') else 'N/A'
                synopses.append(synopsis)

                df = pd.DataFrame({
                    'Title': titles,
                    'Duration': durations,
                    'Genre': genres,
                    'Release Date': release_dates,
                    'Director': directors,
                    'Actors': actors,
                    'Press Rating': press_ratings,
                    'Spectator Rating': spectator_ratings,
                    'Synopsis': synopses
                })

    return df


def run_scrap_allocine(num_pages):
    logging.info("Starting to scrap data...")
    local_directory = f"data/allocine/"
    filename = 'allocine_movies.csv'
    file_path = os.path.join(local_directory, filename)
    os.makedirs(local_directory, exist_ok=True)

    df = pd.DataFrame()
    for genre_name, genre_code in config.genres_dict.items():
        genre_df = scrap_allocine(genre_name, genre_code, num_pages)
        df = pd.concat([df, genre_df], ignore_index=True)

    df.to_csv(file_path, index=False)

