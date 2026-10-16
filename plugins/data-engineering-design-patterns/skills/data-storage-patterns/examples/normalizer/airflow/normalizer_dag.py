"""Normalizer - Airflow 2.x TaskFlow + PostgreSQL in third normal form.

Loads a flat staging table of game releases into 3NF tables:
  studios(name PK, country)                 <- fixes the 3NF transitive dependency
  platforms(name PK, language)              <- fixes the 2NF partial dependency
  games(name PK, studio FK)
  game_releases(game FK, platform FK, release_year, PK(game, platform))
  game_comments(game FK, comment)           <- fixes 1NF repeating groups (array column)
Entities load before the tables referencing them so FK (integrity) constraints hold.

Run: put in dags/; connection `dp_postgres` via env. Requires the Postgres provider.
"""
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "dp_postgres"

DDL = """
CREATE TABLE IF NOT EXISTS dp.studios (name TEXT PRIMARY KEY, country TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS dp.platforms (name TEXT PRIMARY KEY, language TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS dp.games (
    name TEXT PRIMARY KEY, studio TEXT NOT NULL REFERENCES dp.studios(name));
CREATE TABLE IF NOT EXISTS dp.game_releases (
    game TEXT REFERENCES dp.games(name), platform TEXT REFERENCES dp.platforms(name),
    release_year INT NOT NULL, PRIMARY KEY (game, platform));
CREATE TABLE IF NOT EXISTS dp.game_comments (
    game TEXT REFERENCES dp.games(name), comment TEXT NOT NULL);
"""


def run(sql: str) -> None:
    PostgresHook(postgres_conn_id=CONN_ID).run(sql)


@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False,
     tags=["dp", "storage", "normalizer"])
def normalizer():

    @task
    def create_schema():
        run(DDL)

    @task
    def load_studios():
        run("""INSERT INTO dp.studios SELECT DISTINCT studio, studio_country FROM dp.games_staging
               ON CONFLICT (name) DO UPDATE SET country = EXCLUDED.country""")

    @task
    def load_platforms():
        run("""INSERT INTO dp.platforms SELECT DISTINCT platform, platform_language FROM dp.games_staging
               ON CONFLICT (name) DO UPDATE SET language = EXCLUDED.language""")

    @task
    def load_games():
        run("""INSERT INTO dp.games SELECT DISTINCT name, studio FROM dp.games_staging
               ON CONFLICT (name) DO UPDATE SET studio = EXCLUDED.studio""")

    @task
    def load_releases_and_comments():
        run("""
        INSERT INTO dp.game_releases SELECT DISTINCT name, platform, release_year FROM dp.games_staging
        ON CONFLICT (game, platform) DO UPDATE SET release_year = EXCLUDED.release_year;
        DELETE FROM dp.game_comments WHERE game IN (SELECT name FROM dp.games_staging);
        INSERT INTO dp.game_comments
        SELECT DISTINCT name, UNNEST(comments) FROM dp.games_staging;""")

    create_schema() >> [load_studios(), load_platforms()] >> load_games() >> load_releases_and_comments()


normalizer()
