"""
Charge clean/clean.csv dans le Data Warehouse (schema en etoile).
Rejouable : les dimensions sont chargees en "upsert", la table de faits
est reconstruite entierement a partir de clean.csv a chaque execution.


OPTIMISATION : 
- Les dimensions sont chargées en batch avec execute_values()
- Les IDs sont récupérés en une seule requête
- Les faits sont insérés en batch

Adapté pour un warehouse distant (Supabase PostgreSQL).
"""
import os
import pandas as pd
import psycopg2
from pathlib import Path
from dotenv import load_dotenv
from psycopg2.extras import execute_values

BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env")

CLEAN_FILE = Path(
    os.environ.get(
        "CLEAN_FILE",
        BASE_DIR / "clean" / "clean.csv"
    )
)

DB_URL = os.environ.get("WAREHOUSE_DB_URL")

def get_connection():
    if not DB_URL:
        raise RuntimeError(
            "WAREHOUSE_DB_URL n'est pas définie dans les variables d'environnement"
        )

    return psycopg2.connect(DB_URL)


def load_cities(cursor, df):

    cities = (
        df[
            [
                "ville",
                "pays",
                "latitude",
                "longitude"
            ]
        ]
        .drop_duplicates()
    )


    execute_values(
        cursor,
        """
        INSERT INTO dim_city
        (
            ville,
            pays,
            latitude,
            longitude
        )
        VALUES %s

        ON CONFLICT(ville,pays)
        DO UPDATE SET
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude;
        """,

        cities.values.tolist()
    )


    cursor.execute(
        """
        SELECT
            city_id,
            ville,
            pays
        FROM dim_city;
        """
    )


    rows = cursor.fetchall()


    city_map = {
        (ville,pays): city_id
        for city_id,ville,pays in rows
    }


    return city_map



def load_datetimes(cursor, df):

    timestamps = (
        df["timestamp_utc"]
        .drop_duplicates()
    )


    data = []


    for ts in timestamps:

        data.append(
            (
                ts.to_pydatetime(),
                ts.date(),
                ts.hour,
                ts.day,
                ts.month,
                ts.year,
                ts.day_name()
            )
        )


    execute_values(
        cursor,

        """
        INSERT INTO dim_datetime
        (
            timestamp_utc,
            date_only,
            hour,
            day,
            month,
            year,
            weekday
        )

        VALUES %s

        ON CONFLICT(timestamp_utc)
        DO NOTHING;
        """,

        data
    )


    cursor.execute(
        """
        SELECT
            datetime_id,
            timestamp_utc
        FROM dim_datetime;
        """
    )


    rows = cursor.fetchall()


    datetime_map = {
        timestamp: datetime_id
        for datetime_id,timestamp in rows
    }


    return datetime_map


def load_facts(cursor, df, city_map, datetime_map):


    facts = []


    for _, row in df.iterrows():

        city_id = city_map[
            (
                row["ville"],
                row["pays"]
            )
        ]


        datetime_id = datetime_map[
            row["timestamp_utc"].to_pydatetime()
        ]


        facts.append(
            (
                city_id,
                datetime_id,

                row["aqi"],
                row["co"],
                row["no"],
                row["no2"],
                row["o3"],
                row["so2"],
                row["pm2_5"],
                row["pm10"],
                row["nh3"]
            )
        )


    execute_values(
        cursor,

        """
        INSERT INTO fact_air_quality
        (
            city_id,
            datetime_id,
            aqi,
            co,
            no,
            no2,
            o3,
            so2,
            pm2_5,
            pm10,
            nh3
        )

        VALUES %s


        ON CONFLICT(city_id,datetime_id)

        DO UPDATE SET

            aqi = EXCLUDED.aqi,
            co = EXCLUDED.co,
            no = EXCLUDED.no,
            no2 = EXCLUDED.no2,
            o3 = EXCLUDED.o3,
            so2 = EXCLUDED.so2,
            pm2_5 = EXCLUDED.pm2_5,
            pm10 = EXCLUDED.pm10,
            nh3 = EXCLUDED.nh3;

        """,

        facts
    )


    return len(facts)


def run_load():
    if not CLEAN_FILE.exists():
        raise FileNotFoundError(f"Fichier clean introuvable : {CLEAN_FILE}")

    df = pd.read_csv(CLEAN_FILE)

    required_columns = [
        "ville", "pays", "latitude", "longitude", "timestamp_utc",
        "aqi", "co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3"
    ]

    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise RuntimeError(f"Colonnes manquantes dans clean.csv : {missing_columns}")

    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)

    conn = get_connection()

    try:
        cursor = conn.cursor()

        print("Chargement dim_city...")
        city_map = load_cities(cursor,df)

        print("Chargement dim_datetime...")
        datetime_map = load_datetimes(cursor,df)

        print("Chargement fact_air_quality...")
        total = load_facts(
            cursor,
            df,
            city_map,
            datetime_map
        )

        conn.commit()
        cursor.close()

        print(f"Chargement termine : {total} lignes traitees.")
        print("Data warehouse mis a jour avec succes.")

    except Exception as e:
        conn.rollback()
        raise e

    finally:
        conn.close()


if __name__ == "__main__":
    run_load()