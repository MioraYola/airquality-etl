"""
Charge clean/clean.csv dans le Data Warehouse (schema en etoile).
Rejouable : les dimensions sont chargees en "upsert", la table de faits
est reconstruite entierement a partir de clean.csv a chaque execution.

Necessite la variable d'environnement WAREHOUSE_DB_URL
(format: postgresql://user:password@host:port/dbname)

OPTIMISATION : tous les upserts sont faits en une seule requete groupee
(psycopg2.extras.execute_values) au lieu d'un aller-retour reseau par ligne.
Important car le warehouse est heberge a distance (Supabase, eu-west-1) :
avec des centaines/milliers de lignes, des inserts un par un font
exploser le temps d'execution au fil des runs (clean.csv grossit a
chaque extraction horaire).
"""
import os
import pandas as pd
import psycopg2
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env", override=True)

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


def get_or_create_city(cursor, ville, pays, latitude, longitude):
    cursor.execute(
        """
        INSERT INTO dim_city (ville, pays, latitude, longitude)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (ville, pays) DO UPDATE
        SET latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude
        RETURNING city_id;
        """,
        (ville, pays, latitude, longitude)
    )

    result = cursor.fetchone()

    if result:
        return result[0]

    cursor.execute(
        """
        SELECT city_id
        FROM dim_city
        WHERE ville = %s AND pays = %s;
        """,
        (ville, pays)
    )

    return cursor.fetchone()[0]


def get_or_create_datetime(cursor, timestamp_utc):
    date_only = timestamp_utc.date()
    hour = timestamp_utc.hour
    day = timestamp_utc.day
    month = timestamp_utc.month
    year = timestamp_utc.year
    weekday = timestamp_utc.day_name()

    cursor.execute(
        """
        INSERT INTO dim_datetime (
            timestamp_utc, date_only, hour, day, month, year, weekday
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (timestamp_utc) DO UPDATE
        SET date_only = EXCLUDED.date_only
        RETURNING datetime_id;
        """,
        (
            timestamp_utc.to_pydatetime(),
            date_only,
            hour,
            day,
            month,
            year,
            weekday
        )
    )

    result = cursor.fetchone()

    if result:
        return result[0]

    cursor.execute(
        """
        SELECT datetime_id
        FROM dim_datetime
        WHERE timestamp_utc = %s;
        """,
        (timestamp_utc.to_pydatetime(),)
    )

    return cursor.fetchone()[0]


def insert_fact(cursor, row, city_id, datetime_id):
    cursor.execute(
        """
        INSERT INTO fact_air_quality (
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
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (city_id, datetime_id) DO UPDATE
        SET
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
        (
            city_id,
            datetime_id,
            int(row["aqi"]) if pd.notna(row["aqi"]) else None,
            row["co"] if pd.notna(row["co"]) else None,
            row["no"] if pd.notna(row["no"]) else None,
            row["no2"] if pd.notna(row["no2"]) else None,
            row["o3"] if pd.notna(row["o3"]) else None,
            row["so2"] if pd.notna(row["so2"]) else None,
            row["pm2_5"] if pd.notna(row["pm2_5"]) else None,
            row["pm10"] if pd.notna(row["pm10"]) else None,
            row["nh3"] if pd.notna(row["nh3"]) else None,
        )
    )


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

        total_rows = 0

        for _, row in df.iterrows():
            city_id = get_or_create_city(
                cursor,
                row["ville"],
                row["pays"],
                row["latitude"],
                row["longitude"]
            )

            datetime_id = get_or_create_datetime(
                cursor,
                row["timestamp_utc"]
            )

            insert_fact(cursor, row, city_id, datetime_id)

            total_rows += 1

        conn.commit()
        cursor.close()

        print(f"Chargement termine : {total_rows} lignes traitees.")
        print("Data warehouse mis a jour avec succes.")

    except Exception as e:
        conn.rollback()
        raise e

    finally:
        conn.close()


if __name__ == "__main__":
    run_load()