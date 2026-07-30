"""
Backfill historique de la qualité de l'air
(OpenWeatherMap Air Pollution History API).

Le script récupère les données horaires pour chaque ville
et chaque journée.

Principes :
- une journée complète contient normalement 24 observations ;
- une journée partielle (ex. 23/24) est conservée avec un avertissement ;
- une journée avec 0 observation est considérée comme une erreur ;
- les timestamps dupliqués sont refusés ;
- les dates sont traitées en UTC ;
- un fichier RAW est créé par ville et par jour ;
- --start et --end sont inclusifs.

Exemples :

    py scripts/backfill.py --months 3

    py scripts/backfill.py \
        --start 2026-05-01 \
        --end 2026-07-29
"""

import os
import json
import time
import argparse
import requests

from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone


# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[1]

load_dotenv(
    BASE_DIR / ".env",
    override=True
)

RAW_DIR = Path(
    os.environ.get(
        "RAW_DIR",
        BASE_DIR / "raw"
    )
)

CONFIG_PATH = Path(
    os.environ.get(
        "CITIES_CONFIG",
        BASE_DIR / "config" / "cities.json"
    )
)

API_KEY = os.environ.get("OWM_API_KEY")

BASE_URL = (
    "https://api.openweathermap.org/"
    "data/2.5/air_pollution/history"
)

# Limitation API
REQUEST_DELAY_SECONDS = 1.1

# Retry en cas d'erreur réseau ou réponse inutilisable
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5

# Nombre théorique d'heures dans une journée
EXPECTED_OBSERVATIONS_PER_DAY = 24


# ============================================================
# CHARGEMENT DES VILLES
# ============================================================

def load_cities():
    """
    Charge les villes depuis config/cities.json.
    """

    with open(
        CONFIG_PATH,
        "r",
        encoding="utf-8"
    ) as f:
        return json.load(f)


# ============================================================
# VALIDATION DE LA RÉPONSE
# ============================================================

def validate_day_payload(
    payload,
    day_start,
    day_end
):
    """
    Vérifie la cohérence des données reçues.

    Une journée partielle est acceptée.

    Refus uniquement si :
    - aucune observation ;
    - observation sans timestamp ;
    - timestamps dupliqués ;
    - timestamp hors de la journée demandée.

    Retourne le nombre d'observations reçues.
    """

    observations = payload.get("list", [])

    # --------------------------------------------------------
    # Aucune donnée
    # --------------------------------------------------------

    if len(observations) == 0:
        raise ValueError(
            "Aucune observation reçue pour cette journée"
        )

    # --------------------------------------------------------
    # Timestamps
    # --------------------------------------------------------

    timestamps = []

    for item in observations:

        if "dt" not in item:
            raise ValueError(
                "Une observation ne possède pas de timestamp 'dt'"
            )

        timestamps.append(item["dt"])

    # --------------------------------------------------------
    # Doublons
    # --------------------------------------------------------

    if len(timestamps) != len(set(timestamps)):
        raise ValueError(
            "Timestamps dupliqués dans la réponse API"
        )

    # --------------------------------------------------------
    # Vérification de la période
    # --------------------------------------------------------

    start_ts = int(day_start.timestamp())
    end_ts = int(day_end.timestamp())

    invalid_timestamps = [
        ts
        for ts in timestamps
        if ts < start_ts or ts > end_ts
    ]

    if invalid_timestamps:
        raise ValueError(
            f"{len(invalid_timestamps)} observation(s) "
            f"hors de la journée demandée"
        )

    # --------------------------------------------------------
    # Journée partielle
    # --------------------------------------------------------

    count = len(observations)

    if count < EXPECTED_OBSERVATIONS_PER_DAY:

        print(
            f"  [WARN] Journée incomplète : "
            f"{count}/{EXPECTED_OBSERVATIONS_PER_DAY} "
            f"observations disponibles"
        )

    elif count > EXPECTED_OBSERVATIONS_PER_DAY:

        print(
            f"  [WARN] Nombre inhabituel : "
            f"{count} observations reçues"
        )

    return count


# ============================================================
# APPEL API
# ============================================================

def fetch_day(
    lat,
    lon,
    day_start,
    day_end
):
    """
    Récupère les données pour une journée.

    Retry en cas de :
    - timeout ;
    - problème réseau ;
    - réponse sans données ;
    - données incohérentes.
    """

    last_error = None

    for attempt in range(
        1,
        MAX_RETRIES + 1
    ):

        try:

            params = {
                "lat": lat,
                "lon": lon,
                "start": int(
                    day_start.timestamp()
                ),
                "end": int(
                    day_end.timestamp()
                ),
                "appid": API_KEY,
            }

            response = requests.get(
                BASE_URL,
                params=params,
                timeout=20
            )

            response.raise_for_status()

            payload = response.json()

            observation_count = validate_day_payload(
                payload,
                day_start,
                day_end
            )

            return payload, observation_count

        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
            ValueError
        ) as e:

            last_error = e

            if attempt < MAX_RETRIES:

                wait = (
                    RETRY_BACKOFF_SECONDS
                    * attempt
                )

                print(
                    f"  [RETRY {attempt}/{MAX_RETRIES}] "
                    f"{e}"
                )

                print(
                    f"  Nouvelle tentative dans {wait}s..."
                )

                time.sleep(wait)

        except requests.exceptions.HTTPError:
            # 401, 403, 429, etc.
            raise

    raise last_error


# ============================================================
# SAUVEGARDE RAW
# ============================================================

def save_raw(
    ville,
    day,
    payload
):
    """
    Enregistre un JSON par ville et par journée.

    Exemple :
    raw/Paris/Paris_history_2026-05-01.json
    """

    ville_dir = (
        RAW_DIR
        / ville.replace(" ", "_")
    )

    ville_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    filename = (
        f"{ville.replace(' ', '_')}"
        f"_history_"
        f"{day.strftime('%Y-%m-%d')}"
        f".json"
    )

    filepath = (
        ville_dir
        / filename
    )

    with open(
        filepath,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            payload,
            f,
            ensure_ascii=False,
            indent=2
        )

    return filepath


# ============================================================
# GÉNÉRATION DES JOURS
# ============================================================

def daterange(
    start,
    end
):
    """
    Génère toutes les journées entre start et end inclusivement.
    """

    current = start

    while current <= end:

        yield current

        current += timedelta(days=1)


# ============================================================
# BACKFILL PRINCIPAL
# ============================================================

def run_backfill(
    start_date,
    end_date
):

    if not API_KEY:

        raise RuntimeError(
            "OWM_API_KEY n'est pas défini "
            "dans l'environnement"
        )

    cities = load_cities()

    total_files = 0
    total_errors = 0
    total_observations = 0

    incomplete_days = []
    failed_days = []

    for city in cities:

        ville = city["ville"]

        print(
            "\n========================================"
        )

        print(
            f"Ville : {ville}"
        )

        print(
            "========================================"
        )

        for day in daterange(
            start_date,
            end_date
        ):

            # ------------------------------------------------
            # Début de la journée : 00:00:00 UTC
            # ------------------------------------------------

            day_start = day.replace(
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
                tzinfo=timezone.utc
            )

            # ------------------------------------------------
            # Fin : 23:59:59 UTC
            # ------------------------------------------------

            day_end = (
                day_start
                + timedelta(days=1)
                - timedelta(seconds=1)
            )

            try:

                (
                    data,
                    observation_count
                ) = fetch_day(
                    city["lat"],
                    city["lon"],
                    day_start,
                    day_end
                )

                # --------------------------------------------
                # Métadonnées
                # --------------------------------------------

                data["_meta"] = {
                    "ville": ville,
                    "pays": city["pays"],
                    "lat": city["lat"],
                    "lon": city["lon"],
                    "date_backfill": (
                        day.strftime("%Y-%m-%d")
                    ),
                    "timezone": "UTC",
                    "observations": observation_count
                }

                # --------------------------------------------
                # Sauvegarde RAW
                # --------------------------------------------

                path = save_raw(
                    ville,
                    day,
                    data
                )

                total_files += 1
                total_observations += observation_count

                # --------------------------------------------
                # Journée incomplète
                # --------------------------------------------

                if (
                    observation_count
                    < EXPECTED_OBSERVATIONS_PER_DAY
                ):

                    incomplete_days.append(
                        (
                            ville,
                            day.strftime("%Y-%m-%d"),
                            observation_count
                        )
                    )

                    print(
                        f"[OK PARTIEL] "
                        f"{ville} "
                        f"{day.strftime('%Y-%m-%d')} "
                        f"-> {observation_count}/24 observations"
                    )

                else:

                    print(
                        f"[OK] "
                        f"{ville} "
                        f"{day.strftime('%Y-%m-%d')} "
                        f"-> {observation_count} observations"
                    )

            except Exception as e:

                total_errors += 1

                failed_days.append(
                    (
                        ville,
                        day.strftime("%Y-%m-%d"),
                        str(e)
                    )
                )

                print(
                    f"[ERREUR] "
                    f"{ville} "
                    f"{day.strftime('%Y-%m-%d')} "
                    f": {e}"
                )

            finally:

                time.sleep(
                    REQUEST_DELAY_SECONDS
                )

    # ========================================================
    # RÉSUMÉ
    # ========================================================

    print(
        "\n========================================"
    )

    print(
        "RÉSUMÉ DU BACKFILL"
    )

    print(
        "========================================"
    )

    print(
        f"Fichiers écrits       : {total_files}"
    )

    print(
        f"Observations récupérées : {total_observations}"
    )

    print(
        f"Jours incomplets      : {len(incomplete_days)}"
    )

    print(
        f"Jours en erreur       : {total_errors}"
    )

    # --------------------------------------------------------
    # Journées incomplètes
    # --------------------------------------------------------

    if incomplete_days:

        print(
            "\nJournées incomplètes conservées :"
        )

        for (
            ville,
            jour,
            count
        ) in incomplete_days:

            print(
                f"  - {ville} "
                f"{jour} : "
                f"{count}/24 observations"
            )

    # --------------------------------------------------------
    # Journées réellement en erreur
    # --------------------------------------------------------

    if failed_days:

        print(
            "\nJournées non récupérées :"
        )

        for (
            ville,
            jour,
            erreur
        ) in failed_days:

            print(
                f"  - {ville} "
                f"{jour} : "
                f"{erreur}"
            )

    if not failed_days:

        print(
            "\nAucune journée totalement absente."
        )


# ============================================================
# PROGRAMME PRINCIPAL
# ============================================================

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description=(
            "Backfill historique "
            "OpenWeatherMap Air Pollution"
        )
    )

    parser.add_argument(
        "--months",
        type=int,
        default=3,
        help=(
            "Nombre de mois d'historique. "
            "1 mois = 30 jours."
        )
    )

    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help=(
            "Date de début YYYY-MM-DD "
            "(incluse)"
        )
    )

    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help=(
            "Date de fin YYYY-MM-DD "
            "(incluse). "
            "Par défaut : hier UTC."
        )
    )

    args = parser.parse_args()

    # ========================================================
    # DATE DE FIN
    # ========================================================

    if args.end:

        end_date = datetime.strptime(
            args.end,
            "%Y-%m-%d"
        ).replace(
            tzinfo=timezone.utc
        )

    else:

        yesterday = (
            datetime.now(timezone.utc)
            - timedelta(days=1)
        )

        end_date = yesterday.replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0
        )

    # ========================================================
    # DATE DE DÉBUT
    # ========================================================

    if args.start:

        start_date = datetime.strptime(
            args.start,
            "%Y-%m-%d"
        ).replace(
            tzinfo=timezone.utc
        )

    else:

        number_of_days = (
            30 * args.months
        )

        start_date = (
            end_date
            - timedelta(
                days=number_of_days - 1
            )
        )

    # ========================================================
    # VALIDATION DES DATES
    # ========================================================

    if start_date > end_date:

        raise ValueError(
            "La date de début doit être "
            "antérieure ou égale à la date de fin."
        )

    expected_days = (
        end_date.date()
        - start_date.date()
    ).days + 1

    cities = load_cities()

    expected_files = (
        expected_days
        * len(cities)
    )

    expected_observations = (
        expected_files
        * EXPECTED_OBSERVATIONS_PER_DAY
    )

    print(
        "\n========================================"
    )

    print(
        "BACKFILL AIR QUALITY"
    )

    print(
        "========================================"
    )

    print(
        f"Période : "
        f"{start_date.date()} "
        f"-> "
        f"{end_date.date()} "
        f"(inclus)"
    )

    print(
        f"Nombre de jours : {expected_days}"
    )

    print(
        f"Nombre de villes : {len(cities)}"
    )

    print(
        f"Fichiers attendus : {expected_files}"
    )

    print(
        f"Observations théoriques maximum : "
        f"{expected_observations}"
    )

    print(
        "Fuseau horaire : UTC"
    )

    run_backfill(
        start_date,
        end_date
    )