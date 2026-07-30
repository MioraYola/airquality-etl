"""
Backfill historique de la qualité de l'air
(OpenWeatherMap Air Pollution History API).

Le script récupère les données horaires complètes pour chaque ville
et chaque journée.

Le script est rejouable :
un fichier RAW est créé par ville et par jour.
Si le script est relancé, le fichier correspondant est simplement remplacé.

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


REQUEST_DELAY_SECONDS = 1.1


# Nombre maximum de tentatives
MAX_RETRIES = 3


# Attente progressive entre les retries :
# 5 s, 10 s, 15 s
RETRY_BACKOFF_SECONDS = 5


# Une journée UTC complète doit contenir 24 observations
EXPECTED_OBSERVATIONS_PER_DAY = 24

def load_cities():
    """
    Charge la configuration des villes depuis cities.json.
    """

    with open(
        CONFIG_PATH,
        "r",
        encoding="utf-8"
    ) as f:

        return json.load(f)


def validate_day_payload(
    payload,
    day_start,
    day_end
):
    """
    Vérifie que la réponse API correspond à une journée horaire complète.

    Une journée valide doit avoir :
    - une clé "list"
    - exactement 24 observations
    - exactement 24 timestamps uniques
    - uniquement des timestamps appartenant au jour demandé
    """

    observations = payload.get("list", [])


    # --------------------------------------------------------
    # Vérification du nombre d'observations
    # --------------------------------------------------------

    if len(observations) != EXPECTED_OBSERVATIONS_PER_DAY:

        raise ValueError(
            f"Jour incomplet : "
            f"{len(observations)} observations reçues "
            f"au lieu de "
            f"{EXPECTED_OBSERVATIONS_PER_DAY}"
        )


    # --------------------------------------------------------
    # Récupération des timestamps
    # --------------------------------------------------------

    timestamps = [
        item.get("dt")
        for item in observations
        if item.get("dt") is not None
    ]


    if len(timestamps) != EXPECTED_OBSERVATIONS_PER_DAY:

        raise ValueError(
            "Certaines observations ne possèdent pas de timestamp dt"
        )


    # --------------------------------------------------------
    # Vérification des doublons temporels
    # --------------------------------------------------------

    unique_timestamps = set(timestamps)


    if len(unique_timestamps) != EXPECTED_OBSERVATIONS_PER_DAY:

        raise ValueError(
            f"Timestamps dupliqués : "
            f"{len(unique_timestamps)} heures uniques "
            f"au lieu de 24"
        )


    # --------------------------------------------------------
    # Vérifier que toutes les données appartiennent au jour
    # demandé
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


    return True

def fetch_day(
    lat,
    lon,
    day_start,
    day_end
):
    """
    Récupère les données d'une journée.

    Effectue plusieurs tentatives en cas :
    - timeout
    - erreur réseau
    - coupure SSL
    - réponse journalière incomplète
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


            # Vérification de la journée
            validate_day_payload(
                payload,
                day_start,
                day_end
            )


            return payload


        # ----------------------------------------------------
        # Erreurs réseau
        # ----------------------------------------------------

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
                    f"  [RETRY "
                    f"{attempt}/{MAX_RETRIES}] "
                    f"{e}"
                )


                print(
                    f"  Nouvelle tentative "
                    f"dans {wait}s..."
                )


                time.sleep(wait)


        # ----------------------------------------------------
        # Erreur HTTP
        # ----------------------------------------------------

        except requests.exceptions.HTTPError as e:

            # Exemple :
            # 401 clé invalide
            # 429 quota
            # etc.

            raise e


    # Si toutes les tentatives échouent

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
    Enregistre un fichier JSON RAW pour une ville et une journée.

    Exemple :

    raw/Paris/
        Paris_history_2026-05-01.json
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
    Génère les journées de start à end inclusivement.
    """

    current = start


    while current <= end:

        yield current

        current += timedelta(days=1)


# ============================================================
# BACKFILL
# ============================================================

def run_backfill(
    start_date,
    end_date
):
    """
    Exécute le backfill pour toutes les villes
    entre start_date et end_date inclusivement.
    """

    if not API_KEY:

        raise RuntimeError(
            "OWM_API_KEY n'est pas défini "
            "dans l'environnement"
        )


    cities = load_cities()


    total_files = 0
    total_errors = 0


    failed_days = []


    for city in cities:

        ville = city["ville"]


        print(
            "\n"
            "========================================"
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
            # Début de journée UTC
            # ------------------------------------------------

            day_start = day.replace(
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
                tzinfo=timezone.utc
            )


            # ------------------------------------------------
            # Fin de journée UTC
            #
            # 23:59:59 au lieu du lendemain 00:00
            # ------------------------------------------------

            day_end = (
                day_start
                + timedelta(days=1)
                - timedelta(seconds=1)
            )


            try:

                data = fetch_day(
                    city["lat"],
                    city["lon"],
                    day_start,
                    day_end
                )


                # --------------------------------------------
                # Métadonnées ajoutées au RAW
                # --------------------------------------------

                data["_meta"] = {
                    "ville": ville,
                    "pays": city["pays"],
                    "lat": city["lat"],
                    "lon": city["lon"],
                    "date_backfill": (
                        day.strftime(
                            "%Y-%m-%d"
                        )
                    ),
                    "timezone": "UTC"
                }


                # --------------------------------------------
                # Sauvegarde
                # --------------------------------------------

                path = save_raw(
                    ville,
                    day,
                    data
                )


                total_files += 1


                print(
                    f"[OK] "
                    f"{ville} "
                    f"{day.strftime('%Y-%m-%d')} "
                    f"-> 24 observations"
                )


            except Exception as e:

                total_errors += 1


                failed_days.append(
                    (
                        ville,
                        day.strftime(
                            "%Y-%m-%d"
                        ),
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
        "\n"
        "========================================"
    )

    print(
        "RÉSUMÉ DU BACKFILL"
    )

    print(
        "========================================"
    )


    print(
        f"Fichiers valides écrits : "
        f"{total_files}"
    )


    print(
        f"Jours en erreur : "
        f"{total_errors}"
    )


    if failed_days:

        print(
            "\nJours en échec :"
        )


        for (
            ville,
            jour,
            erreur
        ) in failed_days:

            print(
                f"  - "
                f"{ville} "
                f"{jour} "
                f": {erreur}"
            )


        print(
            "\nRelance uniquement "
            "les jours concernés "
            "avec --start et --end."
        )


    else:

        print(
            "\nTous les jours ont été "
            "récupérés correctement."
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

        # On utilise hier plutôt qu'aujourd'hui
        # afin d'éviter une journée historique incomplète.

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


        # -1 car start et end sont inclusifs.
        #
        # Exemple :
        # 90 jours complets
        # = end - 89 jours → end inclus.

        start_date = (
            end_date
            - timedelta(
                days=number_of_days - 1
            )
        )


    # ========================================================
    # VALIDATION
    # ========================================================

    if start_date > end_date:

        raise ValueError(
            "La date de début doit être "
            "antérieure ou égale "
            "à la date de fin."
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


    print(
        "\n"
        "========================================"
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
        f"Nombre de jours : "
        f"{expected_days}"
    )


    print(
        f"Nombre de villes : "
        f"{len(cities)}"
    )


    print(
        f"Fichiers attendus : "
        f"{expected_files}"
    )


    print(
        "Fuseau horaire : UTC"
    )


    run_backfill(
        start_date,
        end_date
    )