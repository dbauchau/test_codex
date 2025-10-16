# Chargeur de cours NASDAQ

Ce dépôt contient un script Python permettant de télécharger les cours
journaliers de toutes les actions listées sur le NASDAQ et de les stocker dans
une base de données SQLite. À chaque exécution, seules les nouvelles valeurs
sont ajoutées, ce qui permet de mettre la base à jour quotidiennement sans
dupliquer les données existantes.

## Installation

Créez un environnement virtuel, puis installez les dépendances :

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Utilisation

```bash
python nasdaq_loader.py --database nasdaq_prices.db
```

Options principales :

- `--database`: chemin vers le fichier SQLite. Par défaut `nasdaq_prices.db`.
- `--start-date`: force une date de départ (format `AAAA-MM-JJ`). Par défaut, la
  date suivant la dernière valeur enregistrée est utilisée ou, pour une base
  vide, le 1er janvier 2010.
- `--limit`: limite le nombre de symboles traités (utile pour des tests).
- `--log-level`: niveau de verbosité (`INFO`, `DEBUG`, ...).

Le script télécharge d'abord la liste officielle des symboles NASDAQ, puis
interroge `yfinance` pour récupérer les prix. Les données sont stockées dans la
table `prices` avec la structure suivante :

| Colonne     | Description                       |
|-------------|-----------------------------------|
| `symbol`    | Symbole de l'action               |
| `date`      | Date de la cotation (UTC)         |
| `open`      | Prix d'ouverture                  |
| `high`      | Prix le plus haut de la séance    |
| `low`       | Prix le plus bas de la séance     |
| `close`     | Prix de clôture                   |
| `adj_close` | Prix de clôture ajusté            |
| `volume`    | Volume échangé                    |

Une table `metadata` conserve la date de la dernière synchronisation pour
faciliter les mises à jour incrémentales.
