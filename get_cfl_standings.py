import json
import logging
import os
from datetime import datetime

import pandas as pd
import requests


def get_cfl_standings(season: int = 2026):
    """ """
    try:
        os.mkdir("standings")
    except FileExistsError:
        logging.info("`./standings` already exists.")

    now = datetime.now()
    season = now.year

    if now.month < 5:
        season -= 1
    url = (
        f"https://api.stats.cfl.ca/standings/{season}"
    )
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4)"
        + " AppleWebKit/537.36 (KHTML, like Gecko) "
        + "chrome/153.0.0.0 Safari/537.36",
    }

    response = requests.get(url=url, headers=headers)

    json_data = json.loads(response.text)

    # full standings
    current_week = json_data["data"]["week"]
    json_data = json_data["data"]["divisions"]
    full_standings_df = pd.json_normalize(
        json_data["unified"]["standings"]
    )
    full_standings_df["current_week"] = current_week
    full_standings_df.to_csv(
        f"standings/{season}_cfl_full_standings.csv",
        index=False
    )

    # east/west standings
    east_standings_df = pd.json_normalize(
        json_data["east"]["standings"]
    )

    west_standings_df = pd.json_normalize(
        json_data["west"]["standings"]
    )
    full_standings_df = pd.concat(
        [
            east_standings_df,
            west_standings_df
        ],
        ignore_index=True
    )
    full_standings_df["current_week"] = current_week

    full_standings_df.to_csv(
        f"standings/{season}_cfl_divisional_standings.csv",
        index=False
    )

    return full_standings_df


if __name__ == "__main__":
    now = datetime.now()
    year = now.year

    if now.month < 5:
        year -= 1
    get_cfl_standings()
