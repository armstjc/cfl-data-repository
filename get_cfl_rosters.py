import json
import logging
# import os
from datetime import datetime

import numpy as np
import pandas as pd
import requests
# from bs4 import BeautifulSoup

from get_schedules import get_cfl_schedules

# def get_cfl_stats_crew_rosters(season: int):
#     """ """
#     url = f"https://www.statscrew.com/football/l-CFL/y-{season}"


def parse_cfl_player_url(player_url: str) -> int:
    """ """
    try:
        player_id = int(player_url.split("/")[-2])
    except Exception as e:
        logging.info(f"Unhandled exception `{e}`")
        player_id = -1000
    return player_id


def get_cfl_rosters():
    """ """
    now = datetime.now()
    season = now.year
    url = (
        "https://www.cfl.ca/wp-content/themes/cfl.ca/inc/"
        + "admin-ajax.php?action=get_all_players"
    )
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4)"
        + " AppleWebKit/537.36 (KHTML, like Gecko) "
        + "Chrome/125.0.0.0 Safari/537.36",
    }
    # rosters_df = pd.DataFrame()
    schedule_df = get_cfl_schedules(now.year)
    schedule_df = schedule_df[
        (schedule_df["team_1_score"] > 0) | (schedule_df["team_2_score"] > 0)
    ]
    # game_types_arr = schedule_df.to_list()
    # if "Regular Season" in game_types_arr:
    #     week = 0
    schedule_df = schedule_df[schedule_df["eventTypeName"] != "Preseason"]

    if "Grey Cup" in schedule_df["eventTypeName"].iloc[-1]:
        week = 0
        season += 1
    if len(schedule_df) == 0:
        week = 0
    else:
        week = max(schedule_df["week"].to_list()) + 1

    response = requests.get(url=url, headers=headers)

    json_data = json.loads(response.text)
    json_data = json_data["data"]
    players_df = pd.DataFrame(
        data=json_data,
        columns=[
            "jersey_num",
            "player_name",
            "current_team_abv",
            "position",
            "import_status",
            "height",
            "weight",
            "age",
            "college",
            "player_url",
        ],
    )
    players_df["jersey_num"] = pd.to_numeric(
        players_df["jersey_num"], errors="coerce"
    )
    players_df["weight"] = pd.to_numeric(players_df["weight"], errors="coerce")
    players_df["age"] = pd.to_numeric(players_df["age"], errors="coerce")
    players_df = players_df.replace(r"^\s*$", np.nan, regex=True)
    players_df = players_df.astype(
        {
            # "jersey_num": "uint16",
            "player_name": "string",
            "current_team_abv": "string",
            "position": "string",
            "import_status": "string",
            "height": "string",
            # "weight": "uint16",
            # "age": "uint16",
            "college": "string",
            "player_url": "string",
        }
    )
    players_df = players_df.sort_values(
        ["current_team_abv", "jersey_num", "player_url"]
    )
    players_df["player_id"] = players_df["player_url"].map(
        lambda x: parse_cfl_player_url(x)
    )
    players_df.loc[
        players_df["player_id"] != 0,
        "last_updated"
    ] = now.isoformat()

    players_df = players_df.replace(r"^\s*$", np.nan, regex=True)
    rosters_df = players_df.dropna(subset=["current_team_abv"])
    players_df.loc[players_df["player_id"] != 0, "season"] = season

    players_df.to_csv("rosters/cfl_players.csv", index=False)

    rosters_df.to_csv(
        f"rosters/{now.year}_cfl_rosters.csv",
        index=False
    )
    rosters_df.loc[players_df["player_id"] != 0, "week"] = week

    rosters_df.to_csv(
        f"rosters/weekly/{now.year}-{week:02d}_cfl_weekly_rosters.csv",
        index=False
    )
    rosters_df
    return rosters_df, players_df


if __name__ == "__main__":
    # get_cfl_rosters()
    print(get_cfl_rosters())
