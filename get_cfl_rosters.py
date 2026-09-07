import json
import logging
import os
import time
from datetime import datetime

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from get_cfl_standings import get_cfl_standings
from get_schedules import get_cfl_schedules


def parse_cfl_player_url(player_url: str) -> int:
    """ """
    try:
        player_id = int(player_url.split("/")[-2])
    except Exception as e:
        logging.info(f"Unhandled exception `{e}`")
        player_id = -1000
    return player_id


def get_cfl_team_roster(team_id: int) -> pd.DataFrame:
    """ """
    url = (
        "https://cfl.ca/api/v1/content/data/players?limit=10000&" +
        f"team_id={team_id}&active_only=true"
    )
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4)"
        + " AppleWebKit/537.36 (KHTML, like Gecko) "
        + "Chrome/152.0.0.0 Safari/537.36",
    }

    response = requests.get(url=url, headers=headers)
    time.sleep(2)

    json_data = json.loads(response.text)

    roster_df = pd.json_normalize(
        json_data["players"]
    )
    return roster_df


def get_cfl_rosters():
    """ """
    now = datetime.now()
    players_df = pd.DataFrame()
    players_df_arr = []

    season = now.year
    if now.month < 5:
        season -= 1

    try:
        os.mkdir("rosters")
    except FileExistsError:
        logging.info("`./rosters` already exists.")

    try:
        os.mkdir("rosters/weekly")
    except FileExistsError:
        logging.info("`./weekly` already exists.")

    standings_df = get_cfl_standings(season=season)
    schedule_df = get_cfl_schedules(season=season)
    schedule_df = schedule_df[schedule_df["game_status"] != "Pre-Game"]

    if "Preseason" in schedule_df["eventTypeName"].iloc[-1]:
        week = 0
        # season += 1
    elif "Grey Cup" in schedule_df["eventTypeName"].iloc[-1]:
        week = 0
        season += 1
    elif len(schedule_df) == 0:
        week = 0
    else:
        week = max(schedule_df["week"].to_list()) + 1

    team_ids_arr = standings_df["team_id"].to_list()

    for team_id in team_ids_arr:
        temp_df = get_cfl_team_roster(team_id=team_id)
        players_df_arr.append(temp_df)

        del temp_df

    rosters_df = pd.concat(players_df_arr, ignore_index=True)
    # players_df.loc[players_df["player_id"] != 0, "season"] = season

    # players_df.to_csv("rosters/cfl_players.csv", index=False)

    rosters_df.to_csv(f"rosters/{now.year}_cfl_rosters.csv", index=False)
    rosters_df.loc["week"] = week

    rosters_df.to_csv(
        f"rosters/weekly/{now.year}-{week:02d}_cfl_weekly_rosters.csv",
        index=False
    )
    rosters_df
    return rosters_df, players_df


def get_stats_crew_cfl_rosters(season: int):
    """ """
    try:
        os.mkdir("rosters")
    except FileExistsError:
        logging.info("`./rosters` already exists.")

    try:
        os.mkdir("rosters/weekly")
    except FileExistsError:
        logging.info("`./weekly` already exists.")

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4)"
        + " AppleWebKit/537.36 (KHTML, like Gecko) "
        + "Chrome/152.0.0.0 Safari/537.36",
    }

    # now = datetime.now()

    # for season in range(2021, now.year+1):
    print(f"Getting CFL rosters for the {season} season.")
    roster_df = pd.DataFrame()
    roster_df_arr = []

    initial_url = f"https://www.statscrew.com/football/l-CFL/y-{season}"
    urls_arr = []
    response = requests.get(url=initial_url, headers=headers)
    time.sleep(5)
    soup = BeautifulSoup(response.text, features="lxml")

    base_urls_arr = soup.find_all("a")

    for url in base_urls_arr:
        url_str = url.get("href")
        if "roster" in url_str and "statscrew.com" in url_str:
            urls_arr.append(url_str)

    for url in tqdm(urls_arr):
        team_id = url.split("/y-")[0]
        team_id = team_id.split("/t-")[1]
        team_id = team_id.replace("CFL", "")
        response = requests.get(url=url, headers=headers)
        time.sleep(5)
        soup = BeautifulSoup(response.text, features="lxml")

        table_html = soup.find("table", {"class": "sortable"})
        t_header = table_html.find("thead").find("tr").find_all("th")
        t_header_arr = [x.text for x in t_header]

        del t_header

        t_body = table_html.find("tbody").find_all("tr")

        for row in t_body:
            try:
                player_id = row.find("a").get("href")
                player_id = player_id.replace("https://www.statscrew.com/", "")
                player_id = player_id.replace("football/stats/p-", "")
                player_id = player_id.replace("/", "")
            except Exception:
                continue

            t_cells = row.find_all("td")
            t_cells = [x.text.strip() for x in t_cells]
            temp_df = pd.DataFrame(data=[t_cells], columns=t_header_arr)
            temp_df["stats_crew_player_id"] = player_id
            temp_df["team_id"] = team_id
            roster_df_arr.append(temp_df)

            del temp_df

    roster_df = pd.concat(roster_df_arr, ignore_index=True)
    roster_df.rename(
        columns={
            "#": "player_jersey_num",
            "Player": "player_full_name",
            "Pos.": "player_position",
            "Birth Date": "player_birthday",
            "Height": "player_height",
            "Weight": "player_weight",
            "College": "player_college",
            "Hometown": "player_hometown",
        },
        inplace=True,
    )
    roster_df["team_id"] = roster_df["team_id"].str.replace("ORB", "OTT")
    roster_df[["player_first_name", "player_last_name"]] = roster_df[
        "player_full_name"
    ].str.split(" ", n=1, expand=True)
    roster_df.to_csv(
        f"rosters/{season}_stats_crew_cfl_rosters.csv",
        index=False
    )
    return roster_df


if __name__ == "__main__":
    now = datetime.now()
    year = now.year

    if now.month < 5:
        year -= 1
    get_cfl_rosters()
