from io import StringIO
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


def get_negotiation_lists():
    """ """
    data_df = pd.DataFrame()
    data_df_arr = []

    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    datetime_iso = now.isoformat()
    try:
        os.mkdir("rosters")
    except FileExistsError:
        logging.info("`./rosters` already exists.")

    try:
        os.mkdir("rosters/negotiation_list")
    except FileExistsError:
        logging.info("`./negotiation_list` already exists.")

    url = "https://www.cfl.ca/negotiation-list/"

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4)"
        + " AppleWebKit/537.36 (KHTML, like Gecko) "
        + "Chrome/138.0.0.0 Safari/537.36",
    }

    response = requests.get(url=url, headers=headers)
    time.sleep(1)
    soup = BeautifulSoup(response.text, features="lxml")
    team_lists = soup.find_all("li", {"role": "tab", "class": "week-row"})
    for team in team_lists:
        temp_df = pd.DataFrame()
        team_abv = team.find(
            "div", {"class": "matchup"}
        ).find(
            "span", {"class": "text"}
        ).text
        table_data = team.find("table").find("tbody").find_all("tr")
        for row in table_data:
            cells = row.find_all("td")
            temp_df = pd.DataFrame(
                {
                    "team_abv": team_abv,
                    "player_full_name": cells[0].text.strip(),
                    "player_position": cells[1].text,
                    "player_college": cells[2].text,
                },
                index=[0],
            )
            data_df_arr.append(temp_df)

    data_df = pd.concat(data_df_arr, ignore_index=True)
    data_df["last_update"] = datetime_iso
    data_df["update_date"] = date_str
    data_df.to_csv(
        f"rosters/negotiation_list/{date_str}_cfl_negotiation_lists.csv",
        index=False
    )

if __name__ == "__main__":
    get_negotiation_lists()
