import json
import logging
import os
from datetime import datetime

import pandas as pd
import requests
from tqdm import tqdm


def get_cfl_schedules(season: int) -> pd.DataFrame:
    """
    Given a season, download, parse, and return CFL schedule data
    back as a pandas `DataFrame` (think spreadsheet).

    Parameters
    ----------
    `season` (int, mandatory):
        The season you want a CFL schedule for.

    Returns
    ----------
    A pandas `DataFrame` with CFL schedule data.
    """
    url = "https://www.cfl.ca/wp-content/themes/cfl.ca/inc/" +\
        f"admin-ajax.php?action=scoreboard&lang=en&week=all&season={season}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4)"
        + " AppleWebKit/537.36 (KHTML, like Gecko) "
        + "Chrome/125.0.0.0 Safari/537.36",
    }
    response = requests.get(url=url, headers=headers)
    json_data = json.loads(response.text)
    schedule_df = pd.json_normalize(json_data)
    schedule_df = schedule_df.infer_objects()
    schedule_df["week"] = pd.to_numeric(schedule_df["week"], errors="coerce")

    try:
        schedule_df = schedule_df.drop(
            columns=["team_1_linescores", "team_2_linescores"]
        )
    except Exception as e:
        logging.info(f"Unhandled exception `{e}`.")
    # schedule_df.to_csv("test.csv", index=False)
    return schedule_df


if __name__ == "__main__":
    now = datetime.now()
    now_timestamp = now.isoformat()
    timestamp_json = f"{{\"timestamp\":\"{now_timestamp}\"}}"
    with open("schedule/timestamp.json", "w+") as f:
        f.write(timestamp_json)

    try:
        os.mkdir("schedule")
    except FileExistsError:
        logging.info("`./schedule` already exists.")

    for i in tqdm(range(now.year-1, now.year+1)):
        df = get_cfl_schedules(i)
        df.to_csv(
            f"schedule/{i}_cfl_schedule.csv",
            index=False
        )
