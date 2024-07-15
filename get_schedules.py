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
    schedule_df["startDate"] = pd.to_datetime(
        schedule_df["startDate"], utc=True
    ).dt.tz_convert("UTC")
    # schedule_df = schedule_df.infer_objects()
    print()
    # print(schedule_df.memory_usage(index=False))
    schedule_df = schedule_df.astype(
        {
            "eventId": "uint16",
            "fixtureId": "uint64",
            # "startDate": "datetime64[ns]",
            "eventTypeId": "uint8",
            "eventStatus_eventStatusId": "uint8",
            "eventStatus_name": "string",
            "eventStatus_period": "uint8",
        },
        errors="ignore",
        # errors="raise"
    )
    # print(schedule_df.memory_usage(index=False))
    # print(schedule_df.dtypes)
    schedule_df["week"] = pd.to_numeric(schedule_df["week"], errors="coerce")
    schedule_df["day_of_week"] = schedule_df["startDate"].dt.day_name()
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

    try:
        os.mkdir("schedule")
    except FileExistsError:
        logging.info("`./schedule` already exists.")

    timestamp_json = f"{{\"timestamp\":\"{now_timestamp}\"}}"
    with open("schedule/timestamp.json", "w+") as f:
        f.write(timestamp_json)

    for i in tqdm(range(now.year-1, now.year+1)):
        df = get_cfl_schedules(i)
        df.to_csv(
            f"schedule/{i}_cfl_schedule.csv",
            index=False
        )
