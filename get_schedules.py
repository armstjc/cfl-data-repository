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
    columns = [
        "season",
        "season_id",
        "week",
        "game_id",
        "season_game_count",
        "home_team_id",
        "home_game_count",
        "away_team_id",
        "game_type_id",
        "quarters",
        "away_team_score",
        "home_team_score",
        "start_at_local",
        "start_at",
        "game_status",
        "venue_id",
        "genius_sports_id",
        "fixtureId",
        "eventTypeName",
        "created_at",
        "revision_at",
        "game_revision_number",
        "season_type"
    ]
    season_id = 0
    schedule_df = pd.DataFrame()
    schedule_df_arr = []
    temp_df = pd.DataFrame()

    match season:
        case 2026:
            season_id = 75
        case 2025:
            season_id = 34
        case 2024:
            season_id = 33
        case 2023:
            season_id = 2
        case 2022:
            season_id = 1
        case 2021:
            season_id = 32
        case 2019:
            season_id = 30
        case 2018:
            season_id = 29
        case 2017:
            season_id = 28
        case 2016:
            season_id = 27

    if season_id == 0:
        raise ValueError(
            f"Unhandled season `{season}`."
        )

    url = f"https://cfl.ca/api/v1/content/data/schedule?seasonId={season_id}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4)"
        + " AppleWebKit/537.36 (KHTML, like Gecko) "
        + "Chrome/152.0.0.0 Safari/537.36",
    }
    response = requests.get(url=url, headers=headers)
    json_data = json.loads(response.text)

    # -------------------------------------------------------------------------
    # Deprecated on 2026-09-02 due to new CFL website design
    # -------------------------------------------------------------------------

    # schedule_df = pd.json_normalize(json_data)
    # schedule_df["startDate"] = pd.to_datetime(
    #     schedule_df["startDate"], utc=True
    # ).dt.tz_convert("UTC")
    # # schedule_df = schedule_df.infer_objects()
    # print()
    # # print(schedule_df.memory_usage(index=False))
    # schedule_df = schedule_df.astype(
    #     {
    #         "eventId": "uint16",
    #         "fixtureId": "uint64",
    #         # "startDate": "datetime64[ns]",
    #         "eventTypeId": "uint8",
    #         "eventStatus_eventStatusId": "uint8",
    #         "eventStatus_name": "string",
    #         "eventStatus_period": "uint8",
    #     },
    #     errors="ignore",
    #     # errors="raise"
    # )
    # # print(schedule_df.memory_usage(index=False))
    # # print(schedule_df.dtypes)
    # schedule_df["week"] = pd.to_numeric(schedule_df["week"], errors="coerce")
    # schedule_df["day_of_week"] = schedule_df["startDate"].dt.day_name()
    # try:
    #     schedule_df = schedule_df.drop(
    #         columns=["team_1_linescores", "team_2_linescores"]
    #     )
    # except Exception as e:
    #     logging.info(f"Unhandled exception `{e}`.")
    # schedule_df.to_csv("test.csv", index=False)

    for game in json_data["fixtures"]:
        temp_df = pd.DataFrame(
            {
                "season": season,
                "season_id": game["season_id"],
                "week":  game["week"],
                "game_id": game["ID"],
                "season_game_count": game["season_game_count"],
                "home_team_id":  game["home_team_id"],
                "home_game_count":  game["home_game_count"],
                "away_team_id":  game["away_team_id"],
                "game_type_id":  game["game_type_id"],
                "start_at_local":  game["start_at_local"],
                "start_at":  game["start_at"],
                "created_at":  game["metadata"]["created_at"],
                "revision_at":  game["metadata"]["revision_at"],
                "game_revision_number":  game["metadata"]["revision"]
            },
            index=[0]
        )

        try:
            temp_df["game_status"] = game["game_status"]
        except Exception:
            temp_df["game_status"] = "Pre-Game"

        try:
            temp_df["quarters"] = game["total_periods"]
        except Exception:
            temp_df["quarters"] = None

        try:
            temp_df["venue_id"] = game["venue_id"]
        except Exception:
            temp_df["venue_id"] = None

        try:
            temp_df["away_team_score"] = game["away_team_score"]
            temp_df["home_team_score"] = game["home_team_score"]
        except Exception:
            temp_df["away_team_score"] = None
            temp_df["home_team_score"] = None

        try:
            temp_df["genius_sports_id"] = game["genius"]["id"]
            temp_df["fixtureId"] = game["genius"]["id"]
        except Exception:
            temp_df["genius_sports_id"] = None
            temp_df["fixtureId"] = None

        schedule_df_arr.append(temp_df)
        del temp_df

    schedule_df = pd.concat(
        schedule_df_arr,
        ignore_index=True
    )
    schedule_df["eventTypeName"] = schedule_df["game_type_id"].map(
        {
            0: "Preseason",
            1: "Regular Season",
            2: "Eastern Semi-Finals",
            3: "Western Semi-Finals",
            4: "Eastern Conference Finals",
            5: "Western Conference Finals",
            6: "Grey Cup",
        }
    )
    schedule_df["season_type"] = schedule_df["game_type_id"].map(
        {
            0: "Preseason",
            1: "Regular Season",
            2: "Postseason",
            3: "Postseason",
            4: "Postseason",
            5: "Postseason",
            6: "Grey Cup",
        }
    )

    schedule_df = schedule_df.reindex(
        columns=columns
    )
    return schedule_df


if __name__ == "__main__":
    now = datetime.now()
    now_timestamp = now.isoformat()
    year = now.year

    if now.month < 5:
        year -= 1

    try:
        os.mkdir("schedule")
    except FileExistsError:
        logging.info("`./schedule` already exists.")

    timestamp_json = f"{{\"timestamp\":\"{now_timestamp}\"}}"
    with open("schedule/timestamp.json", "w+") as f:
        f.write(timestamp_json)

    for i in tqdm(range(year-1, year+1)):
        df = get_cfl_schedules(i)
        df.to_csv(
            f"schedule/{i}_cfl_schedule.csv",
            index=False
        )
