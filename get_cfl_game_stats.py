import json
import logging
import os
import time
from datetime import datetime

import pandas as pd
import requests
from tqdm import tqdm

from get_schedules import get_cfl_schedules


def player_parser(data: dict) -> pd.DataFrame:
    """ """
    pd.set_option('future.no_silent_downcasting', True)
    # print(data)
    # passing
    columns_arr = [
        "competitor_id",
        "player_jersey_number",
        "player_full_name",
        "player_abv_name",
        "player_position",
        "player_status",
        # Passing
        "passing_COMP",
        "passing_ATT",
        "passing_COMP%",
        "passing_YDS",
        "passing_TD",
        "passing_INT",
        "passing_LONG",
        "passing_YDS/ATT",
        "passing_AY/A",
        # Rushing
        "rushing_ATT",
        "rushing_YDS",
        "rushing_TD",
        "rushing_LONG",
        "rushing_AVG",
        # Receiving
        "receiving_TGT",
        "receiving_REC",
        "receiving_YDS",
        "receiving_YAC",
        "receiving_TD",
        "receiving_LONG",
        "receiving_AVG",
        "receiving_YDS/TGT",
        "receiving_CATCH%",
        # Defense
        "defense_SOLO",
        "defense_SACKS",
        "defense_TFL",
        "defense_INT",
        "defense_FF",
        "defense_FR",
        "defense_ST_TAK",
        # FG
        "kicking_FGM",
        "kicking_FGA",
        "kicking_FG%",
        "kicking_FG_LONG",
        "kicking_XP",
        "kicking_ROUGE",
        # Kickoffs
        "kickoffs_NUM",
        "kickoffs_YDS",
        "kickoffs_LONG",
        # Punting
        "punting_NO",
        "punting_GROSS_YDS",
        "punting_GROSS_AVG",
        "punting_LONG",
        "punting_IN_10",
        # Kick Returns
        "kick_return_NUM",
        "kick_return_YDS",
        "kick_return_LONG",
        "kick_return_AVG",
        # Punt Returns
        "punt_return_NUM",
        "punt_return_YDS",
        "punt_return_LONG",
        "punt_return_AVG",
    ]
    temp_df = pd.DataFrame()
    passing_df = pd.DataFrame()
    rushing_df = pd.DataFrame()
    receiving_df = pd.DataFrame()
    defense_df = pd.DataFrame()
    kicking_df = pd.DataFrame()
    kickoffs_df = pd.DataFrame()
    punting_df = pd.DataFrame()
    kick_return_df = pd.DataFrame()
    punt_return_df = pd.DataFrame()

    passing_df_arr = []
    rushing_df_arr = []
    receiving_df_arr = []
    defense_df_arr = []
    kicking_df_arr = []
    kickoffs_df_arr = []
    punting_df_arr = []
    kick_return_df_arr = []
    punt_return_df_arr = []

    # Stat parsing
    for player in data["passing"]:
        competitor_id = player["competitorId"]
        player_full_name = player["name"]
        player_abv_name = player["abbreviationName"]
        player_position = player["position"]
        player_jersey_number = player["number"]
        player_status = player["type"]

        temp_df = pd.DataFrame(
            {
                "competitor_id": competitor_id,
                "player_jersey_number": player_jersey_number,
                "player_full_name": player_full_name,
                "player_abv_name": player_abv_name,
                "player_position": player_position,
                "player_status": player_status,
            },
            index=[0],
        )
        for stat in player["stats"]:
            if stat["name"] == "YARDS":
                temp_df["passing_YDS"] = stat["statValue"]
            elif stat["name"] == "COMPLETIONS_ATTEMPTS":
                temp_df["passing_COMP/ATT"] = stat["statValue"]
            elif stat["name"] == "TOUCHDOWNS":
                temp_df["passing_TD"] = stat["statValue"]
            elif stat["name"] == "INTERCEPTIONS":
                temp_df["passing_INT"] = stat["statValue"]
            elif stat["name"] == "LONGEST":
                temp_df["passing_LONG"] = stat["statValue"]
            elif stat["name"] == "AVERAGE_YARDS":
                pass
            elif stat["name"] == "RATING":
                pass
            else:
                raise LookupError(
                    f"Unhandled passing stat: `{stat}`"
                )
        passing_df_arr.append(temp_df)
        del temp_df

    for player in data["rushing"]:
        competitor_id = player["competitorId"]
        player_full_name = player["name"]
        player_abv_name = player["abbreviationName"]
        player_position = player["position"]
        player_jersey_number = player["number"]
        player_status = player["type"]

        temp_df = pd.DataFrame(
            {
                "competitor_id": competitor_id,
                "player_jersey_number": player_jersey_number,
                "player_full_name": player_full_name,
                "player_abv_name": player_abv_name,
                "player_position": player_position,
                "player_status": player_status,
            },
            index=[0],
        )
        for stat in player["stats"]:
            if stat["name"] == "YARDS":
                temp_df["rushing_YDS"] = stat["statValue"]
            elif stat["name"] == "TOUCHDOWNS":
                temp_df["rushing_TD"] = stat["statValue"]
            elif stat["name"] == "INTERCEPTIONS":
                temp_df["passing_INT"] = stat["statValue"]
            elif stat["name"] == "LONGEST":
                temp_df["rushing_LONG"] = stat["statValue"]
            elif stat["name"] == "CARRIES":
                temp_df["rushing_ATT"] = stat["statValue"]
            elif stat["name"] == "AVERAGE_YARDS":
                pass
            else:
                raise LookupError(
                    f"Unhandled rushing stat: `{stat}`"
                )
        rushing_df_arr.append(temp_df)
        del temp_df

    for player in data["receiving"]:
        competitor_id = player["competitorId"]
        player_full_name = player["name"]
        player_abv_name = player["abbreviationName"]
        player_position = player["position"]
        player_jersey_number = player["number"]
        player_status = player["type"]

        temp_df = pd.DataFrame(
            {
                "competitor_id": competitor_id,
                "player_jersey_number": player_jersey_number,
                "player_full_name": player_full_name,
                "player_abv_name": player_abv_name,
                "player_position": player_position,
                "player_status": player_status,
            },
            index=[0],
        )
        for stat in player["stats"]:
            if stat["name"] == "YARDS":
                temp_df["receiving_YDS"] = stat["statValue"]
            elif stat["name"] == "TOUCHDOWNS":
                temp_df["receiving_TD"] = stat["statValue"]
            elif stat["name"] == "TARGETS":
                temp_df["receiving_TGT"] = stat["statValue"]
            elif stat["name"] == "LONGEST":
                temp_df["receiving_LONG"] = stat["statValue"]
            elif stat["name"] == "YARDS_AFTER_CATCH":
                temp_df["receiving_YAC"] = stat["statValue"]
            elif stat["name"] == "RECEPTIONS":
                temp_df["receiving_REC"] = stat["statValue"]
            elif stat["name"] == "AVERAGE_YARDS":
                pass
            else:
                raise LookupError(
                    f"Unhandled receiving stat: `{stat}`"
                )
        receiving_df_arr.append(temp_df)
        del temp_df

    for player in data["defence"]:
        competitor_id = player["competitorId"]
        player_full_name = player["name"]
        player_abv_name = player["abbreviationName"]
        player_position = player["position"]
        player_jersey_number = player["number"]
        player_status = player["type"]

        temp_df = pd.DataFrame(
            {
                "competitor_id": competitor_id,
                "player_jersey_number": player_jersey_number,
                "player_full_name": player_full_name,
                "player_abv_name": player_abv_name,
                "player_position": player_position,
                "player_status": player_status,
            },
            index=[0],
        )
        for stat in player["stats"]:
            if stat["name"] == "DEFENCE_SOLO":
                temp_df["defense_SOLO"] = stat["statValue"]
            elif stat["name"] == "DEFENCE_SK":
                temp_df["defense_SACKS"] = stat["statValue"]
            elif stat["name"] == "DEFENCE_TFL":
                temp_df["defense_TFL"] = stat["statValue"]
            elif stat["name"] == "DEFENCE_INT":
                temp_df["defense_INT"] = stat["statValue"]
            elif stat["name"] == "DEFENCE_FF":
                temp_df["defense_FF"] = stat["statValue"]
            elif stat["name"] == "DEFENCE_FR":
                temp_df["defense_FR"] = stat["statValue"]
            elif stat["name"] == "DEFENCE_STT":
                temp_df["defense_ST_TAK"] = stat["statValue"]
            else:
                raise LookupError(
                    f"Unhandled defensive stat: `{stat}`"
                )
        defense_df_arr.append(temp_df)
        del temp_df

    for player in data["fieldGoals"]:
        competitor_id = player["competitorId"]
        player_full_name = player["name"]
        player_abv_name = player["abbreviationName"]
        player_position = player["position"]
        player_jersey_number = player["number"]
        player_status = player["type"]

        temp_df = pd.DataFrame(
            {
                "competitor_id": competitor_id,
                "player_jersey_number": player_jersey_number,
                "player_full_name": player_full_name,
                "player_abv_name": player_abv_name,
                "player_position": player_position,
                "player_status": player_status,
            },
            index=[0],
        )
        for stat in player["stats"]:
            if stat["name"] == "FIELDGOALS_FGFGA":
                temp_df["kicking_FG"] = stat["statValue"]
            elif stat["name"] == "FIELDGOALS_LNG":
                temp_df["kicking_FG_LONG"] = stat["statValue"]
            elif stat["name"] == "FIELDGOALS_XP":
                temp_df["kicking_XP"] = stat["statValue"]
            elif stat["name"] == "FIELDGOALS_SNG":
                temp_df["kicking_ROUGE"] = stat["statValue"]
            elif stat["name"] == "FIELDGOALS_FG":
                pass
            else:
                raise LookupError(
                    f"Unhandled kicking stat: `{stat}`"
                )
        kicking_df_arr.append(temp_df)
        del temp_df

    for player in data["kickoffs"]:
        competitor_id = player["competitorId"]
        player_full_name = player["name"]
        player_abv_name = player["abbreviationName"]
        player_position = player["position"]
        player_jersey_number = player["number"]
        player_status = player["type"]

        temp_df = pd.DataFrame(
            {
                "competitor_id": competitor_id,
                "player_jersey_number": player_jersey_number,
                "player_full_name": player_full_name,
                "player_abv_name": player_abv_name,
                "player_position": player_position,
                "player_status": player_status,
            },
            index=[0],
        )
        for stat in player["stats"]:
            if stat["name"] == "KICKOFFS_NO":
                temp_df["kickoffs_NUM"] = stat["statValue"]
            elif stat["name"] == "KICKOFFS_YDS":
                temp_df["kickoffs_YDS"] = stat["statValue"]
            elif stat["name"] == "KICKOFFS_LNG":
                temp_df["kickoffs_LONG"] = stat["statValue"]
            elif stat["name"] == "KICKOFFS_AVG":
                pass
            else:
                raise LookupError(
                    f"Unhandled kickoff stat: `{stat}`"
                )
        kickoffs_df_arr.append(temp_df)
        del temp_df

    for player in data["punts"]:
        competitor_id = player["competitorId"]
        player_full_name = player["name"]
        player_abv_name = player["abbreviationName"]
        player_position = player["position"]
        player_jersey_number = player["number"]
        player_status = player["type"]

        temp_df = pd.DataFrame(
            {
                "competitor_id": competitor_id,
                "player_jersey_number": player_jersey_number,
                "player_full_name": player_full_name,
                "player_abv_name": player_abv_name,
                "player_position": player_position,
                "player_status": player_status,
            },
            index=[0],
        )
        for stat in player["stats"]:
            if stat["name"] == "PUNTING_NO":
                temp_df["punting_NO"] = stat["statValue"]
            elif stat["name"] == "PUNTING_YDS":
                temp_df["punting_GROSS_YDS"] = stat["statValue"]
            elif stat["name"] == "PUNTING_LNG":
                temp_df["punting_LONG"] = stat["statValue"]
            elif stat["name"] == "PUNTING_IN10":
                temp_df["punting_IN_10"] = stat["statValue"]
            elif stat["name"] == "PUNTING_AVG":
                pass
            else:
                raise LookupError(
                    f"Unhandled kickoff stat: `{stat}`"
                )

        punting_df_arr.append(temp_df)
        del temp_df

    for player in data["kickoffReturns"]:
        competitor_id = player["competitorId"]
        player_full_name = player["name"]
        player_abv_name = player["abbreviationName"]
        player_position = player["position"]
        player_jersey_number = player["number"]
        player_status = player["type"]

        temp_df = pd.DataFrame(
            {
                "competitor_id": competitor_id,
                "player_jersey_number": player_jersey_number,
                "player_full_name": player_full_name,
                "player_abv_name": player_abv_name,
                "player_position": player_position,
                "player_status": player_status,
            },
            index=[0],
        )
        for stat in player["stats"]:
            if stat["name"] == "KICKRETURN_NO":
                temp_df["kick_return_NUM"] = stat["statValue"]
            elif stat["name"] == "KICKRETURN_YDS":
                temp_df["kick_return_YDS"] = stat["statValue"]
            elif stat["name"] == "KICKRETURN_LNG":
                temp_df["kick_return_LONG"] = stat["statValue"]
            elif stat["name"] == "KICKRETURN_AVG":
                pass
            else:
                raise LookupError(
                    f"Unhandled kickoff stat: `{stat}`"
                )
        kick_return_df_arr.append(temp_df)
        del temp_df

    for player in data["puntReturns"]:
        competitor_id = player["competitorId"]
        player_full_name = player["name"]
        player_abv_name = player["abbreviationName"]
        player_position = player["position"]
        player_jersey_number = player["number"]
        player_status = player["type"]

        temp_df = pd.DataFrame(
            {
                "competitor_id": competitor_id,
                "player_jersey_number": player_jersey_number,
                "player_full_name": player_full_name,
                "player_abv_name": player_abv_name,
                "player_position": player_position,
                "player_status": player_status,
            },
            index=[0],
        )
        for stat in player["stats"]:
            if stat["name"] == "PUNTRETURN_NO":
                temp_df["punt_return_NUM"] = stat["statValue"]
            elif stat["name"] == "PUNTRETURN_YDS":
                temp_df["punt_return_YDS"] = stat["statValue"]
            elif stat["name"] == "PUNTRETURN_LNG":
                temp_df["punt_return_LONG"] = stat["statValue"]
            elif stat["name"] == "PUNTRETURN_AVG":
                pass
            else:
                raise LookupError(
                    f"Unhandled punt stat: `{stat}`"
                )
        punt_return_df_arr.append(temp_df)
        del temp_df

    # Stat cleaning
    if len(passing_df_arr) > 0:
        passing_df = pd.concat(passing_df_arr, ignore_index=True)
        # print(passing_df.columns)

        passing_df[["passing_COMP", "passing_ATT"]] = passing_df[
            "passing_COMP/ATT"
        ].str.split("/", expand=True)
        passing_df["player_jersey_number"] = passing_df[
            "player_jersey_number"
        ].infer_objects(copy=False).fillna(0)
        passing_df = passing_df.astype(
            {
                "player_jersey_number": "uint8",
                "passing_YDS": "int16",
                "passing_COMP": "uint16",
                "passing_ATT": "uint16",
                "passing_TD": "uint16",
                "passing_INT": "uint16",
                "passing_LONG": "int16",
            }
        )
        passing_df["passing_COMP%"] = round(
            passing_df["passing_COMP"] / passing_df["passing_ATT"], 3
        )
        passing_df["passing_YDS/ATT"] = round(
            passing_df["passing_YDS"] / passing_df["passing_ATT"], 3
        )
        passing_df["passing_AY/A"] = (
            (
                passing_df["passing_YDS"] +
                (passing_df["passing_TD"] * 20) -
                (passing_df["passing_INT"] * 45)
            ) / passing_df["passing_ATT"]
        )

    if len(rushing_df_arr) > 0:
        rushing_df = pd.concat(rushing_df_arr, ignore_index=True)
        rushing_df["player_jersey_number"] = rushing_df[
            "player_jersey_number"
        ].infer_objects(copy=False).fillna(0)
        rushing_df = rushing_df.astype(
            {
                "player_jersey_number": "uint8",
                "rushing_ATT": "uint16",
                "rushing_YDS": "int16",
            }
        )
        rushing_df["rushing_AVG"] = round(
            rushing_df["rushing_YDS"] / rushing_df["rushing_ATT"], 3
        )

    if len(receiving_df_arr) > 0:
        receiving_df = pd.concat(receiving_df_arr, ignore_index=True)
        # print(receiving_df.columns)
        receiving_df["player_jersey_number"] = receiving_df[
            "player_jersey_number"
        ].infer_objects(copy=False).fillna(0)
        receiving_df = receiving_df.astype(
            {
                "player_jersey_number": "uint8",
                "receiving_YDS": "int16",
                "receiving_TD": "uint16",
                "receiving_TGT": "uint16",
                "receiving_LONG": "int16",
                "receiving_YAC": "int16",
                "receiving_REC": "uint16",
            }
        )
        receiving_df.loc[
            receiving_df["receiving_REC"] > 0,
            "receiving_AVG"
        ] = round(
            receiving_df["receiving_YDS"] / receiving_df["receiving_REC"], 3
        )
        receiving_df.loc[
            receiving_df["receiving_REC"] > 0,
            "receiving_YDS/TGT"
        ] = round(
            receiving_df["receiving_YDS"] / receiving_df["receiving_TGT"], 3
        )
        receiving_df.loc[
            receiving_df["receiving_TGT"] > 0,
            "receiving_CATCH%"
        ] = round(
            receiving_df["receiving_REC"] / receiving_df["receiving_TGT"], 3
        )

    if len(defense_df_arr) > 0:
        defense_df = pd.concat(defense_df_arr, ignore_index=True)
        defense_df["player_jersey_number"] = defense_df[
            "player_jersey_number"
        ].infer_objects(copy=False).fillna(0)
        defense_df = defense_df.astype(
            {
                "player_jersey_number": "uint8",
            }
        )

    if len(kicking_df_arr) > 0:
        kicking_df = pd.concat(kicking_df_arr, ignore_index=True)
        kicking_df["player_jersey_number"] = kicking_df[
            "player_jersey_number"
        ].infer_objects(copy=False).fillna(0)
        kicking_df[["kicking_FGM", "kicking_FGA"]] = kicking_df[
            "kicking_FG"
        ].str.split("/", expand=True)
        # kicking_df = kicking_df.fillna(0)
        kicking_df = kicking_df.astype(
            {
                "player_jersey_number": "uint8",
                "kicking_XP": "uint16",
                "kicking_ROUGE": "uint16",
                # "kickoffs_NUM": "uint16",
                # "kickoffs_YDS": "int16",
                # "kickoffs_LONG": "int16",
                "kicking_FGM": "uint16",
                "kicking_FGA": "uint16",
            }
        )
        kicking_df.loc[kicking_df["kicking_FGA"] > 0, "kicking_FG%"] = round(
            kicking_df["kicking_FGM"] / kicking_df["kicking_FGA"], 3
        )

    if len(kickoffs_df_arr) > 0:
        kickoffs_df = pd.concat(kickoffs_df_arr, ignore_index=True)
        kickoffs_df["player_jersey_number"] = kickoffs_df[
            "player_jersey_number"
        ].infer_objects(copy=False).fillna(0)
        kickoffs_df = kickoffs_df.astype(
            {
                "player_jersey_number": "uint8",
                "kickoffs_NUM": "uint16",
                "kickoffs_YDS": "int16",
                "kickoffs_LONG": "int16",
            }
        )
        kickoffs_df.loc[
            kickoffs_df["kickoffs_NUM"] > 0,
            "kickoff_AVG"
        ] = round(
            kickoffs_df["kickoffs_YDS"] / kickoffs_df["kickoffs_NUM"], 3
        )

    if len(punting_df_arr) > 0:
        punting_df = pd.concat(punting_df_arr, ignore_index=True)
        punting_df["player_jersey_number"] = punting_df[
            "player_jersey_number"
        ].infer_objects(copy=False).fillna(0)
        punting_df = punting_df.astype(
            {
                "player_jersey_number": "uint8",
                "punting_NO": "uint16",
                "punting_GROSS_YDS": "int16",
                "punting_LONG": "int16",
                "punting_IN_10": "uint16",
            }
        )
        punting_df.loc[
            punting_df["punting_NO"] > 0,
            "punting_GROSS_AVG"
        ] = round(
            punting_df["punting_GROSS_YDS"] / punting_df["punting_NO"], 3
        )

    if len(kick_return_df_arr) > 0:
        kick_return_df = pd.concat(kick_return_df_arr, ignore_index=True)
        kick_return_df["player_jersey_number"] = kick_return_df[
            "player_jersey_number"
        ].infer_objects(copy=False).fillna(0)
        kick_return_df = kick_return_df.astype(
            {
                "player_jersey_number": "uint8",
                "kick_return_NUM": "uint16",
                "kick_return_YDS": "int16",
                "kick_return_LONG": "int16",
            }
        )
        kick_return_df.loc[
            kick_return_df["kick_return_NUM"] > 0,
            "kick_return_AVG"
        ] = round(
            kick_return_df["kick_return_YDS"] /
            kick_return_df["kick_return_NUM"],
            3
        )
        # print(kick_return_df.columns)

    if len(punt_return_df_arr) > 0:
        punt_return_df = pd.concat(punt_return_df_arr, ignore_index=True)
        punt_return_df["player_jersey_number"] = punt_return_df[
            "player_jersey_number"
        ].infer_objects(copy=False).fillna(0)
        punt_return_df = punt_return_df.astype(
            {
                "player_jersey_number": "uint8",
                "punt_return_NUM": "uint16",
                "punt_return_YDS": "int16",
                "punt_return_LONG": "int16",
            }
        )
        punt_return_df.loc[
            punt_return_df["punt_return_NUM"] > 0,
            "punt_return_AVG"
        ] = round(
            punt_return_df["punt_return_YDS"] /
            punt_return_df["punt_return_NUM"],
            3
        )

    # Combine it all into a single DataFrame
    stats_df = pd.merge(
        left=rushing_df,
        right=passing_df,
        how="outer",
        on=[
            "competitor_id",
            "player_jersey_number",
            "player_full_name",
            "player_abv_name",
            "player_position",
            "player_status",
        ]
    )

    if len(receiving_df) > 0:
        stats_df = stats_df.merge(
            right=receiving_df,
            how="outer",
            on=[
                "competitor_id",
                "player_jersey_number",
                "player_full_name",
                "player_abv_name",
                "player_position",
                "player_status",
            ]
        )

    if len(defense_df) > 0:
        stats_df = stats_df.merge(
            right=defense_df,
            how="outer",
            on=[
                "competitor_id",
                "player_jersey_number",
                "player_full_name",
                "player_abv_name",
                "player_position",
                "player_status",
            ]
        )

    if len(kicking_df) > 0:
        stats_df = stats_df.merge(
            right=kicking_df,
            how="outer",
            on=[
                "competitor_id",
                "player_jersey_number",
                "player_full_name",
                "player_abv_name",
                "player_position",
                "player_status",
            ]
        )

    if len(kickoffs_df) > 0:
        stats_df = stats_df.merge(
            right=kickoffs_df,
            how="outer",
            on=[
                "competitor_id",
                "player_jersey_number",
                "player_full_name",
                "player_abv_name",
                "player_position",
                "player_status",
            ]
        )

    if len(punting_df) > 0:
        stats_df = stats_df.merge(
            right=punting_df,
            how="outer",
            on=[
                "competitor_id",
                "player_jersey_number",
                "player_full_name",
                "player_abv_name",
                "player_position",
                "player_status",
            ]
        )

    if len(kick_return_df) > 0:
        stats_df = stats_df.merge(
            right=kick_return_df,
            how="outer",
            on=[
                "competitor_id",
                "player_jersey_number",
                "player_full_name",
                "player_abv_name",
                "player_position",
                "player_status",
            ]
        )

    if len(punt_return_df) > 0:
        stats_df = stats_df.merge(
            right=punt_return_df,
            how="outer",
            on=[
                "competitor_id",
                "player_jersey_number",
                "player_full_name",
                "player_abv_name",
                "player_position",
                "player_status",
            ]
        )

    # print(stats_df.columns)
    stats_df = stats_df.reindex(columns=columns_arr)
    return stats_df


def get_cfl_player_game_stats(season: int) -> pd.DataFrame:
    """ """
    now = datetime.now()
    stats_df = pd.DataFrame()
    stats_df_arr = []
    schedule_df = get_cfl_schedules(season=season)
    schedule_df = schedule_df[
        (schedule_df["team_1_score"] > 0) | (schedule_df["team_2_score"] > 0)
    ]
    # schedule_df = schedule_df[(schedule_df["eventTypeName"] != "Preseason")]
    schedule_df = schedule_df.dropna(subset=["fixtureId"])
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4)"
        + " AppleWebKit/537.36 (KHTML, like Gecko) "
        + "Chrome/125.0.0.0 Safari/537.36",
    }
    game_ids_arr = schedule_df["eventId"].to_list()
    fixture_ids_arr = schedule_df["fixtureId"].to_list()
    season_type_arr = schedule_df["eventTypeName"].to_list()

    for i in tqdm(range(0, len(game_ids_arr))):
        fixture_id = fixture_ids_arr[i]
        game_id = game_ids_arr[i]

        season_type = season_type_arr[i]

        if "grey cup" in season_type.lower():
            season_type = "Playoffs"

        # url = (
        #     "https://gsm-widgets.betstream.betgenius.com/widget-data/"
        #     + "multisportgametracker?productName=democfl_light"
        #     + f"&fixtureId={fixture_id}&activeContent=playerStats"
        #     + "&sport=AmericanFootball&sportId=17"
        #     + "&competitionId=1035&isUsingBetGeniusId=true"
        # )
        url = "https://gsm-widgets.betstream.betgenius.com/widget-data" +\
            "/multisportgametracker?productName=democfl_light" +\
            f"&fixtureId={fixture_id}&activeContent=playerStats" +\
            f"&sport=AmericanFootball&sportId=17&competitionId={game_id}" +\
            "&isUsingBetGeniusId=true"
        response = requests.get(url=url, headers=headers)

        json_data = json.loads(response.text)
        json_data = json_data["data"]
        try:
            temp_df = player_parser(json_data["playerStats"]["homeTeam"])
            temp_df["game_id"] = game_id
            temp_df["team_id"] = json_data[
                "matchInfo"
            ]["homeTeam"]["details"]["key"]
            temp_df["team_abv"] = json_data[
                "matchInfo"
            ]["homeTeam"]["details"]["abbreviation"]
            temp_df["team_name"] = json_data[
                "matchInfo"
            ]["homeTeam"]["fullName"]
            temp_df["season_type"] = season_type
            stats_df_arr.append(temp_df)
            del temp_df
        except Exception as e:
            logging.warning(
                f"\nUnhandled exception when parsing game ID {game_id} `{e}`"
            )

        try:
            temp_df = player_parser(json_data["playerStats"]["awayTeam"])
            temp_df["game_id"] = game_id
            temp_df["team_id"] = json_data[
                "matchInfo"
            ]["awayTeam"]["details"]["key"]
            temp_df["team_abv"] = json_data[
                "matchInfo"
            ]["awayTeam"]["details"]["abbreviation"]
            temp_df["team_name"] = json_data[
                "matchInfo"
            ]["awayTeam"]["fullName"]
            temp_df["season_type"] = season_type
            stats_df_arr.append(temp_df)

            del temp_df

        except Exception as e:
            logging.warning(
                f"\nUnhandled exception when parsing game ID {game_id} `{e}`"
            )

        time.sleep(2)
    if len(stats_df_arr) > 0:
        stats_df = pd.concat(stats_df_arr, ignore_index=True)
        stats_df["last_updated"] = now.isoformat()

        stats_df.to_csv(
            f"player_stats/game_stats/{season}_cfl_player_game_stats.csv",
            index=False
        )

    return stats_df


def get_cfl_team_game_stats(season: int) -> pd.DataFrame:
    """ """
    now = datetime.now()
    stats_df = pd.DataFrame()
    stats_df_arr = []
    temp_df = pd.DataFrame()
    schedule_df = get_cfl_schedules(season=season)
    schedule_df = schedule_df[
        (schedule_df["team_1_score"] > 0) | (schedule_df["team_2_score"] > 0)
    ]
    # schedule_df = schedule_df[(schedule_df["eventTypeName"] != "Preseason")]
    schedule_df = schedule_df.dropna(subset=["fixtureId"])
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4)"
        + " AppleWebKit/537.36 (KHTML, like Gecko) "
        + "Chrome/125.0.0.0 Safari/537.36",
    }
    game_ids_arr = schedule_df["eventId"].to_list()
    fixture_ids_arr = schedule_df["fixtureId"].to_list()
    season_type_arr = schedule_df["eventTypeName"].to_list()

    for i in tqdm(range(0, len(game_ids_arr))):
        fixture_id = fixture_ids_arr[i]
        game_id = game_ids_arr[i]

        season_type = season_type_arr[i]

        away_passing = []
        away_rushing = []
        away_offense = []
        away_first_downs = []
        away_sacks = []
        away_top = ""
        away_second_down = []
        away_third_down = ""
        away_third_down_ATT = 0
        away_third_down_CONV = 0
        away_giveaways = []
        away_fumbles = []
        away_penalties = []
        away_rz = []
        away_tfl = []
        away_blocks = 0
        away_int = []
        away_fr = []
        away_fg = []
        away_punts = []
        away_pr = []
        away_kr = []
        away_fgr = []
        away_rz = ""
        away_turnovers = 0

        home_passing = []
        home_rushing = []
        home_offense = []
        home_first_downs = []
        home_sacks = []
        home_top = ""
        home_second_down = []
        home_third_down = ""
        home_third_down_ATT = 0
        home_third_down_CONV = 0
        home_giveaways = []
        home_fumbles = []
        home_penalties = []
        home_rz = []
        home_tfl = []
        home_blocks = 0
        home_int = []
        home_fr = []
        home_fg = []
        home_punts = []
        home_pr = []
        home_kr = []
        home_fgr = []
        home_rz = ""
        home_turnovers = 0

        if "grey cup" in season_type.lower():
            season_type = "Playoffs"

        # url = (
        #     "https://gsm-widgets.betstream.betgenius.com/widget-data/"
        #     + "multisportgametracker?productName=democfl_light"
        #     + f"&fixtureId={fixture_id}&activeContent=playerStats"
        #     + "&sport=AmericanFootball&sportId=17"
        #     + "&competitionId=1035&isUsingBetGeniusId=true"
        # )
        url = "https://gsm-widgets.betstream.betgenius.com/widget-data" +\
            "/multisportgametracker?productName=democfl_light" +\
            f"&fixtureId={fixture_id}&activeContent=teamStats" +\
            "&sport=AmericanFootball&sportId=17"
        response = requests.get(url=url, headers=headers)
        time.sleep(2)

        json_data = json.loads(response.text)

        if "fixture information not available" in response.text.lower():
            continue
        json_data = json_data["data"]

        away_team_id = json_data["matchInfo"]["awayTeam"]["competitorId"]
        away_team_abv = json_data["matchInfo"]["awayTeam"]["details"][
            "abbreviation"
        ]
        away_team_name = json_data["matchInfo"]["awayTeam"]["details"][
            "officialName"
        ]
        away_points = json_data["scoreboardInfo"]["awayScore"]

        home_team_id = json_data["matchInfo"]["homeTeam"]["competitorId"]
        home_team_abv = json_data["matchInfo"]["homeTeam"]["details"][
            "abbreviation"
        ]
        home_team_name = json_data["matchInfo"]["homeTeam"]["details"][
            "officialName"
        ]
        home_points = json_data["scoreboardInfo"]["homeScore"]

        for data in json_data["teamStats"]:
            if data["id"] == "passing_detailed":
                away_passing = data["away"]
                home_passing = data["home"]
            elif data["id"] == "rushing_detailed":
                away_rushing = data["away"]
                home_rushing = data["home"]
            elif data["id"] == "net_offense_detailed":
                away_offense = data["away"]
                home_offense = data["home"]
            elif data["id"] == "first_down_detailed":
                away_first_downs = data["away"]
                home_first_downs = data["home"]
            elif data["id"] == "sacks_for_detailed":
                away_sacks = data["away"]
                home_sacks = data["home"]
            elif data["id"] == "time_possession":
                away_top = data["away"]
                home_top = data["home"]
            elif data["id"] == "second_down_detailed":
                away_second_down = data["away"]
                home_second_down = data["home"]
            elif data["id"] == "third_down_efficiency":
                away_third_down = data["away"]
                home_third_down = data["home"]
                home_third_down_CONV, home_third_down_ATT = \
                    home_third_down.split("/")

                home_third_down_CONV = int(home_third_down_CONV)
                home_third_down_ATT = int(home_third_down_ATT)

                away_third_down_CONV, away_third_down_ATT = \
                    away_third_down.split("/")

                away_third_down_CONV = int(away_third_down_CONV)
                away_third_down_ATT = int(away_third_down_ATT)

            elif data["id"] == "giveaways_detailed":
                away_giveaways = data["away"]
                home_giveaways = data["home"]
            elif data["id"] == "fumbles_detailed":
                away_fumbles = data["away"]
                home_fumbles = data["home"]
            elif data["id"] == "penalties_detailed":
                away_penalties = data["away"]
                home_penalties = data["home"]
            elif data["id"] == "red_zone_detailed":
                away_rz = data["away"]
                home_rz = data["home"]
            elif data["id"] == "tackles_loss":
                away_tfl = data["away"]
                home_tfl = data["home"]
            elif data["id"] == "blocked_kicks":
                away_blocks = data["away"][0]
                home_blocks = data["home"][0]
            elif data["id"] == "interception_returns_detailed":
                away_int = data["away"]
                home_int = data["home"]
            elif data["id"] == "fumbles_return_detailed":
                away_fr = data["away"]
                home_fr = data["home"]
            elif data["id"] == "field_goals_detailed":
                away_fg = data["away"]
                home_fg = data["home"]
            elif data["id"] == "punting_detailed":
                away_punts = data["away"]
                home_punts = data["home"]
            elif data["id"] == "punt_returns_detailed":
                away_pr = data["away"]
                home_pr = data["home"]
            elif data["id"] == "kickoff_returns_detailed":
                away_kr = data["away"]
                home_kr = data["home"]
            elif data["id"] == "fgm_returns_detailed":
                away_fgr = data["away"]
                home_fgr = data["home"]
            elif data["id"] == "red_zone":
                away_rz = data["away"]
                home_rz = data["home"]
            elif data["id"] == "turnovers":
                away_turnovers = data["away"][0]
                home_turnovers = data["home"][0]

            # Skipping because these stats are covered somewhere else.
            elif data["id"] == "touchdowns":
                pass
            elif data["id"] == "total_net_yards":
                pass
            elif data["id"] == "offensive_plays":
                pass
            elif data["id"] == "field_goals":
                pass
            elif data["id"] == "punts":
                pass
            elif data["id"] == "yards_per_play":
                pass
            elif data["id"] == "passing_yards":
                pass
            elif data["id"] == "passing_yards_att":
                pass
            elif data["id"] == "first_down":
                pass
            elif data["id"] == "team_losses_detailed":
                pass
            elif data["id"] == "rushing_yards":
                pass
            elif data["id"] == "yards_per_rush":
                pass
            elif data["id"] == "second_down_efficiency":
                pass
            else:
                raise ValueError(
                    "Unhandled stat category: " +
                    data["id"]
                )

        temp_df = pd.DataFrame(
            {
                # "season":[season, season],
                "team_id": [away_team_id, home_team_id],
                "team_abv": [away_team_abv, home_team_abv],
                "team_name": [away_team_name, home_team_name],
                "points_scored": [away_points, home_points],
                "points_allowed": [home_points, away_points],
                "team_time_of_possession": [away_top, home_top],
                "penalties_NUM": [away_penalties[0], home_penalties[0]],
                "penalties_YDS": [away_penalties[1], home_penalties[1]],
                "offense_PLAYS": [away_offense[1], home_offense[1]],
                "offense_YDS": [away_offense[0], home_offense[0]],
                "offense_YDS/PLAY": [away_offense[2], home_offense[2]],
                "offense_first_downs_passing": [
                    away_first_downs[1],
                    home_first_downs[1],
                ],
                "offense_first_downs_rushing": [
                    away_first_downs[0],
                    home_first_downs[0],
                ],
                "offense_first_downs_penalty": [
                    away_first_downs[2],
                    home_first_downs[2],
                ],
                "offense_turnovers": [away_turnovers, home_turnovers],
                "second_downs_ATT": [away_second_down[0], home_second_down[0]],
                "second_downs_MADE": [
                    away_second_down[1],
                    home_second_down[1]
                ],
                "third_downs_ATT": [away_third_down_ATT, home_third_down_ATT],
                "third_downs_MADE": [
                    away_third_down_CONV,
                    home_third_down_CONV
                ],
                "red_zone_OPP": [away_rz[0], home_rz[0]],
                "red_zone_TD": [away_rz[1], home_rz[1]],
                "second_downs_PCT": [away_second_down[2], home_second_down[2]],
                "passing_ATT": [away_passing[1], home_passing[1]],
                "passing_YDS": [away_passing[0], home_passing[0]],
                "passing_INT": [away_giveaways[1], home_giveaways[1]],
                "passing_YDS/ATT": [away_passing[2], home_passing[2]],
                "rushing_ATT": [away_rushing[1], home_rushing[1]],
                "rushing_YDS": [away_rushing[0], home_rushing[0]],
                "rushing_YDS/ATT": [away_rushing[2], home_rushing[2]],
                "fumbles_NUM": [away_fumbles[0], home_fumbles[0]],
                "fumbles_LOST": [away_fumbles[1], home_fumbles[1]],
                "fumbles_FR": [away_fr[0], home_fr[0]],
                "fumbles_FR_YDS": [away_fr[1], home_fr[1]],
                "defense_TFL": [away_tfl, home_tfl],
                "defense_SACKS": [away_sacks[0], home_sacks[0]],
                "defense_SACK_YDS": [away_sacks[1], home_sacks[1]],
                "defense_BLK": [away_blocks, home_blocks],
                "defense_INT": [away_int[0], home_int[0]],
                "defense_INT_YDS": [away_int[1], home_int[1]],
                "kicking_FGM": [away_fg[1], home_fg[1]],
                "kicking_FGA": [away_fg[0], home_fg[0]],
                "kicking_FG_LONG": [away_fg[2], home_fg[2]],
                "punting_NUM": [away_punts[0], home_punts[0]],
                "punting_GROSS_YDS": [away_punts[1], home_punts[1]],
                "punting_GROSS_AVG": [away_punts[2], home_punts[2]],
                "punt_return_NUM": [away_pr[0], home_pr[0]],
                "punt_return_YDS": [away_pr[1], home_pr[1]],
                "punt_return_AVG": [away_pr[2], home_pr[2]],
                "kick_return_NUM": [away_kr[0], home_kr[0]],
                "kick_return_YDS": [away_kr[1], home_kr[1]],
                "kick_return_AVG": [away_kr[2], home_kr[2]],
                "missed_fg_return_NUM": [away_fgr[0], home_fgr[0]],
                "missed_fg_return_YDS": [away_fgr[1], home_fgr[1]],
            },
            # index=[0],
        )

        temp_df["season"] = season
        temp_df["game_id"] = game_id
        temp_df["fixture_id"] = fixture_id
        temp_df["season_type"] = season_type
        # temp_df["points_scored"] = season_type
        # temp_df["points_scored"] = season_type

        stats_df_arr.append(temp_df)
        del temp_df
        # print()

    if len(stats_df_arr) > 0:
        stats_df = pd.concat(stats_df_arr, ignore_index=True)
        stats_df["last_updated"] = now.isoformat()
        stats_df.to_csv(
            f"team_stats/game_stats/{season}_cfl_team_game_stats.csv",
            index=False
        )
    return stats_df


if __name__ == "__main__":
    now = datetime.now()
    try:
        os.mkdir("player_stats")
    except FileExistsError:
        logging.info("`./player_stats` already exists.")
    try:
        os.mkdir("player_stats/game_stats")
    except FileExistsError:
        logging.info("`./player_stats/game_stats` already exists.")

    try:
        os.mkdir("team_stats")
    except FileExistsError:
        logging.info("`./team_stats` already exists.")
    try:
        os.mkdir("team_stats/game_stats")
    except FileExistsError:
        logging.info("`./team_stats/game_stats` already exists.")

    for i in range(now.year, now.year+1):
        print(f"Getting {i} player game stats.")
        print(get_cfl_player_game_stats(i))
        print(f"Getting {i} team game stats.")
        print(get_cfl_team_game_stats(i))
