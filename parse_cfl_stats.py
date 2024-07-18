import logging
import os
from datetime import UTC, datetime

import pandas as pd


def parse_cfl_player_season_stats(season: int):
    """ """
    columns = [
        "season",
        "team_id",
        "team_abv",
        "team_name",
        "season_type",
        "season",
        "competitor_id",
        "player_full_name",
        "games_played",
        "passing_COMP",
        "passing_ATT",
        "passing_COMP%",
        "passing_YDS",
        "passing_TD",
        "passing_INT",
        "passing_LONG",
        "passing_Y/A",
        "passing_AY/A",
        "passing_Y/C",
        "passing_NFL_QBR",
        "passing_CFB_QBR",
        "rushing_ATT",
        "rushing_YDS",
        "rushing_AVG",
        "rushing_TD",
        "rushing_LONG",
        "receiving_TGT",
        "receiving_REC",
        "receiving_AVG",
        "receiving_YDS",
        "receiving_YAC",
        "receiving_TD",
        "receiving_CATCH%",
        "receiving_YDS/TGT",
        "receiving_LONG",
        "defense_SOLO",
        "defense_SACKS",
        "defense_TFL",
        "defense_INT",
        "defense_FF",
        "defense_FR",
        "defense_ST_TAK",
        "kicking_FGM",
        "kicking_FGA",
        "kicking_FG%",
        "kicking_FG_LONG",
        "kicking_XP",
        "kicking_ROUGE",
        "kickoffs_NUM",
        "kickoffs_YDS",
        "kickoffs_LONG",
        "punting_NO",
        "punting_GROSS_YDS",
        "punting_GROSS_AVG",
        "punting_LONG",
        "punting_IN_10",
        "punting_IN_10%",
        "kick_return_NUM",
        "kick_return_YDS",
        "kick_return_AVG",
        "kick_return_LONG",
        "punt_return_NUM",
        "punt_return_AVG",
        "punt_return_YDS",
        "punt_return_LONG",
        "last_updated",
    ]

    base_df = pd.read_csv(
        f"player_stats/game_stats/{season}_cfl_player_game_stats.csv"
    )

    base_df["competitor_id"] = base_df["competitor_id"].fillna(
        value=-1,
        # inplace=True
    )
    base_df.loc[base_df["competitor_id"] == -1, "player_full_name"] = "TEAM"
    final_df = base_df.groupby(
        [
            "team_id",
            "team_abv",
            "team_name",
            "season_type",
            "season",
            "competitor_id",
            "player_full_name",
        ],
        group_keys=False,
        as_index=False,
    ).agg(
        {
            "game_id": "count",
            "passing_COMP": "sum",
            "passing_ATT": "sum",
            "passing_YDS": "sum",
            "passing_TD": "sum",
            "passing_INT": "sum",
            "passing_LONG": "max",
            "rushing_ATT": "sum",
            "rushing_YDS": "sum",
            "rushing_TD": "sum",
            "rushing_LONG": "max",
            "receiving_TGT": "sum",
            "receiving_REC": "sum",
            "receiving_YDS": "sum",
            "receiving_YAC": "sum",
            "receiving_TD": "sum",
            "receiving_LONG": "max",
            "defense_SOLO": "sum",
            "defense_SACKS": "sum",
            "defense_TFL": "sum",
            "defense_INT": "sum",
            "defense_FF": "sum",
            "defense_FR": "sum",
            "defense_ST_TAK": "sum",
            "kicking_FGM": "sum",
            "kicking_FGA": "sum",
            "kicking_FG_LONG": "max",
            "kicking_XP": "sum",
            "kicking_ROUGE": "sum",
            "kickoffs_NUM": "sum",
            "kickoffs_YDS": "sum",
            "kickoffs_LONG": "max",
            "punting_NO": "sum",
            "punting_GROSS_YDS": "sum",
            "punting_LONG": "max",
            "punting_IN_10": "sum",
            "kick_return_NUM": "sum",
            "kick_return_YDS": "sum",
            "kick_return_LONG": "sum",
            "punt_return_NUM": "sum",
            "punt_return_YDS": "sum",
            "punt_return_LONG": "max",
        }
    )

    final_df.rename(columns={"game_id": "games_played"}, inplace=True)
    final_df.loc[final_df["passing_ATT"] > 0, "passing_COMP%"] = (
        final_df["passing_COMP"] / final_df["passing_ATT"]
    )
    final_df.loc[final_df["passing_ATT"] > 0, "passing_Y/A"] = (
        final_df["passing_YDS"] / final_df["passing_ATT"]
    )
    final_df.loc[final_df["passing_ATT"] > 0, "passing_AY/A"] = (
        final_df["passing_YDS"]
        + (final_df["passing_TD"] * 20)
        - (final_df["passing_INT"] * 45)
    ) / final_df["passing_ATT"]
    final_df.loc[final_df["passing_COMP"] > 0, "passing_Y/C"] = (
        final_df["passing_YDS"] / final_df["passing_COMP"]
    )

    # NFL Passer Rating segments
    final_df.loc[final_df["passing_ATT"] > 0, "passing_NFL_QBR_A"] = (
        (final_df["passing_COMP"] / final_df["passing_ATT"]) - 0.3
    ) * 5
    final_df.loc[final_df["passing_ATT"] > 0, "passing_NFL_QBR_B"] = (
        (final_df["passing_YDS"] / final_df["passing_ATT"]) - 3
    ) * 0.25
    final_df.loc[final_df["passing_ATT"] > 0, "passing_NFL_QBR_C"] = (
        final_df["passing_TD"] / final_df["passing_ATT"]
    ) * 20
    final_df.loc[final_df["passing_ATT"] > 0, "passing_NFL_QBR_D"] = 2.375 - (
        (final_df["passing_INT"] / final_df["passing_ATT"]) * 25
    )

    # Yes, this is a required step in the formula.
    final_df.loc[
        final_df["passing_NFL_QBR_A"] > 2.375, "passing_NFL_QBR_A"
    ] = 2.375
    final_df.loc[
        final_df["passing_NFL_QBR_A"] > 2.375, "passing_NFL_QBR_B"
    ] = 2.375
    final_df.loc[
        final_df["passing_NFL_QBR_C"] > 2.375, "passing_NFL_QBR_C"
    ] = 2.375
    final_df.loc[
        final_df["passing_NFL_QBR_D"] > 2.375, "passing_NFL_QBR_D"
    ] = 2.375

    # See above comment.
    final_df.loc[final_df["passing_NFL_QBR_A"] < 0, "passing_NFL_QBR_A"] = 0
    final_df.loc[final_df["passing_NFL_QBR_A"] < 0, "passing_NFL_QBR_B"] = 0
    final_df.loc[final_df["passing_NFL_QBR_C"] < 0, "passing_NFL_QBR_C"] = 0
    final_df.loc[final_df["passing_NFL_QBR_D"] < 0, "passing_NFL_QBR_D"] = 0

    final_df.loc[final_df["passing_ATT"] > 0, "passing_NFL_QBR"] = (
        (
            final_df["passing_NFL_QBR_A"]
            + final_df["passing_NFL_QBR_B"]
            + final_df["passing_NFL_QBR_C"]
            + final_df["passing_NFL_QBR_D"]
        )
        / 6
    ) * 100

    final_df.loc[final_df["passing_ATT"] > 0, "passing_CFB_QBR"] = (
        (final_df["passing_YDS"] * 8.4)
        + (final_df["passing_COMP"] * 100)
        + (final_df["passing_TD"] * 330)
        - (final_df["passing_INT"] * 200)
    ) / final_df["passing_ATT"]

    final_df.loc[final_df["rushing_ATT"] > 0, "rushing_AVG"] = (
        final_df["rushing_YDS"] / final_df["rushing_ATT"]
    )

    final_df.loc[final_df["receiving_REC"] > 0, "receiving_AVG"] = (
        final_df["receiving_YDS"] / final_df["receiving_REC"]
    )

    final_df.loc[final_df["receiving_TGT"] > 0, "receiving_CATCH%"] = (
        final_df["receiving_REC"] / final_df["receiving_TGT"]
    )

    final_df.loc[final_df["receiving_TGT"] > 0, "receiving_YDS/TGT"] = (
        final_df["receiving_YDS"] / final_df["receiving_TGT"]
    )

    final_df.loc[final_df["kicking_FGA"] > 0, "kicking_FG%"] = (
        final_df["kicking_FGM"] / final_df["kicking_FGA"]
    )

    final_df.loc[final_df["punting_NO"] > 0, "punting_GROSS_AVG"] = (
        final_df["punting_GROSS_YDS"] / final_df["punting_NO"]
    )

    # final_df.loc[final_df["punting_NO"] > 0, "punting_TB%"] = (
    #     final_df["punting_TB"] / final_df["punting_NO"]
    # )

    final_df.loc[final_df["punting_NO"] > 0, "punting_IN_10%"] = (
        final_df["punting_IN_10"] / final_df["punting_NO"]
    )

    final_df.loc[final_df["kick_return_NUM"] > 0, "kick_return_AVG"] = (
        final_df["kick_return_YDS"] / final_df["kick_return_NUM"]
    )

    final_df.loc[final_df["punt_return_NUM"] > 0, "punt_return_AVG"] = (
        final_df["punt_return_YDS"] / final_df["punt_return_NUM"]
    )
    # final_df = final_df.reindex(columns=columns)

    final_df = final_df.round(
        {
            "passing_COMP%": 4,
            "passing_Y/A": 3,
            "passing_AY/A": 3,
            "passing_Y/C": 3,
            "passing_NFL_QBR": 3,
            "passing_CFB_QBR": 3,
            "rushing_AVG": 3,
            "receiving_AVG": 3,
            "receiving_CATCH%": 4,
            "receiving_YDS/TGT": 3,
            "kicking_FG%": 4,
            "punting_GROSS_AVG": 3,
            "punting_IN_20%": 4,
            "punting_TB%": 4,
            "kick_return_AVG": 3,
            "punt_return_AVG": 3,
        }
    )
    final_df["last_updated"] = datetime.now(UTC).isoformat()
    final_df["season"] = season
    final_df = final_df.reindex(columns=columns)
    final_df = final_df.round(
        {
            "passing_COMP%": 4,
            "passing_Y/A": 3,
            "passing_AY/A": 3,
            "passing_Y/C": 3,
            "passing_NFL_QBR": 3,
            "passing_CFB_QBR": 3,
            "rushing_AVG": 3,
            "receiving_AVG": 3,
            "receiving_CATCH%": 4,
            "receiving_YDS/TGT": 3,
            "kicking_FG%": 4,
            "punting_GROSS_AVG": 3,
            "punting_IN_20%": 4,
            "punting_TB%": 4,
            "kick_return_AVG": 3,
            "punt_return_AVG": 3,
        }
    )
    final_df.to_csv(
        f"player_stats/season_stats/{season}_cfl_player_season_stats.csv",
        index=False
    )
    # print(final_df.columns)
    return final_df


if __name__ == "__main__":
    now = datetime.now()
    try:
        os.mkdir("player_stats")
    except FileExistsError:
        logging.info("`./player_stats` already exists.")

    try:
        os.mkdir("player_stats/season_stats")
    except FileExistsError:
        logging.info("`./player_stats/season_stats` already exists.")

    for i in range(now.year, now.year+1):
        if i == 2020:
            # Catch the 2020 season, because no CFL games were played
            # due to COVID-19.
            continue
        print(parse_cfl_player_season_stats(i))
