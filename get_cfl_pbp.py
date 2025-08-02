import json
import logging
import os
import re
import time
from datetime import datetime

import pandas as pd
import requests
from tqdm import tqdm

from get_cfl_rosters import get_stats_crew_cfl_rosters
from get_schedules import get_cfl_schedules


def get_yardline(yardline: str, posteam: str) -> int:
    field_length = 110
    midfield_yrd_line = field_length / 2

    try:
        yardline_temp = re.findall(
            r"([\-0-9]+)",
            yardline
        )[0]
    except Exception as e:
        logging.info(
            f"Cannot get a yardline number with {yardline}." +
            f"Full exception {e}"
        )
        yardline_100 = yardline

    if (posteam in yardline) and ("end zone" in yardline.lower()):
        yardline_100 = field_length
    elif (posteam not in yardline) and ("end zone" in yardline.lower()):
        yardline_100 = 0
    elif posteam in yardline:
        yardline_temp = int(yardline_temp)
        yardline_100 = field_length - yardline_temp
    else:
        yardline_temp = int(yardline_temp)
        yardline_100 = field_length - (midfield_yrd_line - yardline_temp) -\
            midfield_yrd_line

    return yardline_100


def get_player_chain(
    season: int,
    away_team_abv: str,
    home_team_abv: str
) -> dict:
    """ """
    player_names_arr = []
    player_chain_arr = {}

    try:
        roster_df = pd.read_csv(
            f"rosters/{season}_stats_crew_cfl_rosters.csv"
        )
    except Exception:
        roster_df = get_stats_crew_cfl_rosters(season)

    roster_df = roster_df[
        (roster_df["team_id"] == away_team_abv) |
        (roster_df["team_id"] == home_team_abv)
    ]
    f_name_arr = roster_df["player_first_name"].to_numpy()
    l_name_arr = roster_df["player_last_name"].to_numpy()
    full_name_arr = roster_df["player_full_name"].to_numpy()
    p_id_arr = roster_df["stats_crew_player_id"].to_numpy()

    for i in range(0, len(p_id_arr)):
        f_name = f_name_arr[i]
        l_name = l_name_arr[i]
        full_name = full_name_arr[i]
        p_id = p_id_arr[i]

        player_name = f"{f_name[0]}.{l_name}"
        player_name_alt = f"{f_name[0]}. {l_name}"

        player_names_arr.append(player_name)
        player_names_arr.append(player_name_alt)

        player_chain_arr[player_name] = p_id
        player_chain_arr[player_name_alt] = p_id

        player_name = f"{f_name[0:2]}.{l_name}"
        player_name_alt = f"{f_name[0:2]}. {l_name}"

        player_names_arr.append(player_name)
        player_names_arr.append(player_name_alt)
        player_names_arr.append(full_name)

        player_chain_arr[player_name] = p_id
        player_chain_arr[player_name_alt] = p_id
        player_chain_arr[full_name] = p_id

        if l_name in player_names_arr:
            pass
        else:
            player_names_arr.append(l_name)
            player_chain_arr[l_name] = p_id
    return player_chain_arr


def parser(
    pbp_data: dict,
    # quarter_num: int,
    away_team_abv: str,
    away_team_id: int,
    home_team_abv: str,
    home_team_id: int,
    total_home_score: int,
    total_away_score: int,
    is_home_opening_kickoff: bool = False
) -> pd.DataFrame:
    """ """
    pbp_df = pd.DataFrame()
    pbp_df_arr = []
    temp_df = pd.DataFrame()

    posteam = ""
    defteam = ""
    home_opening_kickoff = is_home_opening_kickoff
    yardline_100 = 0
    # In the CFL, both sides get 2 timeouts per half, instead of 3.
    # https://cfldb.ca/rulebook/conduct-of-the-game/starting-and-timing/
    home_timeouts_remaining = 2
    away_timeouts_remaining = 2
    posteam_timeouts_remaining = 2
    defteam_timeouts_remaining = 2
    # total_home_score = home_points
    # total_away_score = away_points
    posteam_score = 0
    defteam_score = 0
    score_differential = 0
    posteam_score_post = 0
    defteam_score_post = 0
    score_differential_post = 0

    for p in range(len(pbp_data)-1, -1, -1):
        play = pbp_data[p]
        play["description"] = play["description"].replace("()", "")
        play["description"] = play["description"].replace("Jr,)", "Jr.)")
        # Fixes an issue that would otherwise completely break the regex code.
        play["description"] = play["description"].replace("Ottawa", "OTT")
        play["description"] = play["description"].replace("ottawa", "OTT")

        kicker_player_name = ""
        kickoff_returner_player_name = ""
        return_team = ""
        passer_player_name = ""
        pass_length = ""
        pass_location = ""
        receiver_player_name = ""
        pass_defense_1_player_name = ""
        penalty_team = ""
        penalty_type = ""
        solo_tackle_1_team = ""
        solo_tackle_1_player_name = ""
        solo_tackle_2_team = ""
        solo_tackle_2_player_name = ""
        fumbled_1_team = ""
        fumbled_1_player_name = ""
        forced_fumble_player_1_team = ""
        forced_fumble_player_1_player_name = ""
        fumble_recovery_1_team = ""
        fumble_recovery_1_player_name = ""
        penalty_player_name = ""
        tackle_for_loss_1_player_name = ""
        sack_player_name = ""
        rusher_player_name = ""
        run_location = ""
        punter_player_name = ""
        punt_returner_player_name = ""
        special_teams_play_type = ""
        field_goal_result = ""
        extra_point_result = ""
        two_point_conv_result = ""
        missed_fg_return_team = ""
        missed_fg_return_player_name = ""
        assist_tackle_1_player_name = ""
        assist_tackle_1_team = ""
        assist_tackle_2_player_name = ""
        assist_tackle_2_team = ""
        tackle_for_loss_2_player_name = ""
        half_sack_1_player_name = ""
        half_sack_2_player_name = ""
        interception_player_name = ""
        blocked_player_name = ""
        lateral_punt_returner_player_name = ""
        lateral_rusher_player_name = ""
        replay_or_challenge_result = ""
        lateral_fumble_recovery_player_name = ""
        lateral_fumble_recovery_team = ""
        td_team = ""
        td_player_name = ""
        safety_player_name = ""
        lateral_interception_player_name = ""

        kick_distance = 0
        air_yards = 0
        yards_gained = 0
        passing_yards = 0
        fumble_recovery_1_yards = 0
        penalty_yards = 0
        rushing_yards = 0
        return_yards = 0
        missed_fg_return_yards = 0
        lateral_return_yards = 0
        lateral_rusher_yards = 0
        yards_after_catch = 0
        yds_net = None
        receiving_yards = 0
        punt_end_yl = None
        kickoff_end_yl = None

        is_shotgun = False
        is_qb_dropback = False
        is_pass = False
        is_complete_pass = False
        is_incomplete_pass = False
        is_pass_touchdown = False
        is_first_down = False
        is_first_down_pass = False
        is_second_down_converted = False
        is_second_down_failed = False
        is_third_down_converted = False
        is_third_down_failed = False
        is_fourth_down_converted = False
        is_fourth_down_failed = False
        is_interception = False
        is_fumble_forced = False
        is_fumble_not_forced = False
        is_fumble = False
        is_first_down_penalty = False
        is_scrimmage_play = False
        is_penalty = False
        is_sack = False
        is_rush = False
        is_no_play = False
        is_no_huddle = False
        is_qb_kneel = False
        is_first_down_rush = False
        is_punt = False
        is_punt_downed = False
        is_rouge = False
        is_special_teams_play = False
        is_punt_out_of_bounds = False
        is_field_goal_attempt = False
        is_kickoff_attempt = False
        is_extra_point_attempt = False
        is_fumble_lost = False
        is_two_point_attempt = False
        is_touchdown = False
        is_rush_touchdown = False
        is_return_touchdown = False
        is_qb_spike = False
        is_assist_tackle = False
        is_touchback = False
        is_aborted_play = False
        is_lateral_return = False
        is_replay_or_challenge = False
        is_defensive_extra_point_attempt = False
        is_defensive_extra_point_conv = False
        is_punt_blocked = False
        is_lateral_recovery = False
        is_goal_to_go = False
        is_out_of_bounds = False
        is_safety = False
        is_punt_inside_twenty = False
        is_punt_in_endzone = False
        is_fumble_out_of_bounds = False
        is_kickoff_inside_twenty = False
        is_kickoff_in_endzone = False
        is_kickoff_out_of_bounds = False
        is_kickoff_downed = False
        is_kickoff_fair_catch = False
        is_successful_play = False

        # if posteam == home_team_abv:
        #     posteam_score = total_home_score
        #     defteam_score = total_away_score
        # elif posteam == away_team_abv:
        #     posteam_score = total_away_score
        #     defteam_score = total_home_score

        # posteam_score = posteam_score_post
        # defteam_score = defteam_score_post
        # score_differential = score_differential_post

        if play["teamId"] == home_team_id:
            posteam = home_team_abv
            defteam = away_team_abv
            posteam_score = total_home_score
            posteam_score_post = posteam_score
            defteam_score = total_away_score
            posteam_score_post = posteam_score
            defteam_score_post = defteam_score
            posteam_type = "home"
        elif play["teamId"] == away_team_id:
            posteam = away_team_abv
            defteam = home_team_abv
            posteam_score = total_away_score
            defteam_score = total_home_score
            posteam_score_post = posteam_score
            defteam_score_post = defteam_score
            posteam_type = "away"
        else:
            raise ValueError(
                "Unhandled team ID when parsing the following play:\n" +
                f"{play}"
            )

        # Timestamp
        play_timestamp = datetime.fromtimestamp(
            play["timestamp"] / 1000
        )
        play_date = play_timestamp.strftime("%Y-%m-%d")
        play_timestamp = play_timestamp.isoformat()

        # Play ID
        play_id_raw = play["id"]
        drive_num, p_id_2 = play_id_raw.split("-")
        drive_num = int(drive_num)
        p_id_2 = int(p_id_2)
        play_id = f"{drive_num:02d}{p_id_2:03d}"
        play_id = int(play_id)

        del play_id_raw
        del p_id_2

        if drive_num == 0 and play["teamId"] == home_team_id:
            home_opening_kickoff = True
        # else:
        #     home_opening_kickoff = False

        if len(play["playStartPosition"]) > 0:
            down_and_distance_arr = re.findall(
                r"([0-9])[a-zA-Z]+ & ([\-0-9]+) at ([a-zA-Z0-9\-\s]+)",
                play["playStartPosition"]
            )
            down = int(down_and_distance_arr[0][0])
            yds_to_go = int(down_and_distance_arr[0][1])
            if yds_to_go is None:
                raise ValueError(
                    "Something went wrong"
                )
            yrdln = down_and_distance_arr[0][2]
            # del down_and_distance_arr
            try:
                side_of_field = re.findall("([a-zA-Z]+)", yrdln)[0]
            except Exception:
                # Yes this is probably bad.
                # No, there isn't a better solution.
                side_of_field = posteam
            yardline_100 = get_yardline(yrdln, posteam)
            if yds_to_go == yardline_100:
                is_goal_to_go = True
        else:
            # if down is None:
            #     down = 0
            # if yds_to_go is None:
            #     yds_to_go = 0

            down = 0
            yds_to_go = 0
            yrdln = None
            side_of_field = None
            # yardline_100 = None

        if play["phase"].lower() == "overtime":
            game_half = "Overtime"
        elif int(play["phaseQualifier"]) <= 2:
            game_half = "Half1"
        elif int(play["phaseQualifier"]) >= 3:
            game_half = "Half2"

        time = play["clock"]
        if play["id"] == "0-1" and len(time) == 0:
            time = "15:00"
        if len(time) > 0:
            time_min, time_sec = time.split(":")
            time_min = int(time_min)
            time_sec = int(time_sec)
        else:
            time_min = None
            time_sec = None

        if "out of bounds" in play["description"].lower():
            is_out_of_bounds = True

        if play["phase"].lower() == "overtime":
            quarter_seconds_remaining = 0
            half_seconds_remaining = 0
            game_seconds_remaining = 0
        elif time_min is None:
            quarter_seconds_remaining = None
            half_seconds_remaining = None
            game_seconds_remaining = None
        elif game_half == "Half1":
            quarter_seconds_remaining = (time_min * 60) + time_sec
            half_seconds_remaining = (
                (2 - int(play["phaseQualifier"])) * 900
            ) + (time_min * 60) + time_sec
            game_seconds_remaining = (
                (4 - int(play["phaseQualifier"])) * 900
            ) + (time_min * 60) + time_sec
        elif game_half == "Half2":
            quarter_seconds_remaining = (time_min * 60) + time_sec
            half_seconds_remaining = (
                (4 - int(play["phaseQualifier"])) * 900
            ) + (time_min * 60) + time_sec
            game_seconds_remaining = (
                (4 - int(play["phaseQualifier"])) * 900
            ) + (time_min * 60) + time_sec

        if "timeout" in play["description"].lower():
            raise NotImplementedError(
                "TODO: Implement timeout logic for the following play:" +
                f"\n{play}"
            )

        if "shotgun" in play["description"].lower():
            is_shotgun = True

        if "no huddle" in play["description"].lower():
            is_no_huddle = True
            # raise NotImplementedError(
            #     f"Unhandled play {play["description"]}"
            # )

        if (
            "no play" in play["description"].lower() or
            "nullified by penalty" in play["description"].lower()
        ):
            is_no_play = True

        # Passing
        if (
            play["type"].lower() == "pass" and
            play["subType"].lower() == "incompletepass"
        ):

            is_qb_dropback = True
            is_pass = True
            is_scrimmage_play = True
            is_incomplete_pass = True

            if "safety" in play["description"].lower():
                is_safety = True
                raise NotImplementedError(
                    "TODO: Implement safety logic for the following play:" +
                    f"\n{play}"
                )

            if down == 2:
                is_second_down_failed = True
            elif down == 3:
                is_third_down_failed = True
            elif down == 3:
                is_fourth_down_failed = True

            if (
                "penalty " in play["description"].lower() and
                "declined" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"PENALTY ([a-zA-Z]+) ([a-zA-Z\s\,]+) declined",
                    play["description"]
                )
                penalty_team = play_arr[0][0]
                penalty_type = play_arr[0][1]
                del play_arr
            if "play overturned" in play["description"].lower():
                is_replay_or_challenge = True
                replay_or_challenge_result = "overturned"
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass incomplete ([a-zA-Z]+) ([a-zA-Z]+) to [\#0-9]+ ([a-zA-Z\.\-\s\']+) thrown to ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                temp_ay = get_yardline(play_arr[0][4], posteam)
                air_yards = yardline_100 - temp_ay
            elif "pass complete" in play["description"].lower():
                # Yes, there is a passing play that's both
                # labeled as a completed and a incomplete pass.
                # Welcome to the CFL.
                is_incomplete_pass = False
                is_complete_pass = True

                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"for ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+)" +
                    r"[ \([\#0-9]+ ([a-zA-Z\.\-\s\']+)\)]?",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                # temp_ay = get_yardline(play_arr[0][4], posteam)
                # air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][4])
                yards_gained = passing_yards

                # First down, 1st down
                if (yards_gained >= yds_to_go and down == 1):
                    is_first_down = True
                    is_first_down_pass = True
                # First down, 2nd down
                elif (yards_gained >= yds_to_go and down == 4):
                    is_first_down = True
                    is_first_down_pass = True
                    is_second_down_converted = True
                elif (yards_gained < yds_to_go and down == 4):
                    is_second_down_failed = True
                # First down, 3rd down
                elif (yards_gained >= yds_to_go and down == 4):
                    is_first_down = True
                    is_first_down_pass = True
                    is_third_down_converted = True
                elif (yards_gained < yds_to_go and down == 4):
                    is_third_down_failed = True
                # First down, 4th down
                elif (yards_gained >= yds_to_go and down == 4):
                    is_first_down = True
                    is_first_down_pass = True
                    is_fourth_down_converted = True
                elif (yards_gained < yds_to_go and down == 4):
                    is_fourth_down_failed = True
            elif "broken up by" in play["description"].lower() and \
                    "thrown to" in play["description"].lower():
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass incomplete " +
                        r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                        r"thrown to ([0-9a-zA-Z\-]+) broken up by " +
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                        play["description"]
                    )
                    passer_player_name = play_arr[0][0]
                    pass_length = play_arr[0][1]
                    pass_location = play_arr[0][2]
                    receiver_player_name = play_arr[0][3]
                    temp_ay = get_yardline(play_arr[0][4], posteam)
                    air_yards = yardline_100 - temp_ay
                    pass_defense_1_player_name = play_arr[0][5]

                    del temp_ay
                    del play_arr
                except Exception:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass incomplete ([a-zA-Z]+) ([a-zA-Z]+) thrown to ([0-9a-zA-Z\-]+) broken up by [\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                        play["description"]
                    )
                    passer_player_name = play_arr[0][0]
                    pass_length = play_arr[0][1]
                    pass_location = play_arr[0][2]
                    # receiver_player_name = play_arr[0][3]
                    temp_ay = get_yardline(play_arr[0][3], posteam)
                    air_yards = yardline_100 - temp_ay
                    pass_defense_1_player_name = play_arr[0][4]

                    del temp_ay
                    del play_arr
            elif (
                "broken up by" in play["description"].lower() and
                "to" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass incomplete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"broken up by " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                pass_defense_1_player_name = play_arr[0][4]

                del play_arr
            elif (
                "broken up by" in play["description"].lower() and
                "to" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass incomplete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) broken up by " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                # receiver_player_name = play_arr[0][3]
                pass_defense_1_player_name = play_arr[0][3]

                del play_arr
            elif "thrown to" in play["description"].lower():
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass incomplete " +
                        r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                        r"thrown to ([0-9a-zA-Z\-]+)",
                        play["description"]
                    )
                    passer_player_name = play_arr[0][0]
                    pass_length = play_arr[0][1]
                    pass_location = play_arr[0][2]
                    receiver_player_name = play_arr[0][3]
                    temp_ay = get_yardline(play_arr[0][4], posteam)
                    air_yards = yardline_100 - temp_ay
                    del play_arr
                except Exception:
                    try:
                        play_arr = re.findall(
                            r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass incomplete " +
                            r"([a-zA-Z]+) ([a-zA-Z]+) " +
                            r"thrown to ([0-9a-zA-Z\-]+)",
                            play["description"]
                        )
                        passer_player_name = play_arr[0][0]
                        pass_length = play_arr[0][1]
                        pass_location = play_arr[0][2]
                        # receiver_player_name = play_arr[0][3]
                        temp_ay = get_yardline(play_arr[0][3], posteam)
                        air_yards = yardline_100 - temp_ay
                        del play_arr
                    except Exception:
                        try:
                            play_arr = re.findall(
                                r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass incomplete ([a-zA-Z]+) thrown to ([0-9a-zA-Z\-]+)",
                                play["description"]
                            )
                            passer_player_name = play_arr[0][0]
                            pass_length = play_arr[0][1]
                            # pass_location = play_arr[0][2]
                            # receiver_player_name = play_arr[0][3]
                            temp_ay = get_yardline(play_arr[0][2], posteam)
                            air_yards = yardline_100 - temp_ay
                            del play_arr
                        except Exception:
                            play_arr = re.findall(
                                r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass incomplete ([a-zA-Z]+) to [\#0-9]+ ([a-zA-Z\.\-\s\']+) thrown to ([0-9a-zA-Z\-]+)",
                                play["description"]
                            )
                            passer_player_name = play_arr[0][0]
                            pass_length = play_arr[0][1]
                            # pass_location = play_arr[0][2]
                            receiver_player_name = play_arr[0][2]
                            temp_ay = get_yardline(play_arr[0][3], posteam)
                            air_yards = yardline_100 - temp_ay
                            del play_arr

            elif "spike" in play["description"].lower():
                is_qb_spike = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass incomplete, Spike",
                    play["description"]
                )
                passer_player_name = play_arr[0]
            elif "to" not in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass incomplete ([a-zA-Z]+)( [a-zA-Z]+)?",
                    play["description"]
                )

                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                try:
                    pass_location = play_arr[0][2]
                except Exception:
                    pass_location = None
            elif (
                "right" not in play["description"].lower() and
                "middle" not in play["description"].lower() and
                "left" not in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass incomplete " +
                    r"([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                receiver_player_name = play_arr[0][2]
                del play_arr
            elif (
                "the previous play is under review"
                in play["description"].lower()
            ):
                is_replay_or_challenge = True

                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass incomplete ([a-zA-Z]+) ([a-zA-Z]+) to [\#0-9]+ ([a-zA-Z\.\-\s\']+).? The previous play is under review.?",
                    play["description"]
                )
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass incomplete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]

                if "the rulling on the field" in play["description"].lower():
                    play_arr = re.findall(
                        r"The ruling on the field ([A-Za-z]+).?",
                    play["description"]
                    )
                    replay_or_challenge_result = play_arr[0][0]
            else:
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass incomplete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                # temp_ay = get_yardline(play_arr[0][4], posteam)
                # air_yards = yardline_100 - temp_ay

                del play_arr

            if "safety" in play["description"].lower():
                is_safety = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
                except Exception:
                    play_arr = re.findall(
                        r" ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
        elif (
            play["type"].lower() == "pass" and
            play["subType"].lower() == "completepass"
        ):
            is_qb_dropback = True
            is_complete_pass = True
            is_pass = True
            is_scrimmage_play = True

            if "overturned" in play["description"].lower():
                replay_or_challenge_result = "overturned"
                is_replay_or_challenge = True
                overturned_play = re.findall(
                    r"PLAY OVERTURNED. \(Original Play: (\([0-9\:]+\))?([a-zA-Z\#\s\.0-9]+)\)",
                    play["description"]
                )
                if "incomplete" in overturned_play:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass incomplete ([a-zA-Z]+) ([a-zA-Z]+) to [\#0-9]+ ([a-zA-Z\.\-\s\']+) thrown to ([0-9a-zA-Z\-]+)",
                        overturned_play
                    )

                    passer_player_name = play_arr[0][0]
                    pass_length = play_arr[0][1]
                    pass_location = play_arr[0][2]
                    receiver_player_name = play_arr[0][3]
                    temp_ay = get_yardline(play_arr[0][4], posteam)
                    air_yards = yardline_100 - temp_ay
            elif (
                "yard loss" in play["description"].lower() or
                "yards loss" in play["description"].lower()
            ) and (
                "caught at" in play["description"].lower() and
                "out of bounds" in play["description"].lower() and
                "(" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete ([a-zA-Z]+) ([a-zA-Z]+) to [\#0-9]+ ([a-zA-Z\.\-\s\']+) caught at ([0-9a-zA-Z\-]+), for ([\-0-9]+) yard[s]? loss to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\), out of bounds",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                temp_ay = get_yardline(play_arr[0][4], posteam)
                air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][5]) * -1
                yards_gained = passing_yards
                yards_after_catch = passing_yards - air_yards

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][7]
                )
                if len(tak_arr) == 2:
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                    tackle_for_loss_1_player_name = assist_tackle_1_player_name
                    tackle_for_loss_2_player_name = assist_tackle_2_player_name
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                    tackle_for_loss_1_player_name = solo_tackle_1_player_name
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )

            elif (
                "yard loss" in play["description"].lower() or
                "yards loss" in play["description"].lower()
            ) and (
                "caught at" in play["description"].lower() and
                "out of bounds" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete ([a-zA-Z]+) ([a-zA-Z]+) to [\#0-9]+ ([a-zA-Z\.\-\s\']+) caught at ([0-9a-zA-Z\-]+), for ([\-0-9]+) yard[s]? loss to the ([0-9a-zA-Z\-]+), out of bounds at ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                temp_ay = get_yardline(play_arr[0][4], posteam)
                air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][5]) * -1
                yards_gained = passing_yards
                yards_after_catch = passing_yards - air_yards
            elif (
                "yard loss" in play["description"].lower() or
                "yards loss" in play["description"].lower()
            ) and (
                "caught at" in play["description"].lower() and
                "fumbled by" in play["description"].lower() and
                "forced by" in play["description"].lower() and
                "recovered by" in play["description"].lower() and
                "advances" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete ([a-zA-Z]+) ([a-zA-Z]+) to [\#0-9]+ ([a-zA-Z\.\-\s\']+) caught at ([0-9a-zA-Z\-]+), for ([\-0-9]+) yard[s]? loss to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) forced by [\#0-9]+ ([a-zA-Z\.\-\s\']+) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) advances ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                temp_ay = get_yardline(play_arr[0][4], posteam)
                air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][5]) * -1
                yards_gained = passing_yards
                yards_after_catch = passing_yards - air_yards

                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][7]

                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][9]

                fumble_recovery_1_team = play_arr[0][10]
                fumble_recovery_1_player_name = play_arr[0][11]
                fumble_recovery_1_yards = int(play_arr[0][13])

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][15]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                    tackle_for_loss_1_player_name = assist_tackle_1_player_name
                    tackle_for_loss_2_player_name = assist_tackle_2_player_name
                elif len(tak_arr) == 1:
                    solo_tackle_1_player_name = tak_arr[0]
                    tackle_for_loss_1_player_name = solo_tackle_1_player_name
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "yard loss" in play["description"].lower() or
                "yards loss" in play["description"].lower()
            ) and (
                "caught at" in play["description"].lower() and
                "fumbled by" in play["description"].lower() and
                "forced by" in play["description"].lower() and
                "recovered by" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete ([a-zA-Z]+) ([a-zA-Z]+) to [\#0-9]+ ([a-zA-Z\.\-\s\']+) caught at ([0-9a-zA-Z\-]+), for ([\-0-9]+) yard[s]? loss to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) forced by [\#0-9]+ ([a-zA-Z\.\-\s\']+) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\-\s\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                temp_ay = get_yardline(play_arr[0][4], posteam)
                air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][5]) * -1
                yards_gained = passing_yards
                yards_after_catch = passing_yards - air_yards

                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][7]

                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][9]

                fumble_recovery_1_team = play_arr[0][10]
                fumble_recovery_1_player_name = play_arr[0][11]
                fumble_recovery_1_yards = int(play_arr[0][14])

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][16]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                    tackle_for_loss_1_player_name = assist_tackle_1_player_name
                    tackle_for_loss_2_player_name = assist_tackle_2_player_name
                elif len(tak_arr) == 1:
                    solo_tackle_1_player_name = tak_arr[0]
                    tackle_for_loss_1_player_name = solo_tackle_1_player_name
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "yard loss" in play["description"].lower() or
                "yards loss" in play["description"].lower()
            ) and "caught at" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"caught at ([0-9a-zA-Z\-]+), " +
                    r"for ([\-0-9]+) yard[s]? loss " +
                    r"to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                temp_ay = get_yardline(play_arr[0][4], posteam)
                air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][5]) * -1
                yards_gained = passing_yards

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][7]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                    tackle_for_loss_1_player_name = assist_tackle_1_player_name
                    tackle_for_loss_2_player_name = assist_tackle_2_player_name
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                    tackle_for_loss_1_player_name = solo_tackle_1_player_name
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "caught at" in play["description"].lower() and
                "fumbled by" in play["description"].lower() and
                "forced by" in play["description"].lower() and
                "return for loss of" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_forced = True

                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete ([a-zA-Z]+) ([a-zA-Z]+) to [\#0-9]+ ([a-zA-Z\.\-\s\']+) caught at ([0-9a-zA-Z\-]+), for ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) forced by [\#0-9]+ ([a-zA-Z\.\-\s\']+) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\-\s\']+)return for loss of ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                temp_ay = get_yardline(play_arr[0][4], posteam)
                air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][5])
                yards_gained = passing_yards
                yards_after_catch = passing_yards - air_yards

                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][7]

                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][9]

                fumble_recovery_1_team = play_arr[0][10]
                fumble_recovery_1_player_name = play_arr[0][11]
                fumble_recovery_1_yards = play_arr[0][14] * -1

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][16]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "caught at" in play["description"].lower() and
                "fumbled by" in play["description"].lower() and
                "forced by" in play["description"].lower() and
                "return" in play["description"].lower() and
                "out of bounds at" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_forced = True

                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete ([a-zA-Z]+) ([a-zA-Z]+) to [\#0-9]+ ([a-zA-Z\.\-\s\']+) caught at ([0-9a-zA-Z\-]+), for ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) forced by [\#0-9]+ ([a-zA-Z\.\-\s\']+) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\-\s\']+)return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+)\, out of bounds at ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                temp_ay = get_yardline(play_arr[0][4], posteam)
                air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][5])
                yards_gained = passing_yards
                yards_after_catch = passing_yards - air_yards

                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][7]

                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][9]

                fumble_recovery_1_team = play_arr[0][10]
                fumble_recovery_1_player_name = play_arr[0][11]
                fumble_recovery_1_yards = play_arr[0][14]

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam
            elif (
                "caught at" in play["description"].lower() and
                "fumbled by" in play["description"].lower() and
                "forced by" in play["description"].lower() and
                "return" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_forced = True

                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete ([a-zA-Z]+) ([a-zA-Z]+) to [\#0-9]+ ([a-zA-Z\.\-\s\']+) caught at ([0-9a-zA-Z\-]+), for ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) forced by [\#0-9]+ ([a-zA-Z\.\-\s\']+) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\-\s\']+)return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                temp_ay = get_yardline(play_arr[0][4], posteam)
                air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][5])
                yards_gained = passing_yards
                yards_after_catch = passing_yards - air_yards

                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][7]

                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][9]

                fumble_recovery_1_team = play_arr[0][10]
                fumble_recovery_1_player_name = play_arr[0][11]
                fumble_recovery_1_yards = play_arr[0][14]

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][16]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "caught at" in play["description"].lower() and
                "out of bounds" in play["description"].lower() and
                " (#" in play["description"].lower() and
                (
                    "left" not in play["description"].lower() and
                    "right" not in play["description"].lower() and
                    "middle" not in play["description"].lower()
                )
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete ([a-zA-Z]+) to [\#0-9]+ ([a-zA-Z\.\-\s\']+) caught at ([0-9a-zA-Z\-]+), for ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\), out of bounds",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                # pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][2]
                temp_ay = get_yardline(play_arr[0][3], posteam)
                air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][4])
                yards_gained = passing_yards
                yards_after_catch = passing_yards - air_yards

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][6]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_1_team = defteam
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )

            elif (
                "caught at" in play["description"].lower() and
                "out of bounds" in play["description"].lower() and
                " (#" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete ([a-zA-Z]+) ([a-zA-Z]+) to [\#0-9]+ ([a-zA-Z\.\-\s\']+) caught at ([0-9a-zA-Z\-]+), for ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\), out of bounds",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                temp_ay = get_yardline(play_arr[0][4], posteam)
                air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][5])
                yards_gained = passing_yards
                yards_after_catch = passing_yards - air_yards

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][7]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_1_team = defteam
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "caught at" in play["description"].lower() and
                "out of bounds" in play["description"].lower() and
                "out of bounds at" not in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete ([a-zA-Z]+) ([a-zA-Z]+) to [\#0-9]+ ([a-zA-Z\.\-\s\']+) caught at ([0-9a-zA-Z\-]+), for ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) ?, out of bounds",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                temp_ay = get_yardline(play_arr[0][4], posteam)
                air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][5])
                yards_after_catch = passing_yards - air_yards
                yards_gained = passing_yards
            elif (
                "caught at" in play["description"].lower() and
                "fumbled by" in play["description"].lower() and
                "forced by" in play["description"].lower() and
                "out of bounds" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete ([a-zA-Z]+) ([a-zA-Z]+) to [\#0-9]+ ([a-zA-Z\.\-\s\']+) caught at ([0-9a-zA-Z\-]+), for ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) forced by [\#0-9]+ ([a-zA-Z\.\-\s\']+)\, out of bounds at ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                temp_ay = get_yardline(play_arr[0][4], posteam)
                air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][5])
                yards_after_catch = passing_yards - air_yards
                yards_gained = passing_yards

                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][7]

                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][7]
            elif (
                "caught at" in play["description"].lower() and
                "out of bounds" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"caught at ([0-9a-zA-Z\-]+), for ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+), " +
                    r"out of bounds at ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                temp_ay = get_yardline(play_arr[0][4], posteam)
                air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][5])
                yards_after_catch = passing_yards - air_yards
                yards_gained = passing_yards
            elif (
                "caught at" in play["description"].lower() and
                "end of play" in play["description"].lower() and
                "fumbled by" in play["description"].lower() and
                "forced by" in play["description"].lower() and
                "advances" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete ([a-zA-Z]+) ([a-zA-Z]+) to [\#0-9]+ ([a-zA-Z\.\-\s\']+) caught at ([0-9a-zA-Z\-]+), for ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) forced by [\#0-9]+ ([a-zA-Z\.\-\s\']+) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) advances ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+), End Of Play",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                temp_ay = get_yardline(play_arr[0][4], posteam)
                air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][5])
                yards_gained = passing_yards
                yards_after_catch = passing_yards - air_yards

                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][7]

                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][9]

                fumble_recovery_1_team = play_arr[0][10]
                fumble_recovery_1_player_name = play_arr[0][11]
                fumble_recovery_1_yards = play_arr[0][13]

            elif (
                "caught at" in play["description"].lower() and
                "end of play" in play["description"].lower() and
                "fumbled by" in play["description"].lower() and
                "forced by" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete ([a-zA-Z]+) ([a-zA-Z]+) to [\#0-9]+ ([a-zA-Z\.\-\s\']+) caught at ([0-9a-zA-Z\-]+), for ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) forced by [\#0-9]+ ([a-zA-Z\.\-\s\']+) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+), End Of Play",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                temp_ay = get_yardline(play_arr[0][4], posteam)
                air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][5])
                yards_gained = passing_yards
                yards_after_catch = passing_yards - air_yards

                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][7]

                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][9]

                fumble_recovery_1_team = play_arr[0][10]
                fumble_recovery_1_player_name = play_arr[0][11]
                fumble_recovery_1_yards = 0
            elif (
                "caught at" in play["description"].lower() and
                "end of play" in play["description"].lower() and
                "open field kick" in play["description"].lower() and
                "recovered by" in play["description"].lower() and
                "end of play" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete ([a-zA-Z]+) ([a-zA-Z]+) to [\#0-9]+ ([a-zA-Z\.\-\s\']+) caught at ([0-9a-zA-Z\-]+), for ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\-\s\']+) open field kick ([0-9\-]+) yard[s]? to the ([0-9a-zA-Z\-]+) recovered by ([A-Z]+) [\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+), [END|end]+ [OF|of]+ [PLAY|play]+",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                temp_ay = get_yardline(play_arr[0][4], posteam)
                air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][5])
                yards_gained = passing_yards
                yards_after_catch = passing_yards - air_yards
                punter_player_name = play_arr[0][7]
                kick_distance = int(play_arr[0][8])
                fumble_recovery_1_team = play_arr[0][10]
                fumble_recovery_1_player_name = play_arr[0][11]

            elif (
                "caught at" in play["description"].lower() and
                "end of play" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"caught at ([0-9a-zA-Z\-]+), for ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+), End Of Play",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                temp_ay = get_yardline(play_arr[0][4], posteam)
                air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][5])
                yards_gained = passing_yards
                yards_after_catch = passing_yards - air_yards
            elif (
                "caught at" in play["description"].lower() and
                "fumbled by" in play["description"].lower() and
                "recovered by" in play["description"].lower() and
                "advances" in play["description"].lower() and
                "forced by" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete ([a-zA-Z]+) ([a-zA-Z]+) to [\#0-9]+ ([a-zA-Z\.\-\s\']+) caught at ([0-9a-zA-Z\-]+), for ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) forced by [\#0-9]+ ([a-zA-Z\.\-\s\']+) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) advances ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                temp_ay = get_yardline(play_arr[0][4], posteam)
                air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][5])
                yards_gained = passing_yards
                yards_after_catch = passing_yards - air_yards

                fumbled_1_team = play_arr[0][7]
                fumbled_1_player_name = posteam

                forced_fumble_player_1_team = play_arr[0][8]
                forced_fumble_player_1_player_name = play_arr[0][9]

                fumble_recovery_1_team = play_arr[0][10]
                fumble_recovery_1_player_name = play_arr[0][11]
                fumble_recovery_1_yards = play_arr[0][13]

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][15]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "caught at" in play["description"].lower() and
                "fumbled by" in play["description"].lower() and
                "recovered by" in play["description"].lower() and
                "advances" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete ([a-zA-Z]+) ([a-zA-Z]+) to [\#0-9]+ ([a-zA-Z\.\-\s\']+) caught at ([0-9a-zA-Z\-]+), for ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) advances ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                temp_ay = get_yardline(play_arr[0][4], posteam)
                air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][5])
                yards_gained = passing_yards
                yards_after_catch = passing_yards - air_yards

                fumbled_1_team = play_arr[0][7]
                fumbled_1_player_name = posteam

                fumble_recovery_1_team = play_arr[0][9]
                fumble_recovery_1_player_name = play_arr[0][10]
                fumble_recovery_1_yards = play_arr[0][12]

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][14]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "caught at" in play["description"].lower() and
                "open field kick" in play["description"].lower()
            ):
                is_punt = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete ([a-zA-Z]+) ([a-zA-Z]+) to [\#0-9]+ ([a-zA-Z\.\-\s\']+) caught at ([0-9a-zA-Z\-]+), for ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\-\s\']+) open field kick ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                temp_ay = get_yardline(play_arr[0][4], posteam)
                air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][5])
                yards_gained = passing_yards
                yards_after_catch = passing_yards - air_yards

                punter_player_name = play_arr[0][7]
                kick_distance = int(play_arr[0][8])

                punt_returner_player_name = play_arr[0][11]
                return_yards = 0

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][13]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
                punt_end_yl = get_yardline(play_arr[0][12], posteam)
            elif (
                "caught at" in play["description"].lower() and
                (
                    "left" not in play["description"].lower() and
                    "right" not in play["description"].lower() and
                    "middle" not in play["description"].lower()
                )
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete " +
                    r"([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"caught at ([0-9a-zA-Z\-]+), for ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                # pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][2]
                temp_ay = get_yardline(play_arr[0][3], posteam)
                air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][4])
                yards_gained = passing_yards
                yards_after_catch = passing_yards - air_yards

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][6]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "caught at" in play["description"].lower() and
                "(" not in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"caught at ([0-9a-zA-Z\-]+), for ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) " ,
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                temp_ay = get_yardline(play_arr[0][4], posteam)
                air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][5])
                yards_gained = passing_yards
                yards_after_catch = passing_yards - air_yards
            elif "caught at" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"caught at ([0-9a-zA-Z\-]+), for ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                temp_ay = get_yardline(play_arr[0][4], posteam)
                air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][5])
                yards_gained = passing_yards
                yards_after_catch = passing_yards - air_yards

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][7]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "yard loss" in play["description"].lower() or
                "yards loss" in play["description"].lower()
            ) and "end of play" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"for ([\-0-9]+) yard[s]? loss " +
                    r"to the ([0-9a-zA-Z\-]+), End Of Play",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                # temp_ay = get_yardline(play_arr[0][4], posteam)
                # air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][4]) * -1
                yards_gained = passing_yards
            elif (
                "yard loss" in play["description"].lower() or
                "yards loss" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"for ([\-0-9]+) yard[s]? loss " +
                    r"to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                # temp_ay = get_yardline(play_arr[0][4], posteam)
                # air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][4]) * -1
                yards_gained = passing_yards

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][6]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                    tackle_for_loss_1_player_name = assist_tackle_1_player_name
                    tackle_for_loss_1_player_name = solo_tackle_1_player_name
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                    tackle_for_loss_1_player_name = solo_tackle_1_player_name
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "out of bounds at" in play["description"].lower() and
                "fumbled by" in play["description"].lower() and
                "recovered by" in play["description"].lower() and
                "returned" not in play["description"].lower() and
                "return" not in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) for ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) fumbled by " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) " +
                    r"forced by [\#0-9]+ ([a-zA-Z\.\-\s\']+) recovered by " +
                    r"([A-Z]{2,4}) [\#0-9]+ ([a-zA-Z\.\-\s\']+) at " +
                    r"([0-9a-zA-Z\-]+), out of bounds at ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                # temp_ay = get_yardline(play_arr[0][4], posteam)
                # air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][4])
                yards_gained = passing_yards

                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][6]

                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][8]

                fumble_recovery_1_team = play_arr[0][9]
                fumble_recovery_1_player_name = play_arr[0][10]
                fumble_recovery_1_yards = 0

                if fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
            elif (
                "out of bounds at" in play["description"].lower() and
                "fumbled by" in play["description"].lower() and
                "recovered by" not in play["description"].lower()
            ):
                is_fumble_out_of_bounds = True
                is_fumble = True
                is_fumble_not_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to [\#0-9]+ " +
                    r"([a-zA-Z\.\-\s\']+) for ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ " +
                    r"([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) forced by " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+), out of bounds",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                # temp_ay = get_yardline(play_arr[0][4], posteam)
                # air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][4])
                yards_gained = passing_yards

                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][6]

                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][8]
            elif (
                "out of bounds at" in play["description"].lower() and
                "fumbled by" in play["description"].lower() and
                "forced by" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_not_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) for ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) fumbled by " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) " +
                    r"forced by [\#0-9]+ ([a-zA-Z\.\-\s\']+) recovered by " +
                    r"([A-Z]{2,4}) [\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+), " +
                    r"out of bounds",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                # temp_ay = get_yardline(play_arr[0][4], posteam)
                # air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][4])
                yards_gained = passing_yards

                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][6]

                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][8]

                fumble_recovery_1_team = play_arr[0][9]
                fumble_recovery_1_player_name = play_arr[0][10]
                return_yards = int(play_arr[0][13])
            elif "out of bounds at" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"for ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+)" +
                    r", out of bounds at ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                # temp_ay = get_yardline(play_arr[0][4], posteam)
                # air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][4])
                yards_gained = passing_yards

            elif (
                "fumbled by" in play["description"].lower()
                and "1st down" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) for ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) fumbled by " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) " +
                    r"forced by [\#0-9]+ ([a-zA-Z\.\-\s\']+) recovered " +
                    r"by ([A-Z]{2,4}) [\#0-9]+ ([a-zA-Z\.\-\s\']+) at " +
                    r"([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                # temp_ay = get_yardline(play_arr[0][4], posteam)
                # air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][4])
                yards_gained = passing_yards

                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][6]

                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][8]

                fumble_recovery_1_team = play_arr[0][9]
                fumble_recovery_1_player_name = play_arr[0][10]
                fumble_recovery_1_yards = 0

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][12]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    # assist_tackle_1_team = defteam
                    # assist_tackle_2_team = defteam
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "fumbled by" in play["description"].lower() and
                "forced by" not in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) for ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) fumbled by " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) " +
                    r"recovered by ([A-Z]{2,4}) " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                # temp_ay = get_yardline(play_arr[0][4], posteam)
                # air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][4])
                yards_gained = passing_yards

                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][8]
                fumble_recovery_1_player_name = play_arr[0][9]

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][11]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    # assist_tackle_1_team = defteam
                    # assist_tackle_2_team = defteam
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "fumbled by" in play["description"].lower() and
                "end of play" in play["description"].lower() and
                "returned" not in play["description"].lower() and
                "return" not in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) for ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) fumbled by " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) " +
                    r"forced by [\#0-9]+ ([a-zA-Z\.\-\s\']+) recovered by " +
                    r"([A-Z]{2,4}) [\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"at ([0-9a-zA-Z\-]+), [\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"for ([\-0-9]+) yard[s]? to the " +
                    r"([0-9a-zA-Z\-]+), End Of Play",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                # temp_ay = get_yardline(play_arr[0][4], posteam)
                # air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][4])
                yards_gained = passing_yards

                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][6]

                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][8]

                fumble_recovery_1_team = play_arr[0][9]
                fumble_recovery_1_player_name = play_arr[0][10]
                fumble_recovery_1_yards = int(play_arr[0][13])

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = posteam
            elif (
                "fumbled by" in play["description"].lower() and
                "returned" not in play["description"].lower() and
                "return" not in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) for ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) fumbled by " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) " +
                    r"forced by [\#0-9]+ ([a-zA-Z\.\-\s\']+) recovered by " +
                    r"([A-Z]{2,4}) [\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"at ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                # temp_ay = get_yardline(play_arr[0][4], posteam)
                # air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][4])
                yards_gained = passing_yards

                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][6]

                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][8]

                fumble_recovery_1_team = play_arr[0][9]
                fumble_recovery_1_player_name = play_arr[0][10]
                fumble_recovery_1_yards = 0

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][12]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    # assist_tackle_1_team = defteam
                    # assist_tackle_2_team = defteam
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "fumbled by" in play["description"].lower() and
                "end of play" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) for ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) fumbled by " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) " +
                    r"forced by [\#0-9]+ ([a-zA-Z\.\-\s\']+) recovered by " +
                    r"([A-Z]{2,4}) [\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"return ([\-0-9]+) yard[s]? to the " +
                    r"([0-9a-zA-Z\-]+), End Of Play",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                # temp_ay = get_yardline(play_arr[0][4], posteam)
                # air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][4])
                yards_gained = passing_yards

                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][6]

                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][8]

                fumble_recovery_1_team = play_arr[0][9]
                fumble_recovery_1_player_name = play_arr[0][10]
                fumble_recovery_1_yards = play_arr[0][13]
            elif (
                "fumbled by" in play["description"].lower() and
                "(" not in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) for ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) fumbled by " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) " +
                    r"forced by [\#0-9]+ ([a-zA-Z\.\-\s\']+) recovered by " +
                    r"([A-Z]{2,4}) [\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                # temp_ay = get_yardline(play_arr[0][4], posteam)
                # air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][4])
                yards_gained = passing_yards

                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][6]

                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][8]

                fumble_recovery_1_team = play_arr[0][9]
                fumble_recovery_1_player_name = play_arr[0][10]
                fumble_recovery_1_yards = play_arr[0][13]

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = posteam
            elif "fumbled by" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) for ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) fumbled by " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) " +
                    r"forced by [\#0-9]+ ([a-zA-Z\.\-\s\']+) recovered by " +
                    r"([A-Z]{2,4}) [\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                # temp_ay = get_yardline(play_arr[0][4], posteam)
                # air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][4])
                yards_gained = passing_yards

                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][6]

                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][8]

                fumble_recovery_1_team = play_arr[0][9]
                fumble_recovery_1_player_name = play_arr[0][10]
                fumble_recovery_1_yards = play_arr[0][13]

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][15]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    # assist_tackle_1_team = defteam
                    # assist_tackle_2_team = defteam
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "end of play" in play["description"].lower() and
                "open field kick" in play["description"].lower() and
                "return" not in play["description"].lower()
            ):
                is_punt = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"for ([\-0-9]+) yard[s]? to the " +
                    r"([0-9a-zA-Z\-]+) ? open field kick ([\-0-9]+) " +
                    r"yards to the ([0-9a-zA-Z\-]+) recovered by " +
                    r"([A-Z{2|3}]+) [\#0-9]+ ([a-zA-Z\.\-\s\']+) at " +
                    r"([0-9a-zA-Z\-]+), End Of Play",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                passing_yards = int(play_arr[0][4])
                yards_gained = passing_yards
                punter_player_name = receiver_player_name
                kick_distance = int(play_arr[0][6])
                punt_returner_player_name = play_arr[0][9]
                return_yards = 0
                punt_end_yl = get_yardline(play_arr[0][10], posteam)
            elif (
                "end of play" in play["description"].lower() and
                "open field kick" in play["description"].lower()
            ):
                is_punt = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"for ([\-0-9]+) yard[s]? to the " +
                    r"([0-9a-zA-Z\-]+) ? open field kick ([\-0-9]+) " +
                    r"yards to the ([0-9a-zA-Z\-]+) recovered by " +
                    r"([A-Z{2|3}]+) [\#0-9]+ ([a-zA-Z\.\-\s\']+) at " +
                    r"([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"return ([\-0-9]+) yards to the " +
                    r"([0-9a-zA-Z\-]+), End Of Play",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                passing_yards = int(play_arr[0][4])
                yards_gained = passing_yards
                punter_player_name = receiver_player_name
                kick_distance = int(play_arr[0][6])
                punt_returner_player_name = play_arr[0][9]
                return_yards = int(play_arr[0][12])
            elif "end of play" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"for ([\-0-9]+) yard[s]? to the " +
                    r"([0-9a-zA-Z\-]+), End Of Play",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                # temp_ay = get_yardline(play_arr[0][4], posteam)
                # air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][4])
                yards_gained = passing_yards
            elif (
                "right" not in play["description"].lower() and
                "left" not in play["description"].lower() and
                "middle" not in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete " +
                    r"([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"for ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                # pass_length = play_arr[0][1]
                pass_location = play_arr[0][1]
                receiver_player_name = play_arr[0][2]
                # temp_ay = get_yardline(play_arr[0][4], posteam)
                # air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][3])
                yards_gained = passing_yards
                # solo_tackle_1_team = defteam
                # solo_tackle_1_player_name = play_arr[0][6]
                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][5]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_1_team = defteam
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "overturned play" in play["description"].lower() and
                "incomplete" in play["description"].lower()
            ):
                is_complete_pass = False
                is_incomplete_pass = True
                is_replay_or_challenge = True
                replay_or_challenge_result = "overturned"
                overturn_play_temp = re.findall(
                    r"\(OVERTURNED PLAY: ([a-zA-Z0-9\s\(\)\#\.\:\,\-\']+)\)",
                    play["description"]
                )
                overturn_play_end = overturn_play_temp[0]
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass incomplete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to [\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    overturn_play_end
                )

                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                del overturn_play_temp
                del overturn_play_end
            elif "open field kick" in play["description"].lower():
                is_punt = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) for ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"open field kick ([\-0-9]) yard[s]? to the " +
                    r"([0-9a-zA-Z\-]+) recovered by ([A-Z{2|3}]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                # temp_ay = get_yardline(play_arr[0][4], posteam)
                # air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][4])
                yards_gained = passing_yards
                punter_player_name = play_arr[0][6]
                kick_distance = int(play_arr[0][7])

                punt_returner_player_name = play_arr[0][10]
                return_yards = 0

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][12]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_1_team = defteam
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            else:
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass complete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"for ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                # temp_ay = get_yardline(play_arr[0][4], posteam)
                # air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][4])
                yards_gained = passing_yards
                # solo_tackle_1_team = defteam
                # solo_tackle_1_player_name = play_arr[0][6]
                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][6]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_1_team = defteam
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )

            if "safety" in play["description"].lower():
                is_safety = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
                except Exception:
                    play_arr = re.findall(
                        r" ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]

            # First down, 1st down
            if (yards_gained >= yds_to_go and down == 1):
                is_first_down = True
                is_first_down_pass = True
            # First down, 2nd down
            elif (yards_gained >= yds_to_go and down == 4):
                is_first_down = True
                is_first_down_pass = True
                is_second_down_converted = True
            elif (yards_gained < yds_to_go and down == 4):
                is_second_down_failed = True
            # First down, 3rd down
            elif (yards_gained >= yds_to_go and down == 4):
                is_first_down = True
                is_first_down_pass = True
                is_third_down_converted = True
            elif (yards_gained < yds_to_go and down == 4):
                is_third_down_failed = True
            # First down, 4th down
            elif (yards_gained >= yds_to_go and down == 4):
                is_first_down = True
                is_first_down_pass = True
                is_fourth_down_converted = True
            elif (yards_gained < yds_to_go and down == 4):
                is_fourth_down_failed = True

            if "lateral" in play["description"]:
                raise NotImplementedError(
                    "TODO: Implement lateral logic for completed passes."
                )
        elif (
            play["type"].lower() == "pass" and
            play["subType"].lower() == "touchdown"
        ):
            is_qb_dropback = True
            is_complete_pass = True
            is_pass = True
            is_touchdown = True
            is_pass_touchdown = True
            is_first_down_pass = True
            is_scrimmage_play = True

            if (
                "caught at" in play["description"].lower() and
                "fumbled by" in play["description"].lower() and
                "forced by" in play["description"].lower() and
                "recovered by" in play["description"].lower() and
                "return" in play["description"].lower() and
                "touchdown" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) pass complete ([a-zA-Z]+) ([a-zA-Z]+) to [\#0-9]+ ([a-zA-Z\.\-\s\']+) caught at ([0-9a-zA-Z\-]+), for ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) forced by [\#0-9]+ ([a-zA-Z\.\s\-\']+) recovered by ([a-zA-Z]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) TOUCHDOWN",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                temp_ay = get_yardline(play_arr[0][4], posteam)
                air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][5])
                yards_gained = passing_yards

                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][7]
                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][9]
                fumble_recovery_1_team = play_arr[0][10]
                fumble_recovery_1_player_name = play_arr[0][11]
                fumble_recovery_1_yards = int(play_arr[0][14])

                td_team = play_arr[0][10]
                td_player_name = play_arr[0][13]
            elif "caught at" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) pass complete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"caught at ([0-9a-zA-Z\-]+), " +
                    r"for ([\-0-9]+) yard[s]? to the " +
                    r"([0-9a-zA-Z\-]+) TOUCHDOWN",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                temp_ay = get_yardline(play_arr[0][4], posteam)
                air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][5])
                yards_gained = passing_yards
                td_team = posteam
                td_player_name = receiver_player_name
            elif (
                "pass intercepted by" in play["description"].lower() and
                "broken up by" in play["description"].lower()
            ):
                is_return_touchdown = True
                is_interception = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass intercepted by [\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) broken up by [\#0-9]+ ([a-zA-Z\.\-\s\']+) [\#0-9]+ ([a-zA-Z\.\-\s\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) TOUCHDOWN",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                interception_player_name = play_arr[0][1]
                pass_defense_1_player_name = play_arr[0][3]
                return_yards = play_arr[0][5]
                td_team = defteam
                td_player_name = interception_player_name
            elif (
                "pass intercepted by" in play["description"].lower() and
                "lateral to" in play["description"].lower()
            ):
                is_return_touchdown = True
                is_interception = True
                is_lateral_return = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass intercepted by [\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\-\s\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) lateral to [\#0-9]+ ([a-zA-Z\.\-\s\']+) TOUCHDOWN",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                interception_player_name = play_arr[0][1]
                return_yards = play_arr[0][4]
                lateral_interception_player_name = play_arr[0][6]
                td_team = defteam
                td_player_name = interception_player_name
            elif "pass intercepted by" in play["description"].lower():
                is_return_touchdown = True
                is_interception = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) pass intercepted by [\#0-9]+ ([a-zA-Z\.\-\s\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\-\s\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) TOUCHDOWN",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                interception_player_name = play_arr[0][1]
                return_yards = play_arr[0][4]
                td_team = defteam
                td_player_name = interception_player_name
            elif "touchdown" not in play["description"].lower():
                # people: "Why do young people refuse to watch the CFL?"
                # CFL: (marks non-TD plays as TDs)
                is_touchdown = False
                is_pass_touchdown = False
                if "first down" not in play["description"].lower():
                    is_first_down_pass = False
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) pass complete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) for ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                passing_yards = int(play_arr[0][4])
                yards_gained = passing_yards
                td_team = posteam
                td_player_name = receiver_player_name

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][6]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_1_team = defteam
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "fumbled by" in play["description"].lower() and
                "forced by" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_forced = True
                is_return_touchdown = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) pass complete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) for ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) fumbled by " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) " +
                    r"forced by [\#0-9]+ ([a-zA-Z\.\s\-\']+) recovered by " +
                    r"([A-Z{2|3}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return " +
                    r"([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) TOUCHDOWN",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                passing_yards = int(play_arr[0][4])
                yards_gained = passing_yards

                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][6]

                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][8]

                fumble_recovery_1_team = play_arr[0][9]
                fumble_recovery_1_player_name = play_arr[0][10]
                fumble_recovery_1_yards = play_arr[0][13]
                td_team = fumble_recovery_1_team
                td_player_name = fumble_recovery_1_player_name

                if fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
            elif "end of play" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) pass complete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"for ([\-0-9]+) yard[s]? to the " +
                    r"([0-9a-zA-Z\-]+), End Of Play TOUCHDOWN",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                # temp_ay = get_yardline(play_arr[0][4], posteam)
                # air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][4])
                yards_gained = passing_yards
                td_team = posteam
                td_player_name = receiver_player_name
            else:
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) pass complete " +
                    r"([a-zA-Z]+) ([a-zA-Z]+) to " +
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) " +
                    r"for ([\-0-9]+) yard[s]? to the " +
                    r"([0-9a-zA-Z\-]+) TOUCHDOWN",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                pass_length = play_arr[0][1]
                pass_location = play_arr[0][2]
                receiver_player_name = play_arr[0][3]
                # temp_ay = get_yardline(play_arr[0][4], posteam)
                # air_yards = yardline_100 - temp_ay
                passing_yards = int(play_arr[0][4])
                yards_gained = passing_yards
                td_team = posteam
                td_player_name = receiver_player_name

            if "safety" in play["description"].lower():
                is_safety = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
                except Exception:
                    play_arr = re.findall(
                        r" ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]

            # if "lateral" in play["description"]:
            #     raise NotImplementedError(
            #         "TODO: Implement lateral logic for completed passes."
            #     )
        elif (
            play["type"].lower() == "pass" and
            play["subType"].lower() == "interception"
        ):
            is_qb_dropback = True
            is_incomplete_pass = True
            is_pass = True
            is_interception = True
            is_scrimmage_play = True

            if down == 2:
                is_second_down_failed = True
            elif down == 3:
                is_third_down_failed = True
            elif down == 3:
                is_fourth_down_failed = True

            if "return for loss of" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) pass intercepted by " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return for loss of ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                interception_player_name = play_arr[0][1]
                return_yards = int(play_arr[0][4]) * -1
            elif (
                "return" in play["description"].lower() and
                "broken up by" in play["description"].lower() and
                "out of bounds at" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) pass intercepted by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) broken up by [\#0-9]+ ([a-zA-Z\.\s\-\']+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+), out of bounds at ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                interception_player_name = play_arr[0][1]
                pass_defense_1_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][5])
                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][7]
                )
                # if len(tak_arr) == 2:
                #     is_assist_tackle = True
                #     assist_tackle_1_team = posteam
                #     assist_tackle_2_team = posteam
                #     assist_tackle_1_player_name = tak_arr[0][0]
                #     assist_tackle_2_player_name = tak_arr[1][0]
                # elif len(tak_arr) == 1:
                #     solo_tackle_1_team = posteam
                #     solo_tackle_1_player_name = tak_arr[0][0]
                # else:
                #     raise ValueError(
                #         f"Unhandled play {play}"
                #     )
            elif (
                "return" in play["description"].lower() and
                "broken up by" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) pass intercepted by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) broken up by [\#0-9]+ ([a-zA-Z\.\s\-\']+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                interception_player_name = play_arr[0][1]
                pass_defense_1_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][5])
                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][7]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = posteam
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif "return" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) pass intercepted by " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                interception_player_name = play_arr[0][1]
                return_yards = int(play_arr[0][4])
            elif "touchback" in play["description"].lower():
                is_touchback = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) pass intercepted by " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+), Touchback",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                interception_player_name = play_arr[0][1]
                return_yards = 0
            else:
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) pass intercepted by " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+), End Of Play",
                    play["description"]
                )
                passer_player_name = play_arr[0][0]
                interception_player_name = play_arr[0][1]
                return_yards = 0

            if (
                "fumble" in play["description"].lower() and
                "advances" in play["description"].lower()
            ):
                is_fumble_forced = True
                play_arr = re.findall(
                    r"fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) forced by " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"recovered by ([a-zA-Z]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"at ([0-9a-zA-Z\-]+) advances " +
                    r"([\-0-9]+) yard[s]? to the " +
                    r"([0-9a-zA-Z\-]+) \([\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                    play["description"]
                )
                fumbled_1_team = defteam
                fumbled_1_player_name = play_arr[0][0]
                forced_fumble_player_1_team = posteam
                forced_fumble_player_1_player_name = play_arr[0][2]
                fumble_recovery_1_team = play_arr[0][3]
                fumble_recovery_1_player_name = play_arr[0][4]
                fumble_recovery_1_yards = play_arr[0][6]

                if fumble_recovery_1_team == defteam:
                    solo_tackle_2_team = defteam
                    solo_tackle_2_player_name = play_arr[0][8]
                elif fumble_recovery_1_team == posteam:
                    solo_tackle_2_team = posteam
                    solo_tackle_2_player_name = play_arr[0][8]
            elif (
                "fumbled by" in play["description"].lower() and
                "end of play" in play["description"].lower() and
                "forced by" not in play["description"].lower()
            ):
                is_fumble_forced = False
                is_fumble_not_forced = True
                play_arr = re.findall(
                    r"fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) recovered by ([A-Z{2|3}]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) return " +
                    r"([\-0-9]+) yard[s]? to the " +
                    r"([0-9a-zA-Z\-]+), End Of Play",
                    play["description"]
                )
                fumbled_1_team = defteam
                fumbled_1_player_name = play_arr[0][0]
                # forced_fumble_player_1_team = posteam
                # forced_fumble_player_1_player_name = play_arr[0][2]
                fumble_recovery_1_team = play_arr[0][2]
                fumble_recovery_1_player_name = play_arr[0][3]
                fumble_recovery_1_yards = play_arr[0][6]
            elif "lateral" in play["description"].lower():
                raise NotImplementedError(
                    f"Unhandled interception return:\n{play["description"]}"
                )
            elif "touchdown" in play["description"].lower():
                raise NotImplementedError(
                    f"Unhandled interception return:\n{play["description"]}"
                )
            elif (
                "end of play" not in play["description"].lower() and
                "(" in play["description"].lower()
            ):
                # raise NotImplementedError(
                #     f"Unhandled interception return:\n{play["description"]}"
                # )
                tak_arr = re.findall(
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    tak_arr[0]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_1_team = defteam
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif "end of play" in play["description"].lower():
                # This is already handled earlier in the code,
                # so lets skip it
                pass
            elif "touchback" in play["description"].lower():
                # This is already handled earlier in the code,
                # so lets skip it
                pass
            elif "(" not in play["description"].lower():
                # If we get to this point in the if statement,
                # there's nothing to parse in the play
                pass
            else:
                raise ValueError(
                    f"Unhandled play {play}"
                )

            if (down == 2):
                is_second_down_failed = True
            elif (down == 3):
                is_third_down_failed = True
            elif (down == 4):
                is_fourth_down_failed = True
            if "safety" in play["description"].lower():
                is_safety = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
                except Exception:
                    play_arr = re.findall(
                        r" ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
        elif (
            play["type"].lower() == "pass" and
            play["subType"].lower() == "penalty"
        ):
            is_scrimmage_play = True
            is_penalty = True

            if ("first down" in play["description"] and down == 1) or \
                    ("1st down" in play["description"] and down == 1):
                is_first_down = True
                is_first_down_penalty = True
            elif ("first down" in play["description"] and down == 2) or \
                    ("1st down" in play["description"] and down == 2):
                is_first_down = True
                is_first_down_penalty = True
                is_second_down_converted = True
            elif ("1st down" in play["description"] and down == 3) or \
                    ("1st down" in play["description"] and down == 3):
                is_first_down = True
                is_first_down_penalty = True
                is_third_down_converted = True
            elif ("first down" in play["description"] and down == 4) or \
                    ("1st down" in play["description"] and down == 4):
                is_first_down = True
                is_first_down_penalty = True
                is_fourth_down_converted = True

            if "penalty" in play["description"].lower():
                # Yes, the data validation in the CFL is so bad,
                # that a play that's marked as
                # "a passing play that had a penalty",
                # has the chance of having no penalties in that play.
                penalty_arr = re.findall(
                    r"PENALTY([a-zA-Z0-9\s\(\)\#\.\,\-\']+)",
                    play["description"]
                )[0]

                if (
                    "yards from" in penalty_arr.lower() and
                    "(" in penalty_arr.lower()
                ):
                    try:
                        play_arr = re.findall(
                            r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+) " +
                            r"\(([a-zA-Z0-9\#\.\-\s\'\;]+)\) ? ([\-0-9]+) " +
                            r"yard[s]? from ([0-9a-zA-Z\-]+)? " +
                            r"to ([0-9a-zA-Z\-]+)",
                            penalty_arr
                        )
                        penalty_team = play_arr[0][0]
                        penalty_type = play_arr[0][1]
                        penalty_player_name = play_arr[0][2]
                        penalty_yards = int(play_arr[0][3])
                    except Exception:
                        logging.info(
                            f"Abnormal penalty play:{play}"
                        )

                    try:
                        play_arr = re.findall(
                            r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+) " +
                            r"\(([a-zA-Z0-9\#\.\-\s\'\;]+)\) ? " +
                            r"([\-0-9]+) yard[s]? " +
                            r"from ([0-9a-zA-Z\-]+)? to ([0-9a-zA-Z\-]+)",
                            penalty_arr
                        )
                        penalty_team = play_arr[0][0]
                        penalty_type = play_arr[0][1]
                        penalty_player_name = play_arr[0][2]
                        penalty_yards = int(play_arr[0][3])
                    except Exception:
                        play_arr = re.findall(
                            r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+) ? " +
                            r"([\-0-9]+) yard[s]? from ([0-9a-zA-Z\-]+)? " +
                            r"to ([0-9a-zA-Z\-]+)",
                            penalty_arr
                        )
                        penalty_team = play_arr[0][0]
                        penalty_type = play_arr[0][1]
                        # penalty_player_name = play_arr[0][2]
                        penalty_yards = int(play_arr[0][2])

                    if "#" in penalty_player_name:
                        penalty_player_name = re.findall(
                            r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                            play_arr[0][2]
                        )[0]
                    else:
                        penalty_player_name = None
                elif (
                    "Pass interference, defense, 1ST" in penalty_arr
                ):
                    play_arr = re.findall(
                        r"([A-Z]{2,4}) Pass interference, defense, 1ST",
                        penalty_arr
                    )
                    penalty_team = play_arr[0]
                    penalty_type = "Pass interference, defense"
                    penalty_yards = 0

                elif (
                    "yards from" in penalty_arr.lower()
                ):
                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+)\s" +
                        r"([\-0-9]+) yard[s]? from ([0-9a-zA-Z\-]+) " +
                        r"to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_yards = int(play_arr[0][2])
                elif ("(" not in penalty_arr.lower()):
                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([a-zA-Z\-\s\,0-9]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                else:
                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([a-zA-Z\-\s\,0-9]+) " +
                        r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_player_name = play_arr[0][2]
                    # penalty_yards = int(play_arr[0][3])
                del penalty_arr
            if "safety" in play["description"].lower():
                is_safety = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
                except Exception:
                    play_arr = re.findall(
                        r" ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
        elif (
            play["type"].lower() == "sack" and
            play["subType"] is None
        ):
            is_qb_dropback = True
            is_pass = True
            is_sack = True
            is_scrimmage_play = True
            if (
                "fumble by" in play["description"].lower() and
                "forced by" not in play["description"].lower() and
                "sacked for gain of" in play["description"].lower()
            ):
                # For the sake of sanity,
                # this is a rushing play if the QB gains yards.
                is_qb_dropback = False
                is_pass = False
                is_sack = False
                is_rush = True

                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) sacked for gain of " +
                    r"([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;]+)\), fumble by " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) recovered by " +
                    r"([A-Z{2|3}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                rushing_yards = int(play_arr[0][1])
                yards_gained = rushing_yards

                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][4]

                fumble_recovery_1_team = play_arr[0][5]
                fumble_recovery_1_player_name = play_arr[0][6]
                fumble_recovery_1_yards = int(play_arr[0][9])

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_player_name = defteam
                    assist_tackle_2_player_name = defteam
                elif fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = posteam
                    assist_tackle_1_player_name = posteam
                    assist_tackle_2_player_name = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][11]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "fumble by" in play["description"].lower() and
                "forced by" in play["description"].lower()
            ):
                raise ValueError(
                    f"Unhandled play {play}"
                )
            elif (
                "fumble by" in play["description"].lower() and
                "forced by" in play["description"].lower()
            ):
                raise ValueError(
                    f"Unhandled play {play}"
                )
            else:
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) sacked for loss of " +
                    r"([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )

                passer_player_name = play_arr[0][0]
                yards_gained = int(play_arr[0][1]) * -1
                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][3]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                    tackle_for_loss_1_player_name = assist_tackle_1_player_name
                    tackle_for_loss_2_player_name = assist_tackle_2_player_name
                    half_sack_1_player_name = assist_tackle_1_player_name
                    half_sack_2_player_name = assist_tackle_2_player_name
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )

            if "safety" in play["description"].lower():
                is_safety = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
                except Exception:
                    play_arr = re.findall(
                        r" ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
        elif (
            play["type"].lower() == "sack" and
            play["subType"].lower() == "penalty"
        ):
            is_qb_dropback = True
            is_pass = True
            is_sack = True
            is_scrimmage_play = True

            if "sack" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) sacked for [loss|gain]+ of ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )

                passer_player_name = play_arr[0][0]
                yards_gained = int(play_arr[0][1]) * -1

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][3]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                    tackle_for_loss_1_player_name = assist_tackle_1_player_name
                    tackle_for_loss_2_player_name = assist_tackle_2_player_name
                    half_sack_1_player_name = assist_tackle_1_player_name
                    half_sack_2_player_name = assist_tackle_2_player_name
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                    tackle_for_loss_1_player_name = solo_tackle_1_player_name
                    sack_player_name = solo_tackle_1_player_name
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )

            # tackle_for_loss_1_player_name = play_arr[0][3]
            # sack_player_name = tackle_for_loss_1_player_name

            penalty_arr = re.findall(
                r"PENALTY ([a-zA-Z0-9\s\(\)\#\.\,\-\']+)",
                play["description"]
            )[0]

            if (
                "yards from" in penalty_arr.lower() and
                "(" in penalty_arr.lower()
            ):
                try:
                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+) " +
                        r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\) " +
                        r"([\-0-9]+) yard[s]? from " +
                        r"([0-9a-zA-Z\-]+)? to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_player_name = play_arr[0][2]

                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+) " +
                        r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\) " +
                        r"([\-0-9]+) yard[s]? " +
                        r"from ([0-9a-zA-Z\-]+)? to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_player_name = play_arr[0][2]
                    penalty_yards = int(play_arr[0][3])
                except Exception:
                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+)\s" +
                        r"([\-0-9]+) yard[s]? from ([0-9a-zA-Z\-]+) " +
                        r"to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_yards = int(play_arr[0][2])
            elif (
                "yards from" in penalty_arr
            ):
                play_arr = re.findall(
                    r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+)\s" +
                    r"([\-0-9]+) yard[s]? from ([0-9a-zA-Z\-]+) " +
                    r"to ([0-9a-zA-Z\-]+)",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = play_arr[0][1]
                penalty_yards = int(play_arr[0][2])
            else:
                play_arr = re.findall(
                    r"([A-Z]{2,4}) ([a-zA-Z\-\s\,0-9]+) " +
                    r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = play_arr[0][1]
                penalty_player_name = play_arr[0][2]
                # penalty_yards = int(play_arr[0][3])
            del penalty_arr

            if "safety" in play["description"].lower():
                is_safety = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
                except Exception:
                    play_arr = re.findall(
                        r" ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]

        # Rushing
        elif (
            play["type"].lower() == "run" and
            play["subType"] is None
        ):
            is_scrimmage_play = True
            is_rush = True

            if (
                "fumbled snap" in play["description"].lower() and
                "end of play" in play["description"].lower()
            ) and (
                "yards loss" in play["description"].lower() or
                "yard loss" in play["description"].lower()
            ):
                is_fumble = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) " +
                    r"for ([\-0-9]+) yard[s]? loss to the " +
                    r"([0-9a-zA-Z\-]+) fumbled by " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) recovered by ([A-Z{2|3}]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"at ([0-9a-zA-Z\-]+), End Of Play",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2]) * -1
                yards_gained = rushing_yards
                fumbled_1_player_name = play_arr[0][4]
                fumble_recovery_1_team = play_arr[0][6]
                fumble_recovery_1_player_name = play_arr[0][7]

                if fumble_recovery_1_team != posteam:
                    is_fumble_lost = True
                    is_fumble_not_forced = True
            elif (
                " for 0 yards to the" in play["description"].lower() and
                "fumbled by" in play["description"].lower() and
                "forced by" not in play["description"].lower() and
                "return" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) recovered by ([a-zA-Z]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2])
                yards_gained = rushing_yards
                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][4]
                # forced_fumble_player_1_team = defteam
                # forced_fumble_player_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][6]
                fumble_recovery_1_player_name = play_arr[0][7]
                fumble_recovery_1_yards = int(play_arr[0][10])

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][12]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )

            elif (
                " for 0 yards to the" in play["description"].lower() and
                "fumbled by" in play["description"].lower() and
                "forced by" not in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yard[s]? to the " +
                    r"([0-9a-zA-Z\-]+) fumbled by " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) " +
                    r"recovered by ([a-zA-Z]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"at ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2])
                yards_gained = rushing_yards
                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][4]
                # forced_fumble_player_1_team = defteam
                # forced_fumble_player_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][6]
                fumble_recovery_1_player_name = play_arr[0][7]
                fumble_recovery_1_yards = 0

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][9]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                " for 0 yards to the" in play["description"].lower() and
                "fumbled by" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) forced by " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) recovered by " +
                    r"([a-zA-Z]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return ([\-0-9]+) yards to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2]) * -1
                yards_gained = rushing_yards
                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][4]
                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][7]
                fumble_recovery_1_player_name = play_arr[0][8]
                fumble_recovery_1_yards = int(play_arr[0][11])

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][13]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "yard gain" in play["description"].lower() or
                "yards gain" in play["description"].lower()
            ) and (
                "fumbled by" in play["description"].lower() and
                "out of bounds at" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_forced = True
                is_fumble_out_of_bounds = True
                play_arr = re.findall(
                    r"[\#0-9 ]+? ([a-zA-Z\.\s\-]+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yard[s]? gain to the ([0-9a-zA-Z\-]+) " +
                    r"fumbled by [\#0-9 ]+? ([a-zA-Z\.\s\-]+) at " +
                    r"([0-9a-zA-Z\-]+) forced by " +
                    r"[\#0-9 ]+? ([a-zA-Z\.\s\-]+), " +
                    r"out of bounds at ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2])
                yards_gained = rushing_yards
                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][4]
                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][6]

                # fumble_recovery_1_team = play_arr[0][7]
                # fumble_recovery_1_player_name = play_arr[0][8]
                # fumble_recovery_1_yards = int(play_arr[0][11])

                # if fumble_recovery_1_team == posteam:
                #     solo_tackle_1_team = defteam
                # elif fumble_recovery_1_team == defteam:
                #     is_fumble_lost = True
                #     solo_tackle_1_team = posteam

                # solo_tackle_1_player_name = play_arr[0][4]
            elif (
                "yard loss" in play["description"].lower() or
                "yards loss" in play["description"].lower()
            ) and (
                "fumbled by" in play["description"].lower() and
                "end of play" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yard[s]? loss to the ([0-9a-zA-Z\-]+) " +
                    r"fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) forced by " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) recovered by " +
                    r"([a-zA-Z]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"at ([0-9a-zA-Z\-]+), End Of Play",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2]) * -1
                yards_gained = rushing_yards
                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][4]
                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][7]
                fumble_recovery_1_player_name = play_arr[0][8]
                fumble_recovery_1_yards = 0

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                elif fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = posteam
            elif (
                "yard loss" in play["description"].lower() or
                "yards loss" in play["description"].lower()
            ) and (
                "fumbled by" in play["description"].lower() and
                "recovered by" in play["description"].lower() and
                "advances" in play["description"].lower() and
                "forced by" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_not_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for ([\-0-9]+) yard[s]? loss to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) forced by [\#0-9]+ ([a-zA-Z\.\s\-\']+) recovered by ([a-zA-Z]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) advances ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2]) * -1
                yards_gained = rushing_yards
                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][4]
                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][7]
                fumble_recovery_1_player_name = play_arr[0][8]
                fumble_recovery_1_yards = int(play_arr[0][10])

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][12]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "yard loss" in play["description"].lower() or
                "yards loss" in play["description"].lower()
            ) and (
                "fumbled by" in play["description"].lower() and
                "recovered by" in play["description"].lower() and
                "return" not in play["description"].lower() and
                "forced by" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_not_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yard[s]? loss to the ([0-9a-zA-Z\-]+) " +
                    r"fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) forced by " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) recovered by " +
                    r"([a-zA-Z]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"at ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2]) * -1
                yards_gained = rushing_yards
                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][4]
                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][7]
                fumble_recovery_1_player_name = play_arr[0][8]
                fumble_recovery_1_yards = 0

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][10]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "yard loss" in play["description"].lower() or
                "yards loss" in play["description"].lower()
            ) and (
                "fumbled by" in play["description"].lower() and
                "recovered by" in play["description"].lower() and
                "advances" in play["description"].lower() and
                "out of bounds at" in play["description"].lower() and
                "forced by" not in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_not_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for ([\-0-9]+) yard[s]? loss to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) recovered by ([a-zA-Z]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) advances ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+), out of bounds at",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2]) * -1
                yards_gained = rushing_yards
                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][4]
                # forced_fumble_player_1_team = defteam
                # forced_fumble_player_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][6]
                fumble_recovery_1_player_name = play_arr[0][7]
                fumble_recovery_1_yards = int(play_arr[0][9])

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                # tak_arr = re.findall(
                #     r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                #     play_arr[0][11]
                # )
                # if len(tak_arr) == 2:
                #     is_assist_tackle = True
                #     assist_tackle_1_player_name = tak_arr[0][0]
                #     assist_tackle_2_player_name = tak_arr[1][0]
                # elif len(tak_arr) == 1:
                #     solo_tackle_1_team = defteam
                #     solo_tackle_1_player_name = tak_arr[0]
                # else:
                #     raise ValueError(
                #         f"Unhandled play {play}"
                #     )
            elif (
                "yard loss" in play["description"].lower() or
                "yards loss" in play["description"].lower()
            ) and (
                "fumbled by" in play["description"].lower() and
                "recovered by" in play["description"].lower() and
                "advances" in play["description"].lower() and
                "forced by" not in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_not_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for ([\-0-9]+) yard[s]? loss to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) recovered by ([a-zA-Z]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) advances ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2]) * -1
                yards_gained = rushing_yards
                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][4]
                # forced_fumble_player_1_team = defteam
                # forced_fumble_player_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][6]
                fumble_recovery_1_player_name = play_arr[0][7]
                fumble_recovery_1_yards = int(play_arr[0][9])

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][11]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "yard loss" in play["description"].lower() or
                "yards loss" in play["description"].lower()
            ) and (
                "fumbled by" in play["description"].lower() and
                "recovered by" in play["description"].lower() and
                "return" not in play["description"].lower() and
                "forced by" not in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_not_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yard[s]? loss to the ([0-9a-zA-Z\-]+) " +
                    r"fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) recovered by " +
                    r"([a-zA-Z]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"at ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2]) * -1
                yards_gained = rushing_yards
                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][4]
                # forced_fumble_player_1_team = defteam
                # forced_fumble_player_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][6]
                fumble_recovery_1_player_name = play_arr[0][7]
                fumble_recovery_1_yards = 0

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][9]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "yard loss" in play["description"].lower() or
                "yards loss" in play["description"].lower()
            ) and (
                "fumbled by" in play["description"].lower() and
                "forced by" in play["description"].lower() and
                "lateral to" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_forced = True
                is_lateral_recovery = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for ([\-0-9]+) yard[s]? loss to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) forced by [\#0-9]+ ([a-zA-Z\.\s\-\']+) recovered by ([a-zA-Z]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yards to the ([0-9a-zA-Z\-]+) lateral to [\#0-9]+ ([a-zA-Z\.\s\-\']+) for ([\-0-9]+) yard[s]? gain to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2]) * -1
                yards_gained = rushing_yards
                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][4]
                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][7]
                fumble_recovery_1_player_name = play_arr[0][8]
                fumble_recovery_1_yards = int(play_arr[0][11])
                lateral_fumble_recovery_team = play_arr[0][7]
                lateral_fumble_recovery_player_name = play_arr[0][13]

                lateral_return_yards = int(play_arr[0][14])

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][16]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "yard loss" in play["description"].lower() or
                "yards loss" in play["description"].lower()
            ) and (
                "fumbled by" in play["description"].lower() and
                "forced by" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yard[s]? loss to the ([0-9a-zA-Z\-]+) " +
                    r"fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) forced by " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) recovered by " +
                    r"([a-zA-Z]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return ([\-0-9]+) yards to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2]) * -1
                yards_gained = rushing_yards
                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][4]
                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][7]
                fumble_recovery_1_player_name = play_arr[0][8]
                fumble_recovery_1_yards = int(play_arr[0][11])

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][13]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "yard loss" in play["description"].lower() or
                "yards loss" in play["description"].lower()
            ) and (
                "fumbled by" in play["description"].lower() and
                "montreal alouettes" in play["description"].lower()
            ):

                is_fumble = True
                is_fumble_not_forced = True
                play_arr = re.findall(
                    r"Montreal Alouettes rush ([a-zA-Z]+) for ([\-0-9]+) " +
                    r"yard[s]? loss to the ([0-9a-zA-Z\-]+) fumbled by " +
                    r"Montreal Alouettes at ([0-9a-zA-Z\-]+) recovered by " +
                    r"([a-zA-Z]+) ?[\#0-9]+? ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return " +
                    r"([\-0-9]+) yards to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                rusher_player_name = "-TEAM-"
                run_location = play_arr[0][0]
                rushing_yards = int(play_arr[0][1]) * -1
                yards_gained = rushing_yards
                fumbled_1_team = posteam
                fumbled_1_player_name = "-TEAM-"
                # forced_fumble_player_1_team = defteam
                # forced_fumble_player_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][4]
                fumble_recovery_1_player_name = play_arr[0][6]
                fumble_recovery_1_yards = int(play_arr[0][8])

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][10]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "yard loss" in play["description"].lower() or
                "yards loss" in play["description"].lower()
            ) and (
                "fumbled by" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_not_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yard[s]? loss to the ([0-9a-zA-Z\-]+) " +
                    r"fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) recovered by " +
                    r"([a-zA-Z]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return ([\-0-9]+) yards to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2]) * -1
                yards_gained = rushing_yards
                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][4]
                # forced_fumble_player_1_team = defteam
                # forced_fumble_player_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][6]
                fumble_recovery_1_player_name = play_arr[0][7]
                fumble_recovery_1_yards = int(play_arr[0][10])

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][12]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif "yard loss" in play["description"].lower() or\
                    "yards loss" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yard[s]? loss to the ([0-9a-zA-Z\-]+) " +
                    r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2]) * -1
                yards_gained = rushing_yards
                solo_tackle_1_team = defteam
                solo_tackle_1_player_name = play_arr[0][4]
            elif (
                "yard gain" in play["description"].lower() or
                "yards gain" in play["description"].lower()
            ) and "out of bounds" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yard[s]? gain to the ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                if len(play_arr) == 0:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush for " +
                        r"([\-0-9]+) yard[s]? gain to the ([0-9a-zA-Z\-]+)",
                        play["description"]
                    )
                    rusher_player_name = play_arr[0][0]
                    # run_location = play_arr[0][1]
                    rushing_yards = int(play_arr[0][1])
                    yards_gained = rushing_yards
                else:
                    rusher_player_name = play_arr[0][0]
                    run_location = play_arr[0][1]
                    rushing_yards = int(play_arr[0][2])
                    yards_gained = rushing_yards
            elif (
                "yard gain" in play["description"].lower() or
                "yards gain" in play["description"].lower()
            ) and "end of play" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yard[s]? gain to the ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2])
                yards_gained = rushing_yards
            elif (
                "yard gain" in play["description"].lower() or
                "yards gain" in play["description"].lower()
            ) and (
                "fumbled by" in play["description"].lower() and
                "forced by" in play["description"].lower() and
                "return" not in play["description"].lower() and
                "advances" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for ([\-0-9]+) yard[s]? gain to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) forced by [\#0-9]+ ([a-zA-Z\.\s\-\']+) recovered by ([a-zA-Z]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) advances ([\-0-9]+) yards to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2]) * -1
                yards_gained = rushing_yards
                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][4]
                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][7]
                fumble_recovery_1_player_name = play_arr[0][8]
                fumble_recovery_1_yards = int(play_arr[0][10])

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][12]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "yard gain" in play["description"].lower() or
                "yards gain" in play["description"].lower()
            ) and (
                "fumbled by" in play["description"].lower() and
                "return" not in play["description"].lower() and
                "advances" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for ([\-0-9]+) yard[s]? gain to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) recovered by ([a-zA-Z]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) advances ([\-0-9]+) yards to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2]) * -1
                yards_gained = rushing_yards
                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][4]
                # forced_fumble_player_1_team = defteam
                # forced_fumble_player_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][6]
                fumble_recovery_1_player_name = play_arr[0][7]
                fumble_recovery_1_yards = int(play_arr[0][9])

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][11]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "yard gain" in play["description"].lower() or
                "yards gain" in play["description"].lower()
            ) and (
                "fumbled by" in play["description"].lower() and
                "return" not in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yard[s]? gain to the ([0-9a-zA-Z\-]+) " +
                    r"fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) forced by " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) recovered by " +
                    r"([a-zA-Z]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"at ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2]) * -1
                yards_gained = rushing_yards
                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][4]
                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][7]
                fumble_recovery_1_player_name = play_arr[0][8]
                fumble_recovery_1_yards = 0

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][10]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "yard gain" in play["description"].lower() or
                "yards gain" in play["description"].lower()
            ) and (
                "fumbled by" in play["description"].lower() and
                "forced by" not in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_not_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yard[s]? gain to the ([0-9a-zA-Z\-]+) " +
                    r"fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) recovered by " +
                    r"([a-zA-Z]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2]) * -1
                yards_gained = rushing_yards
                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][4]
                # forced_fumble_player_1_team = defteam
                # forced_fumble_player_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][6]
                fumble_recovery_1_player_name = play_arr[0][7]
                fumble_recovery_1_yards = int(play_arr[0][10])

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][12]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "yard gain" in play["description"].lower() or
                "yards gain" in play["description"].lower()
            ) and (
                "fumbled by" in play["description"].lower() and
                "return for loss of" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yard[s]? gain to the ([0-9a-zA-Z\-]+) " +
                    r"fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) forced by " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) recovered by " +
                    r"([a-zA-Z]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return for loss of ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2]) * -1
                yards_gained = rushing_yards
                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][4]
                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][7]
                fumble_recovery_1_player_name = play_arr[0][8]
                fumble_recovery_1_yards = int(play_arr[0][11]) * -1

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][13]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "yard gain" in play["description"].lower() or
                "yards gain" in play["description"].lower()
            ) and "fumbled by" in play["description"].lower():
                is_fumble = True
                is_fumble_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yard[s]? gain to the ([0-9a-zA-Z\-]+) " +
                    r"fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) forced by " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) recovered by " +
                    r"([a-zA-Z]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2]) * -1
                yards_gained = rushing_yards
                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][4]
                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][7]
                fumble_recovery_1_player_name = play_arr[0][8]
                fumble_recovery_1_yards = int(play_arr[0][11])

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][13]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "yard gain" in play["description"].lower() or
                "yards gain" in play["description"].lower()
            ) and "rush for" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush for " +
                    r"([\-0-9]+) yard[s]? gain to the ([0-9a-zA-Z\-]+) " +
                    r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                # run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][1])
                yards_gained = rushing_yards
                solo_tackle_1_team = defteam
                solo_tackle_1_player_name = play_arr[0][3]
            elif (
                "yard gain" in play["description"].lower() or
                "yards gain" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yard[s]? gain to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2])
                yards_gained = rushing_yards
                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][4]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif "sacked for loss" in play["description"].lower():
                is_qb_dropback = True
                is_pass = True
                is_sack = True
                is_scrimmage_play = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) sacked for loss of " +
                    r"([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                    play["description"]
                )

                passer_player_name = play_arr[0][0]
                yards_gained = int(play_arr[0][1]) * -1
                tackle_for_loss_1_player_name = play_arr[0][3]
                sack_player_name = tackle_for_loss_1_player_name
            elif (
                "end of play" in play["description"].lower() and
                "yards to the" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2])
                yards_gained = rushing_yards
                # solo_tackle_1_team = defteam
                # solo_tackle_1_player_name = play_arr[0][4]
            elif "end of play" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yard[s]? gain to the ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2])
                yards_gained = rushing_yards
                # solo_tackle_1_team = defteam
                # solo_tackle_1_player_name = play_arr[0][4]
            elif (
                "kneel down" in play["description"].lower() and
                "for gain of" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"Kneel down by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) for gain of ([\-0-9]+) yards",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                rushing_yards = int(play_arr[0][2])
                yards_gained = rushing_yards
            elif ("kneel down" in play["description"].lower()):
                raise ValueError(
                    f"Unhandled play {play}"
                )
            elif "(" not in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2])
                yards_gained = rushing_yards
            else:
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2])
                yards_gained = rushing_yards
                solo_tackle_1_team = defteam
                solo_tackle_1_player_name = play_arr[0][4]

            if "first down" in play["description"].lower() or\
                    "1st down" in play["description"].lower():
                is_first_down_rush = True
            elif rushing_yards > yds_to_go:
                is_first_down_rush = True

            if "safety" in play["description"].lower():
                is_safety = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
                except Exception:
                    play_arr = re.findall(
                        r" ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]

            if "touchdown" in play["description"].lower():
                raise ValueError(
                    f"Unhandled play {play}"
                )
        elif (
            play["type"].lower() == "run" and
            play["subType"].lower() == "touchdown"
        ):
            is_scrimmage_play = True
            is_rush = True
            is_touchdown = True
            is_rush_touchdown = True

            if (
                "yards loss" in play["description"].lower() or
                "yard loss" in play["description"].lower()
            ) and (
                "fumbled" in play["description"].lower() and
                "forced by" in play["description"].lower() and
                "touchdown" in play["description"].lower()
            ):
                is_fumble_forced = True
                is_fumble = True
                is_return_touchdown = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yard[s]? loss to the ([0-9a-zA-Z\-]+) " +
                    r"fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) forced by " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"recovered by ([A-Z{2|3}]+) [\#0-9]+ " +
                    r"([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yards " +
                    r"to the ([0-9a-zA-Z\-]+) TOUCHDOWN, clock ([0-9\:]+)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                yards_gained = int(play_arr[0][2])
                rushing_yards = yards_gained
                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][4]
                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][6]
                fumble_recovery_1_team = play_arr[0][7]
                fumble_recovery_1_player_name = play_arr[0][8]
                td_team = fumble_recovery_1_team
                td_player_name = fumble_recovery_1_player_name
                return_yards = int(play_arr[0][11])
            elif (
                "yards gain" in play["description"].lower() or
                "yard gain" in play["description"].lower()
            ) and (
                "fumbled" in play["description"].lower() and
                "forced by" in play["description"].lower() and
                "touchdown" in play["description"].lower()
            ):
                is_fumble_forced = True
                is_fumble = True
                is_return_touchdown = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yard[s]? gain to the ([0-9a-zA-Z\-]+) " +
                    r"fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) forced by " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"recovered by ([A-Z{2|3}]+) [\#0-9]+ " +
                    r"([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yards " +
                    r"to the ([0-9a-zA-Z\-]+) TOUCHDOWN, clock ([0-9\:]+)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                yards_gained = int(play_arr[0][2])
                rushing_yards = yards_gained
                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][4]
                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][6]
                fumble_recovery_1_team = play_arr[0][7]
                fumble_recovery_1_player_name = play_arr[0][8]
                td_team = fumble_recovery_1_team
                td_player_name = fumble_recovery_1_player_name
                return_yards = int(play_arr[0][11])
            elif (
                "fumbled by" in play["description"].lower() and
                "forced by" in play["description"].lower()
            ):
                raise ValueError(
                    f"Unhandled play {play}"
                )
            elif (
                "fumbled by" in play["description"].lower() and
                "touchdown" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_not_forced = True
                is_return_touchdown = True
                is_rush_touchdown = False
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yard[s]? loss to the ([0-9a-zA-Z\-]+) " +
                    r"fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) recovered by ([A-Z{2|3}]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yards " +
                    r"to the ([0-9a-zA-Z\-]+) TOUCHDOWN",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                yards_gained = int(play_arr[0][2]) * -1
                rushing_yards = yards_gained
                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][4]
                fumble_recovery_1_team = play_arr[0][6]
                fumble_recovery_1_player_name = play_arr[0][7]
                td_team = fumble_recovery_1_team
                td_player_name = fumble_recovery_1_player_name

                if fumble_recovery_1_team == defteam:
                    is_fumble_lost = True
                return_yards = play_arr[0][10]
            elif "touchdown" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yard[s]? gain to the " +
                    r"([0-9a-zA-Z\-]+) TOUCHDOWN",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2])
                yards_gained = rushing_yards
                td_team = posteam
                td_player_name = rusher_player_name
            else:
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yard[s]? gain to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2])
                yards_gained = rushing_yards
                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][4]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_1_team = defteam
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )

            if "safety" in play["description"].lower():
                is_safety = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
                except Exception:
                    play_arr = re.findall(
                        r" ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
        elif (
            play["type"].lower() == "run" and
            play["subType"].lower() == "penalty"
        ):
            is_penalty = True
            is_rush = True
            is_scrimmage_play = True

            if (
                "yard loss" in play["description"].lower() or
                "yards loss" in play["description"].lower()
            ) and "lateral" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) " +
                    r"for ([\-0-9]+) yard[s]? loss to the " +
                    r"([0-9a-zA-Z\-]+) lateral to [\#0-9]+ " +
                    r"([a-zA-Z\.\s\-\']+) for ([\-0-9]+) yards to the " +
                    r"([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2]) * -1
                yards_gained = rushing_yards
                lateral_rusher_player_name = play_arr[0][4]
                lateral_rusher_yards = int(play_arr[0][5])
                yards_gained += lateral_rusher_yards

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][7]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_1_team = defteam
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
            elif (
                "yard loss" in play["description"].lower() or
                "yards loss" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yard[s]? loss to the ([0-9a-zA-Z\-]+) " +
                    r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2]) * -1
                yards_gained = rushing_yards
                solo_tackle_1_team = defteam
                solo_tackle_1_player_name = play_arr[0][4]
            elif (
                "yard gain" in play["description"].lower() or
                "yards gain" in play["description"].lower()
            ) and (
                "fumble" in play["description"].lower() and
                "return for loss of" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yard[s]? gain to the ([0-9a-zA-Z\-]+) " +
                    r"fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) forced by " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"recovered by ([a-zA-Z]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return for loss of ([\-0-9]+) yard[s]? to the " +
                    r"([0-9a-zA-Z\-]+) \([\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2])
                yards_gained = rushing_yards
                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][4]
                forced_fumble_player_1_team = defteam
                forced_fumble_player_1_player_name = play_arr[0][6]
                fumble_recovery_1_team = play_arr[0][7]
                fumble_recovery_1_player_name = play_arr[0][8]
                fumble_recovery_1_yards = int(play_arr[0][11]) * -1

                if fumble_recovery_1_team == defteam:
                    solo_tackle_1_team = posteam
                elif fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                solo_tackle_1_player_name = play_arr[0][13]
            elif (
                "yard gain" in play["description"].lower() or
                "yards gain" in play["description"].lower()
            ) and "touchdown" in play["description"].lower():
                is_rush_touchdown = True
                is_touchdown = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) " +
                    r"for ([\-0-9]+) yard[s]? gain to the " +
                    r"([0-9a-zA-Z\-]+) TOUCHDOWN",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2])
                yards_gained = rushing_yards
                td_team = posteam
                td_player_name = rusher_player_name
            elif (
                "yard gain" in play["description"].lower() or
                "yards gain" in play["description"].lower()
            ) and "end of play " in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) " +
                    r"for ([\-0-9]+) yard[s]? gain to the ([0-9a-zA-Z\-]+), " +
                    r"End Of Play",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2])
                yards_gained = rushing_yards
            elif (
                "yard gain" in play["description"].lower() or
                "yards gain" in play["description"].lower()
            ) and (
                "out of bounds at" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yard[s]? gain to the ([0-9a-zA-Z\-]+), " +
                    r"out of bounds at ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2])
                yards_gained = rushing_yards
                # solo_tackle_1_team = defteam
                # solo_tackle_1_player_name = play_arr[0][4]
            elif (
                "fumbled by" in play["description"].lower() and
                "return" in play["description"].lower() and
                "forced by" not in play["description"].lower()
            ) and (
                "yards gain" in play["description"].lower() or
                "yard gain" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for ([\-0-9]+) yard[s]? gain to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2])
                yards_gained = rushing_yards

                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][4]

                # forced_fumble_player_1_team = defteam
                # forced_fumble_player_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][6]
                fumble_recovery_1_player_name = play_arr[0][7]
                return_yards = int(play_arr[0][10])

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][12]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "fumbled by" in play["description"].lower() and
                "return" in play["description"].lower() and
                "forced by" in play["description"].lower()
            ) and (
                "yards gain" in play["description"].lower() or
                "yard gain" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for ([\-0-9]+) yard[s]? gain to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) forced by [\#0-9]+ ([a-zA-Z\.\s\-\']+) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2])
                yards_gained = rushing_yards

                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][4]

                fumble_recovery_1_team = play_arr[0][7]
                fumble_recovery_1_player_name = play_arr[0][8]
                return_yards = int(play_arr[0][11])

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][13]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif "yards gain (" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) " +
                    r"for ([\-0-9]+) yard[s]? gain " +
                    r"\([0-9]+\) to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2])
                yards_gained = rushing_yards
                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][4]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "yards gain" in play["description"].lower() or
                "yard gain" in play["description"].lower()
            ) and "end of play" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) " +
                    r"for ([\-0-9]+) yard[s]? gain to the " +
                    r"([0-9a-zA-Z\-]+), End Of Play",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2])
                yards_gained = rushing_yards
            elif (
                "yards gain" in play["description"].lower() or
                "yard gain" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) " +
                    r"for ([\-0-9]+) yard[s]? gain to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2])
                yards_gained = rushing_yards
                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][4]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )

            if "first down" in play["description"].lower():
                is_first_down_penalty = True

            penalty_arr = re.findall(
                r"PENALTY ([a-zA-Z0-9\s\(\)\#\.\,\-\']+)",
                play["description"]
            )[0]

            if (
                "illegal sub (too many men)" in penalty_arr.lower() and
                "#" not in penalty_arr.lower()
            ):
                play_arr = re.findall(
                    r"([A-Z]{2,4}) Illegal sub \(too many men\) " +
                    r"([\-0-9]+) yards from " +
                    r"([0-9a-zA-Z\-]+) to ([0-9a-zA-Z\-]+)",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = "Illegal sub (too many men)"
                penalty_yards = int(play_arr[0][1])
            elif (
                "yards from" in penalty_arr.lower() and
                "(" in penalty_arr.lower()
            ):
                try:
                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+) " +
                        r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\) " +
                        r"([\-0-9]+) yard[s]? from " +
                        r"([0-9a-zA-Z\-]+)? to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_player_name = play_arr[0][2]

                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+) " +
                        r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\) " +
                        r"([\-0-9]+) yard[s]? " +
                        r"from ([0-9a-zA-Z\-]+)? to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_player_name = play_arr[0][2]
                    penalty_yards = int(play_arr[0][3])
                except Exception:
                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+)\s" +
                        r"([\-0-9]+) yard[s]? from ([0-9a-zA-Z\-]+) " +
                        r"to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_yards = int(play_arr[0][2])
            elif (
                "yards from" in penalty_arr
            ):
                play_arr = re.findall(
                    r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+)\s" +
                    r"([\-0-9]+) yard[s]? from ([0-9a-zA-Z\-]+) " +
                    r"to ([0-9a-zA-Z\-]+)",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = play_arr[0][1]
                penalty_yards = int(play_arr[0][2])
            elif (
                "yards from" not in penalty_arr.lower() and
                "(" not in penalty_arr.lower() and
                "1st down" in penalty_arr.lower()
            ):
                play_arr = re.findall(
                    r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+), 1ST DOWN",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = play_arr[0][1]
                # penalty_yards = int(play_arr[0][2])
            else:
                play_arr = re.findall(
                    r"([A-Z]{2,4}) ([a-zA-Z\-\s\,0-9]+) " +
                    r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = play_arr[0][1]
                penalty_player_name = play_arr[0][2]
                # penalty_yards = int(play_arr[0][3])
            del penalty_arr

            if "safety" in play["description"].lower():
                is_safety = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
                except Exception:
                    play_arr = re.findall(
                        r" ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]

        elif (
            play["type"].lower() == "kneel" and
            play["subType"] is None
        ):
            is_qb_kneel = True
            is_scrimmage_play = True
            is_rush = True

            if "kneel down  at" in play["description"].lower():
                play_arr = re.findall(
                    r"Kneel down  at " +
                    r"([0-9a-zA-Z\-]+) for loss of ([\-0-9]+) yard[s]?",
                    play["description"]
                )
                rusher_player_name = "-TEAM-"
                # run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][1]) * -1
            elif (
                "kneel" not in play["description"].lower() and
                "#" not in play["description"].lower() and
                "penalty" in play["description"].lower() and
                "declined" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"PENALTY ([A-Z{2|3}]+) ([a-zA-Z\s]+) declined",
                    play["description"]
                )
                penalty_team = play_arr[0][0]
                penalty_type = play_arr[0][1]
            elif "gain of" in play["description"].lower():
                play_arr = re.findall(
                    r"Kneel down by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) for gain of ([\-0-9]+) yard[s]?",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                # run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2])
            else:
                play_arr = re.findall(
                    r"Kneel down by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) for loss of ([\-0-9]+) yard[s]?",
                    play["description"]
                )
                rusher_player_name = play_arr[0][0]
                # run_location = play_arr[0][1]
                rushing_yards = int(play_arr[0][2]) * -1
            if "safety" in play["description"].lower():
                is_safety = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
                except Exception:
                    play_arr = re.findall(
                        r" ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]

        # Fumble
        elif (
            play["type"].lower() == "fumble" and
            play["subType"] is None
        ):
            is_scrimmage_play = True
            is_aborted_play = True
            is_fumble_not_forced = True
            if (
                "fumbled snap at" in play["description"].lower() and
                "for loss of" in play["description"].lower() and
                "touchdown" in play["description"].lower()
            ):
                pass
            elif (
                "rush" in play["description"].lower() and
                "shotgun" in play["description"].lower() and
                "yards loss" in play["description"].lower() and
                play["description"].lower().count("fumbled") == 2
            ):
                is_rush = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) fumbled snap Shotgun " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yard[s]? loss to the ([0-9a-zA-Z\-]+) " +
                    r"fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) recovered by ([A-Z{2|3}]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) " +
                    r"yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                fumbled_1_player_name = play_arr[0][0]
                fumble_recovery_1_player_name = play_arr[0][1]
                rusher_player_name = play_arr[0][1]
                run_location = play_arr[0][2]
                rushing_yards = int(play_arr[0][3])
                yards_gained = rushing_yards

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][13]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_1_team = defteam
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "rush" in play["description"].lower() and
                "shotgun" in play["description"].lower() and
                "recovered by" in play["description"].lower() and
                "yards gain" in play["description"].lower()
            ):
                is_rush = True

                play_arr = re.findall(
                    r"Shotgun [\#0-9]+ ([a-zA-Z\.\s\-\']+) fumbled snap at " +
                    r"([0-9a-zA-Z\-]+) for loss of ([\-0-9]+) yard[s]? " +
                    r"recovered by ([A-Z{2,4}]+) [\#0-9]+ " +
                    r"([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) " +
                    r"for ([\-0-9]+) yard[s]? gain to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                fumble_recovery_1_team = posteam
                fumbled_1_player_name = play_arr[0][0]
                fumble_recovery_1_team = play_arr[0][3]
                fumble_recovery_1_player_name = play_arr[0][4]
                fumble_recovery_1_yards = int(play_arr[0][2]) * -1
                rusher_player_name = play_arr[0][6]
                run_location = play_arr[0][7]
                rushing_yards = int(play_arr[0][8])
                yards_gained = rushing_yards

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][10]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_1_team = defteam
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "rush" in play["description"].lower() and
                "shotgun" in play["description"].lower() and
                "yards gain" in play["description"].lower()
            ):
                is_rush = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) fumbled snap Shotgun " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yards gain to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                fumbled_1_player_name = play_arr[0][0]
                fumble_recovery_1_player_name = play_arr[0][1]
                rusher_player_name = play_arr[0][1]
                run_location = play_arr[0][2]
                rushing_yards = int(play_arr[0][3])
                yards_gained = rushing_yards

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][5]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_1_team = defteam
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "rush" in play["description"].lower() and
                "shotgun" in play["description"].lower()
            ):
                is_rush = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) fumbled snap Shotgun " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush ([a-zA-Z]+) for " +
                    r"([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                fumbled_1_player_name = play_arr[0][0]
                fumble_recovery_1_player_name = play_arr[0][1]
                rusher_player_name = play_arr[0][1]
                run_location = play_arr[0][2]
                rushing_yards = int(play_arr[0][3])
                yards_gained = rushing_yards

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][5]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_1_team = defteam
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "pass" in play["description"].lower() and
                "incomplete" in play["description"].lower() and
                "for loss of" in play["description"].lower() and
                "thrown to" in play["description"].lower()
            ):
                is_pass = True
                is_incomplete_pass = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) fumbled snap at ([0-9a-zA-Z\-]+) for loss of ([\-0-9]+) yard[s]? recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) pass incomplete ([a-z]+) ([a-z]+) to [\#0-9]+ ([a-zA-Z\.\s\-\']+) thrown to ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][0]
                fumble_recovery_1_team = posteam
                fumble_recovery_1_player_name = play_arr[0][4]
                fumble_recovery_1_yards = int(play_arr[0][2]) * -1

                passer_player_name = play_arr[0][6]
                pass_length = play_arr[0][7]
                pass_location = play_arr[0][8]
                receiver_player_name = play_arr[0][9]

                temp_ay = get_yardline(play_arr[0][10], posteam)
                air_yards = yardline_100 - temp_ay
            elif (
                "pass" in play["description"].lower() and
                "incomplete" in play["description"].lower()
            ):
                is_pass = True
                is_incomplete_pass = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) fumbled snap " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) pass incomplete " +
                    r"([a-z]+) ([a-z]+) to [\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"broken up by [\#0-9]+ ([a-zA-Z\.\s\-\']+)",
                    play["description"]
                )
                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][0]
                fumble_recovery_1_team = posteam
                fumble_recovery_1_player_name = play_arr[0][0]

                passer_player_name = play_arr[0][1]
                pass_length = play_arr[0][2]
                pass_location = play_arr[0][3]
                receiver_player_name = play_arr[0][4]

                pass_defense_1_player_name = play_arr[0][5]

            if "safety" in play["description"].lower():
                is_safety = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
                except Exception:
                    play_arr = re.findall(
                        r" ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
        elif (
            play["type"].lower() == "fumble" and
            play["subType"].lower() == "penalty"
        ):
            is_scrimmage_play = True
            penalty_arr = re.findall(
                r"PENALTY ([a-zA-Z0-9\s\(\)\#\.\,\-\']+)",
                play["description"]
            )[0]

            if "safety" in play["description"].lower():
                raise NotImplementedError(
                    "TODO: Implement safety logic for the following play:" +
                    f"\n{play}"
                )

            if (
                "yards from" in penalty_arr.lower() and
                "(" in penalty_arr.lower()
            ):
                try:
                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+) " +
                        r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\) " +
                        r"([\-0-9]+) yard[s]? from " +
                        r"([0-9a-zA-Z\-]+)? to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_player_name = play_arr[0][2]

                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+) " +
                        r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\) " +
                        r"([\-0-9]+) yard[s]? " +
                        r"from ([0-9a-zA-Z\-]+)? to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_player_name = play_arr[0][2]
                    penalty_yards = int(play_arr[0][3])
                except Exception:
                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+)\s" +
                        r"([\-0-9]+) yard[s]? from ([0-9a-zA-Z\-]+) " +
                        r"to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_yards = int(play_arr[0][2])
            elif (
                "yards from" in penalty_arr.lower()
            ):
                play_arr = re.findall(
                    r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+)\s" +
                    r"([\-0-9]+) yard[s]? from ([0-9a-zA-Z\-]+) " +
                    r"to ([0-9a-zA-Z\-]+)",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = play_arr[0][1]
                penalty_yards = int(play_arr[0][2])
            elif (
                "(" not in penalty_arr.lower() and
                "declined" in penalty_arr.lower()
            ):
                play_arr = re.findall(
                    r"([A-Z]{2,4}) ([a-zA-Z\-\s\,0-9]+) declined",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = play_arr[0][1]
                # penalty_player_name = play_arr[0][2]
                # penalty_yards = int(play_arr[0][3])
            elif ("offside, 1st down" in penalty_arr.lower()):
                play_arr = re.findall(
                    r"([A-Z{2,4}]+) Offside, 1ST DOWN",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = "Offside"
            else:
                play_arr = re.findall(
                    r"([A-Z]{2,4}) ([a-zA-Z\-\s\,0-9]+) " +
                    r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = play_arr[0][1]
                penalty_player_name = play_arr[0][2]
                # penalty_yards = int(play_arr[0][3])
            del penalty_arr

            if "safety" in play["description"].lower():
                is_safety = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
                except Exception:
                    play_arr = re.findall(
                        r" ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]

        # Punting
        elif (
            play["type"].lower() == "punt" and
            play["subType"] is None
        ):
            is_punt = True
            is_special_teams_play = True
            special_teams_play_type = "punt"
            punt_end_yl = 0

            if "sacked" in play["description"].lower():
                # Because CFL
                is_punt = False
                is_special_teams_play = False
                special_teams_play_type = None

                is_qb_dropback = True
                is_pass = True
                is_sack = True
                is_scrimmage_play = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) sacked for loss of " +
                    r"([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )

                passer_player_name = play_arr[0][0]
                yards_gained = int(play_arr[0][1]) * -1
                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][3]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                    tackle_for_loss_1_player_name = assist_tackle_1_player_name
                    tackle_for_loss_2_player_name = assist_tackle_2_player_name
                    half_sack_1_player_name = assist_tackle_1_player_name
                    half_sack_2_player_name = assist_tackle_2_player_name
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
                    tackle_for_loss_1_player_name = solo_tackle_1_player_name
                    sack_player_name = solo_tackle_1_player_name
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "return for loss of" in play["description"].lower() and
                "recovered by" in play["description"].lower() and
                "fumbled by" not in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) recovered by ([a-zA-Z]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return for loss of ([\-0-9]) yard[s]? to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][4]
                return_yards = int(play_arr[0][7]) * -1

                punt_end_yl = get_yardline(play_arr[0][8], posteam)
                if play_arr[0][3] == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif play_arr[0][3] == defteam:
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam
                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][9]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
                punt_end_yl = get_yardline(play_arr[0][8], posteam)
            elif (
                "return for loss of" in play["description"].lower() and
                "fumbled by" in play["description"].lower() and
                "forced by" in play["description"].lower() and
                "recovered by" in play["description"].lower() and
                "return" in play["description"].lower() and
                "touchdown" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_forced = True
                is_return_touchdown = True

                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return for loss of ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) forced by [\#0-9]+ ([a-zA-Z\.\s\-\']+) recovered by ([a-zA-Z{2|3}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) TOUCHDOWN",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4]) * -1

                fumbled_1_team = defteam
                fumbled_1_player_name = play_arr[0][6]

                forced_fumble_player_1_team = posteam
                forced_fumble_player_1_player_name = play_arr[0][8]

                fumble_recovery_1_team = play_arr[0][9]
                fumble_recovery_1_player_name = play_arr[0][10]
                fumble_recovery_1_yards = int(play_arr[0][13])

                if fumble_recovery_1_team == defteam:
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam
                elif fumble_recovery_1_team == posteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam

                # tak_arr = re.findall(
                #     r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                #     play_arr[0][15]
                # )
                # if len(tak_arr) == 2:
                #     is_assist_tackle = True
                #     assist_tackle_1_player_name = tak_arr[0][0]
                #     assist_tackle_2_player_name = tak_arr[1][0]
                # elif len(tak_arr) == 1:
                #     solo_tackle_1_player_name = tak_arr[0][0]
                # else:
                #     raise ValueError(
                #         f"Unhandled play {play}"
                #     )

                punt_end_yl = get_yardline(play_arr[0][14], posteam)
            elif (
                "return for loss of" in play["description"].lower() and
                "fumbled by" in play["description"].lower() and
                "forced by" in play["description"].lower() and
                "recovered by" in play["description"].lower() and
                "advances" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_forced = True

                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return for loss of ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) forced by [\#0-9]+ ([a-zA-Z\.\s\-\']+) recovered by ([a-zA-Z{2|3}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) advances ([\-0-9]+) yards to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4]) * -1

                fumbled_1_team = defteam
                fumbled_1_player_name = play_arr[0][6]

                forced_fumble_player_1_team = posteam
                forced_fumble_player_1_player_name = play_arr[0][8]

                fumble_recovery_1_team = play_arr[0][9]
                fumble_recovery_1_player_name = play_arr[0][10]
                fumble_recovery_1_yards = int(play_arr[0][12])

                if fumble_recovery_1_team == defteam:
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam
                elif fumble_recovery_1_team == posteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][14]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )

                punt_end_yl = get_yardline(play_arr[0][5], posteam)
            elif (
                "return for loss of" in play["description"].lower() and
                "fumbled by" in play["description"].lower() and
                "forced by" in play["description"].lower() and
                "recovered by" in play["description"].lower() and
                "return" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_forced = True

                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return for loss of ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) forced by [\#0-9]+ ([a-zA-Z\.\s\-\']+) recovered by ([a-zA-Z{2|3}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yards to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4]) * -1

                fumbled_1_team = defteam
                fumbled_1_player_name = play_arr[0][6]

                forced_fumble_player_1_team = posteam
                forced_fumble_player_1_player_name = play_arr[0][8]

                fumble_recovery_1_team = play_arr[0][9]
                fumble_recovery_1_player_name = play_arr[0][10]
                fumble_recovery_1_yards = int(play_arr[0][13])

                if fumble_recovery_1_team == defteam:
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam
                elif fumble_recovery_1_team == posteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][15]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )

                punt_end_yl = get_yardline(play_arr[0][5], posteam)
            elif (
                "return for loss of" in play["description"].lower() and
                "fumbled by" not in play["description"].lower() and
                "lateral to" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return for loss of ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) lateral to [\#0-9]+ ([a-zA-Z\.\s\-\']+) for ([\-0-9]+) yard[s]? gain to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4]) * -1
                lateral_punt_returner_player_name = play_arr[0][6]
                lateral_return_yards = int(play_arr[0][7])
                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][9]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_1_team = defteam
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
                punt_end_yl = get_yardline(play_arr[0][8], posteam)
            elif (
                "return for loss of" in play["description"].lower() and
                "fumbled by" not in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"punt ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return for loss of ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4]) * -1
                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][6]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_1_team = defteam
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
                punt_end_yl = get_yardline(play_arr[0][5], posteam)
            elif (
                "fumbled by" in play["description"].lower() and
                "forced by" in play["description"].lower() and
                play["description"].count("recovered by") == 2
            ):
                is_fumble_forced = True
                is_fumble = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"punt ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) recovered by ([a-zA-Z]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) return " +
                    r"([\-0-9]) yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) forced by " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"recovered by ([a-zA-Z]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][7])
                fumbled_1_team = defteam
                fumbled_1_player_name = play_arr[0][9]
                forced_fumble_player_1_team = posteam
                forced_fumble_player_1_player_name = play_arr[0][11]

                fumble_recovery_1_team = play_arr[0][12]
                if fumble_recovery_1_team == posteam:
                    is_fumble_lost = True

                fumble_recovery_1_player_name = play_arr[0][13]
                fumble_recovery_1_yards = int(play_arr[0][16])

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][18]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
                punt_end_yl = get_yardline(play_arr[0][17], posteam)
            elif (
                "fumbled by" in play["description"].lower() and
                "forced by" in play["description"].lower() and
                play["description"].count("recovered by") == 1
                and "return" in play["description"].lower() and
                "end of play" in play["description"].lower()
            ):
                is_fumble_forced = True
                is_fumble = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return ([\-0-9]+) yards to the ([0-9a-zA-Z\-]+) " +
                    r"fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) forced by " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"recovered by ([a-zA-Z]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"at ([0-9a-zA-Z\-]+), End Of Play",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4])
                fumbled_1_team = defteam
                fumbled_1_player_name = play_arr[0][6]
                forced_fumble_player_1_team = posteam
                forced_fumble_player_1_player_name = play_arr[0][8]

                fumble_recovery_1_team = play_arr[0][9]
                if fumble_recovery_1_team == posteam:
                    is_fumble_lost = True

                fumble_recovery_1_player_name = play_arr[0][10]
                fumble_recovery_1_yards = 0
                punt_end_yl = get_yardline(play_arr[0][11], posteam)
            elif (
                "fumbled by" in play["description"].lower() and
                "forced by" in play["description"].lower() and
                play["description"].count("recovered by") == 1
                and "return" in play["description"].lower() 
                and "out of bounds at" in play["description"].lower() 
            ):
                is_fumble_forced = True
                is_fumble = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yards to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) forced by [\#0-9]+ ([a-zA-Z\.\s\-\']+) recovered by ([a-zA-Z{2|3}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yards to the ([0-9a-zA-Z\-]+), out of bounds at ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4])
                fumbled_1_team = defteam
                fumbled_1_player_name = play_arr[0][6]
                forced_fumble_player_1_team = posteam
                forced_fumble_player_1_player_name = play_arr[0][8]

                fumble_recovery_1_team = play_arr[0][9]
                if fumble_recovery_1_team == posteam:
                    is_fumble_lost = True

                fumble_recovery_1_player_name = play_arr[0][10]
                fumble_recovery_1_yards = play_arr[0][13]

                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam
            elif (
                "fumbled by" in play["description"].lower() and
                "forced by" in play["description"].lower() and
                play["description"].count("recovered by") == 1
                and "return" in play["description"].lower()
            ):
                is_fumble_forced = True
                is_fumble = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                        r"punt ([\-0-9]+) yard[s]? " +
                        r"to the ([0-9a-zA-Z\-]+) " +
                        r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                        r"return ([\-0-9]+) yards to the ([0-9a-zA-Z\-]+) " +
                        r"fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                        r"([0-9a-zA-Z\-]+) forced by " +
                        r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) recovered by " +
                        r"([a-zA-Z{2|3}]+) " +
                        r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                        r"([0-9a-zA-Z\-]+) " +
                        r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) return " +
                        r"([\-0-9]+) yards to the ([0-9a-zA-Z\-]+) " +
                        r"\(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                        play["description"]
                    )
                    punter_player_name = play_arr[0][0]
                    kick_distance = int(play_arr[0][1])
                    punt_returner_player_name = play_arr[0][3]
                    return_yards = int(play_arr[0][4])
                    fumbled_1_team = defteam
                    fumbled_1_player_name = play_arr[0][6]
                    forced_fumble_player_1_team = posteam
                    forced_fumble_player_1_player_name = play_arr[0][8]

                    fumble_recovery_1_team = play_arr[0][9]
                    if fumble_recovery_1_team == posteam:
                        is_fumble_lost = True

                    fumble_recovery_1_player_name = play_arr[0][10]
                    fumble_recovery_1_yards = play_arr[0][13]

                    if fumble_recovery_1_team == posteam:
                        solo_tackle_1_team = defteam
                        assist_tackle_1_team = defteam
                        assist_tackle_2_team = defteam
                    elif fumble_recovery_1_team == defteam:
                        solo_tackle_1_team = posteam
                        assist_tackle_1_team = posteam
                        assist_tackle_2_team = posteam

                    tak_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                        play_arr[0][15]
                    )
                    if len(tak_arr) == 2:
                        is_assist_tackle = True
                        assist_tackle_1_player_name = tak_arr[0][0]
                        assist_tackle_2_player_name = tak_arr[1][0]
                    elif len(tak_arr) == 1:
                        solo_tackle_1_player_name = tak_arr[0]
                    else:
                        raise ValueError(
                            f"Unhandled play {play}"
                        )

                    punt_end_yl = get_yardline(play_arr[0][7], posteam)
                except Exception:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                        r"punt ([\-0-9]+) yard[s]? " +
                        r"to the ([0-9a-zA-Z\-]+) " +
                        r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                        r"return ([\-0-9]+) yards to the ([0-9a-zA-Z\-]+) " +
                        r"fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                        r"([0-9a-zA-Z\-]+) forced by " +
                        r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                        r"recovered by ([a-zA-Z]+) " +
                        r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) " +
                        r"\(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                        play["description"]
                    )
                    punter_player_name = play_arr[0][0]
                    kick_distance = int(play_arr[0][1])
                    punt_returner_player_name = play_arr[0][3]
                    return_yards = int(play_arr[0][4])
                    fumbled_1_team = defteam
                    fumbled_1_player_name = play_arr[0][6]
                    forced_fumble_player_1_team = posteam
                    forced_fumble_player_1_player_name = play_arr[0][8]

                    fumble_recovery_1_team = play_arr[0][9]
                    if fumble_recovery_1_team == posteam:
                        is_fumble_lost = True

                    fumble_recovery_1_player_name = play_arr[0][10]
                    fumble_recovery_1_yards = 0

                    if fumble_recovery_1_team == posteam:
                        solo_tackle_1_team = defteam
                        assist_tackle_1_team = defteam
                        assist_tackle_2_team = defteam
                    elif fumble_recovery_1_team == defteam:
                        solo_tackle_1_team = posteam
                        assist_tackle_1_team = posteam
                        assist_tackle_2_team = posteam

                    tak_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                        play_arr[0][12]
                    )
                    if len(tak_arr) == 2:
                        is_assist_tackle = True
                        assist_tackle_1_player_name = tak_arr[0][0]
                        assist_tackle_1_team = defteam
                        assist_tackle_2_player_name = tak_arr[1][0]
                    elif len(tak_arr) == 1:
                        solo_tackle_1_team = defteam
                        solo_tackle_1_player_name = tak_arr[0][0]
                    else:
                        raise ValueError(
                            f"Unhandled play {play}"
                        )
                    punt_end_yl = get_yardline(play_arr[0][11], posteam)
            elif (
                "recovered by" in play["description"].lower() and
                "return for loss" in play["description"].lower() and
                "touchdown" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_not_forced = True
                is_return_touchdown = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return for loss of ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) TOUCHDOWN",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4]) * -1
                fumbled_1_team = defteam
                fumbled_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][8]
                fumble_recovery_1_player_name = play_arr[0][9]
                fumble_recovery_1_yards = play_arr[0][12]

                td_team = play_arr[0][8]
                td_player_name = fumble_recovery_1_player_name
                punt_end_yl = get_yardline(play_arr[0][7], posteam)
            elif (
                "recovered by" in play["description"].lower() and
                "touchdown" in play["description"].lower() and
                "fumbled by" not in play["description"].lower()
            ):
                is_return_touchdown = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"punt ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) recovered by ([a-zA-Z]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) TOUCHDOWN",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][4]
                return_yards = int(play_arr[0][7])
                td_team = play_arr[0][3]
                td_player_name = punt_returner_player_name
                punt_end_yl = get_yardline(play_arr[0][8], posteam)
            elif (
                "recovered by" in play["description"].lower() and
                "return" not in play["description"].lower() and
                "blocked" in play["description"].lower()
            ):
                is_punt_blocked = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) blocked by [\#0-9]+ ([a-zA-Z\.\s\-\']+) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                blocked_player_name = play_arr[0][3]
                fumble_recovery_1_team = play_arr[0][4]
                fumble_recovery_1_player_name = play_arr[0][5]
                fumble_recovery_1_yards = 0

                if fumble_recovery_1_team == defteam:
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam
                elif fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][7]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_1_team = defteam
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
                punt_end_yl = get_yardline(play_arr[0][6], posteam)
            elif (
                "recovered by" in play["description"].lower() and
                "return" not in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"punt ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) recovered by ([a-zA-Z]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][4]
                return_yards = 0

                if fumble_recovery_1_team == defteam:
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam
                elif fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][6]
                )

                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_1_team = defteam
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
                # solo_tackle_1_player_name = play_arr[0][6]
                punt_end_yl = get_yardline(play_arr[0][5], posteam)
            elif (
                "recovered by" in play["description"].lower() and
                "blocked by" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) blocked by [\#0-9]+ ([a-zA-Z\.\s\-\']+) recovered by ([a-zA-Z]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                blocked_player_name = play_arr[0][3]
                fumble_recovery_1_team = play_arr[0][4]
                fumble_recovery_1_player_name = play_arr[0][5]
                return_yards = int(play_arr[0][8])
                if fumble_recovery_1_team == posteam:
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam
                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][10]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
                punt_end_yl = get_yardline(play_arr[0][2], posteam)
            elif (
                "fumbled by" in play["description"].lower() and
                "recovered by" in play["description"].lower() and
                "end of play" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_not_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+), End Of Play",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4])

                fumbled_1_team = defteam
                fumbled_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][8]
                fumble_recovery_1_player_name = play_arr[0][9]
                fumble_recovery_1_yards = 0

                if fumble_recovery_1_team == posteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam
                punt_end_yl = get_yardline(play_arr[0][10], posteam)
            elif (
                "fumbled by" in play["description"].lower() and
                "recovered by" in play["description"].lower() and
                "advances" in play["description"].lower() and
                "return for loss of" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_not_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return for loss of ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) advances ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4]) * -1

                fumbled_1_team = defteam
                fumbled_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][8]
                fumble_recovery_1_player_name = play_arr[0][9]
                fumble_recovery_1_yards = play_arr[0][11]

                if fumble_recovery_1_team == posteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][13]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
                punt_end_yl = get_yardline(play_arr[0][5], posteam)
            elif (
                "fumbled by" in play["description"].lower() and
                "recovered by" in play["description"].lower() and
                "return for loss of" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_not_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return for loss of ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4]) * -1

                fumbled_1_team = defteam
                fumbled_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][8]
                fumble_recovery_1_player_name = play_arr[0][9]
                fumble_recovery_1_yards = 0

                if fumble_recovery_1_team == posteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][11]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
                punt_end_yl = get_yardline(play_arr[0][10], posteam)
            elif (
                "fumbled by" in play["description"].lower() and
                "recovered by" in play["description"].lower() and
                "return" in play["description"].lower() and
                "advances for loss of" in play["description"].lower() and
                play["description"].lower().count("return") == 1
            ):
                is_fumble = True
                is_fumble_not_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) advances for loss of ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4])

                fumbled_1_team = defteam
                fumbled_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][8]
                fumble_recovery_1_player_name = play_arr[0][9]
                fumble_recovery_1_yards = int(play_arr[0][11]) * -1

                if fumble_recovery_1_team == posteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][13]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
                punt_end_yl = get_yardline(play_arr[0][12], posteam)
            elif (
                "fumbled by" in play["description"].lower() and
                "recovered by" in play["description"].lower() and
                "return" in play["description"].lower() and
                "advances" in play["description"].lower() and
                play["description"].lower().count("return") == 1
            ):
                is_fumble = True
                is_fumble_not_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) advances ([0-9\-]) yard[s]? to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4])

                fumbled_1_team = defteam
                fumbled_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][8]
                fumble_recovery_1_player_name = play_arr[0][9]
                fumble_recovery_1_yards = play_arr[0][11]

                if fumble_recovery_1_team == posteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][13]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
                punt_end_yl = get_yardline(play_arr[0][10], posteam)

            elif (
                "fumbled by" in play["description"].lower() and
                "recovered by" in play["description"].lower() and
                "return" in play["description"].lower() and
                play["description"].lower().count("return") == 1
            ):
                is_fumble = True
                is_fumble_not_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4])

                fumbled_1_team = defteam
                fumbled_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][8]
                fumble_recovery_1_player_name = play_arr[0][9]
                fumble_recovery_1_yards = 0

                if fumble_recovery_1_team == posteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][11]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
                punt_end_yl = get_yardline(play_arr[0][10], posteam)
            elif (
                "fumbled by" in play["description"].lower() and
                "recovered by" in play["description"].lower() and
                "return" in play["description"].lower() and
                play["description"].lower().count("return") == 2 and
                "touchdown" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_not_forced = True
                is_return_touchdown = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) TOUCHDOWN",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4])

                fumbled_1_team = defteam
                fumbled_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][8]
                fumble_recovery_1_player_name = play_arr[0][9]
                fumble_recovery_1_yards = int(play_arr[0][12])

                if fumble_recovery_1_team == posteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam
                punt_end_yl = get_yardline(play_arr[0][13], posteam)
            elif (
                "fumbled by" in play["description"].lower() and
                "recovered by" in play["description"].lower() and
                "return" in play["description"].lower() and
                play["description"].lower().count("return") == 2 and
                "touchdown" not in play["description"].lower() and
                "out of bounds at" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_not_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+), out of bounds at ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4])

                fumbled_1_team = defteam
                fumbled_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][8]
                fumble_recovery_1_player_name = play_arr[0][9]
                fumble_recovery_1_yards = int(play_arr[0][12])

                if fumble_recovery_1_team == posteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam
                punt_end_yl = get_yardline(play_arr[0][14], posteam)
            elif (
                "fumbled by" in play["description"].lower() and
                "recovered by" in play["description"].lower() and
                "return" in play["description"].lower() and
                play["description"].lower().count("return") == 2 and
                "touchdown" not in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_not_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4])

                fumbled_1_team = defteam
                fumbled_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][8]
                fumble_recovery_1_player_name = play_arr[0][9]
                fumble_recovery_1_yards = int(play_arr[0][12])

                if fumble_recovery_1_team == posteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][14]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
                punt_end_yl = get_yardline(play_arr[0][13], posteam)
            elif (
                "fumbled by" in play["description"].lower() and
                "recovered by" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_not_forced = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4])

                fumbled_1_team = defteam
                fumbled_1_player_name = play_arr[0][6]

                fumble_recovery_1_team = play_arr[0][8]
                fumble_recovery_1_player_name = play_arr[0][9]
                fumble_recovery_1_yards = 0

                if fumble_recovery_1_team == posteam:
                    is_fumble_lost = True
                    solo_tackle_1_team = defteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                elif fumble_recovery_1_team == defteam:
                    solo_tackle_1_team = posteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][11]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
                punt_end_yl = get_yardline(play_arr[0][10], posteam)
            elif (
                "muffed by" in play["description"].lower() and
                "recovered by" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) muffed by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][4]
                return_yards = 0

                fumbled_1_team = defteam
                fumbled_1_player_name = punt_returner_player_name

                fumble_recovery_1_team = play_arr[0][5]
                fumble_recovery_1_player_name = play_arr[0][6]
                fumble_recovery_1_yards = int(play_arr[0][9])

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][11]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = posteam
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
                punt_end_yl = get_yardline(play_arr[0][10], posteam)

            elif "recovered by" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"punt ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) recovered by ([a-zA-Z]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][4]
                return_yards = int(play_arr[0][7])

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][9]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = posteam
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
                punt_end_yl = get_yardline(play_arr[0][8], posteam)
            elif (
                "), out of bounds" in play["description"].lower() and
                "return" in play["description"].lower() and
                "lateral to" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) lateral to [\#0-9]+ ([a-zA-Z\.\s\-\']+) for ([\-0-9]+) yard[s]? gain to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\), out of bounds",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4])
                lateral_punt_returner_player_name = play_arr[0][6]
                lateral_return_yards = int(play_arr[0][7])

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][9]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = posteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
                punt_end_yl = get_yardline(play_arr[0][8], posteam)
            elif (
                "), out of bounds" in play["description"].lower() and
                "return" in play["description"].lower()
            ):
                # is_punt_out_of_bounds = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;]+)\), out of bounds",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4])

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][6]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = posteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
                punt_end_yl = get_yardline(play_arr[0][5], posteam)
            elif (
                "fumbled by" in play["description"].lower() and
                "forced by" in play["description"].lower() and
                "out of bounds" in play["description"].lower() and
                play["description"].lower().count("return") == 1
            ):
                is_fumble = True
                is_fumble_forced = True
                is_fumble_out_of_bounds = True
                is_punt_out_of_bounds = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) forced by [\#0-9]+ ([a-zA-Z\.\s\-\']+), out of bounds at ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = play_arr[0][4]

                fumbled_1_team = defteam
                fumbled_1_player_name = play_arr[0][6]
                punt_end_yl = get_yardline(play_arr[0][5], posteam)
            elif (
                "fumbled by" in play["description"].lower() and
                "out of bounds" in play["description"].lower() and
                "return for loss of" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_not_forced = True
                is_fumble_out_of_bounds = True
                is_punt_out_of_bounds = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return for loss of ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+), out of bounds at ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4]) * -1

                fumbled_1_team = defteam
                fumbled_1_player_name = play_arr[0][6]
                punt_end_yl = get_yardline(play_arr[0][8], posteam)
            elif (
                "fumbled by" in play["description"].lower() and
                "out of bounds" in play["description"].lower() and
                "return" in play["description"].lower()
            ):
                is_fumble = True
                is_fumble_not_forced = True
                is_fumble_out_of_bounds = True
                is_punt_out_of_bounds = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+), out of bounds at ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4])

                fumbled_1_team = defteam
                fumbled_1_player_name = play_arr[0][6]
                punt_end_yl = get_yardline(play_arr[0][8], posteam)
            elif (
                "out of bounds" in play["description"].lower() and
                "return" in play["description"].lower()
            ):
                is_punt_out_of_bounds = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+), " +
                    r"out of bounds at ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = play_arr[0][4]
                punt_end_yl = get_yardline(play_arr[0][6], posteam)
            elif "out of bounds" in play["description"].lower():
                is_punt_out_of_bounds = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"punt ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+), " +
                    r"out of bounds at ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_end_yl = get_yardline(play_arr[0][3], posteam)

            elif "touchdown" in play["description"].lower():
                is_return_touchdown = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return ([\-0-9]+) yard[s]? to the " +
                    r"([0-9a-zA-Z\-]+) TOUCHDOWN, clock ([0-9\:]+)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4])
                td_team = defteam
                td_player_name = punt_returner_player_name
                punt_end_yl = get_yardline(play_arr[0][5], posteam)
            elif "  return 0 yards to the" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+)  return 0 yards to the",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = None
                return_yards = 0
                punt_end_yl = get_yardline(play_arr[0][2], posteam)
            elif "(" not in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt " +
                    r"([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) " +
                    r"yard[s]? to the ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4])
                punt_end_yl = get_yardline(play_arr[0][5], posteam)
            elif "lateral to" in play["description"].lower():
                is_lateral_return = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"lateral to [\#0-9]+ ([a-zA-Z\.\s\-\']+) for " +
                    r"([\-0-9]+) yard[s]? gain to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4])
                lateral_punt_returner_player_name = play_arr[0][6]
                lateral_return_yards = int(play_arr[0][7])

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][9]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_1_team = posteam
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = posteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
                punt_end_yl = get_yardline(play_arr[0][8], posteam)
            elif "return" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt " +
                    r"([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) " +
                    r"yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4])

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][6]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = posteam
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
                punt_end_yl = get_yardline(play_arr[0][5], posteam)
            else:
                raise ValueError(
                    f"Unhandled play {play}"
                )
            # if (
            #     "block" in play["description"].lower() and
            #     "illegal block" not in play["description"].lower()
            # ):
            #     raise ValueError(
            #         f"Unhandled play {play}"
            #     )

            # if "touchdown" in play["description"].lower():
            #     raise ValueError(
            #         f"Unhandled play {play}"
            #     )
            if "safety" in play["description"].lower():
                is_safety = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
                except Exception:
                    play_arr = re.findall(
                        r" ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]

            if "downed" in play["description"].lower():
                is_punt_downed = True
        elif (
            play["type"].lower() == "punt" and
            play["subType"].lower() == "single"
        ):
            is_punt = True
            is_special_teams_play = True
            special_teams_play_type = "punt"
            is_rouge = True
            is_punt_in_endzone = True

            if (
                "out of bounds" in play["description"].lower() and
                "return" in play["description"].lower()
            ):
                is_punt_out_of_bounds = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+), out of bounds at ([0-9a-zA-Z\-]+) SINGLE",
                    play["description"]
                )
                punter_player_name = play_arr[0][3]
                kick_distance = play_arr[0][1]
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4])
            elif "out of bounds" in play["description"].lower():
                is_punt_out_of_bounds = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"punt ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+), out of bounds " +
                    r"at ([0-9a-zA-Z\-]+) SINGLE",
                    play["description"]
                )
                punter_player_name = play_arr[0][3]
                kick_distance = play_arr[0][1]
            elif "return for loss of" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"punt ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return for loss of ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+)" +
                    r"( \([\#0-9]+ ([a-zA-Z\.\s\-\']+)\))?",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = play_arr[0][1]
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4]) * -1
                solo_tackle_1_team = posteam
                solo_tackle_1_player_name = play_arr[0][6]
            elif "touchback" in play["description"].lower():
                is_touchback = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt " +
                    r"([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+), " +
                    r"Touchback SINGLE, clock ([\:0-9]+)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = play_arr[0][1]
            elif (
                "recovered by" in play["description"].lower() and
                "end of play single" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) " +
                    r"yard[s]? to the ([0-9a-zA-Z\-]+) recovered by " +
                    r"([A-Z{2|3}]+) [\#0-9 ]+? ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+), End Of Play SINGLE",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = play_arr[0][1]
                punt_returner_player_name = play_arr[0][4]
                return_yards = 0
            elif "(" not in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"punt ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) ",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
            else:
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"punt ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+)" +
                    r"( \([\#0-9]+ ([a-zA-Z\.\s\-\']+)\))?",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = play_arr[0][1]
                return_yards = int(play_arr[0][4])
                punt_returner_player_name = play_arr[0][3]
                solo_tackle_1_team = posteam
                solo_tackle_1_player_name = play_arr[0][6]

            punter_player_name = play_arr[0][0]
            kick_distance = int(play_arr[0][1])
            # if len(play_arr[0]) >= 7:
            #     solo_tackle_1_team = posteam
            #     solo_tackle_1_player_name = play_arr[0][6]

            # This should never be the case,
            # # but let's make sure it's caught anyways.
            if "downed" in play["description"].lower():
                is_punt_downed = True

            if "safety" in play["description"].lower():
                is_safety = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
                except Exception:
                    play_arr = re.findall(
                        r" ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
        elif (
            play["type"].lower() == "punt" and
            play["subType"].lower() == "penalty"
        ):
            play_arr = None
            is_punt = True
            is_special_teams_play = True
            special_teams_play_type = "punt"
            # is_rouge = True

            if (
                "touchdown nullified by penalty" in play["description"].lower() and
                "no yards, 15 yards" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"TOUCHDOWN nullified by penalty, clock ([0-9\:]+) [PENALTY|penalty]+ ([A-Z]+) No yards, 15 yards \([\#0-9]+ ([a-zA-Z\.\s\-\']+)\) ([0-9]+) yard[s]+ from ([0-9a-zA-Z\-]+) to ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                test_str = play_arr[0][0]

                del test_str
            elif "return for loss of" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"punt ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return for loss of ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+)" +
                    r"( \([\#0-9]+ ([a-zA-Z\.\s\-\']+)\))?",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4]) * -1
                # solo_tackle_1_team = posteam
                # solo_tackle_1_player_name = play_arr[0][6]
            elif (
                "recovered by" in play["description"].lower() and
                "out of bounds at" in play["description"].lower()
            ):
                is_punt_out_of_bounds = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt " +
                    r"([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"recovered by ([a-zA-Z]+) [\#0-9 ]+? " +
                    r"([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) return " +
                    r"([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+), " +
                    r"out of bounds at ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][4]
                return_yards = int(play_arr[0][7])
            elif (
                "recovered by" in play["description"].lower() and
                "touchdown" in play["description"].lower()
            ):
                is_return_touchdown = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                        r"punt ([\-0-9]+) yard[s]? " +
                        r"to the ([0-9a-zA-Z\-]+) recovered by ([a-zA-Z]+) " +
                        r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) " +
                        r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                        r"return ([\-0-9]+) yards " +
                        r"to the ([0-9a-zA-Z\-]+) TOUCHDOWN",
                        play["description"]
                    )
                    punter_player_name = play_arr[0][0]
                    kick_distance = int(play_arr[0][1])
                    punt_returner_player_name = play_arr[0][4]
                    return_yards = play_arr[0][7]
                    td_team = play_arr[0][3]
                    td_player_name = punt_returner_player_name
                except Exception:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt " +
                        r"([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) " +
                        r"recovered by ([a-zA-Z]+) " +
                        r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                        r"([0-9a-zA-Z\-]+) TOUCHDOWN",
                        play["description"]
                    )
                    punter_player_name = play_arr[0][0]
                    kick_distance = int(play_arr[0][1])
                    punt_returner_player_name = play_arr[0][4]
                    temp_yardline = get_yardline(
                        yardline=play_arr[0][5],
                        posteam=posteam
                    )
                    return_yards = 110 - temp_yardline
                    td_team = play_arr[0][3]
                    td_player_name = punt_returner_player_name
            elif (
                "recovered by" in play["description"].lower() and
                "end of play" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt " +
                    r"([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"recovered by ([a-zA-Z]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return ([\-0-9]+) yard[s]? to the " +
                    r"([0-9a-zA-Z\-]+), End Of Play",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][4]
                return_yards = int(play_arr[0][7])
            elif (
                "fumbled by" in play["description"].lower() and
                "forced by" in play["description"].lower() and
                "recovered by" in play["description"].lower() and
                "advances" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) forced by [\#0-9]+ ([a-zA-Z\.\s\-\']+) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) advances ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4])
                fumbled_1_team = defteam
                fumbled_1_player_name = play_arr[0][6]
                fumble_recovery_1_team = play_arr[0][9]

                forced_fumble_player_1_team = posteam
                forced_fumble_player_1_player_name = play_arr[0][8]
                fumble_recovery_1_player_name = play_arr[0][10]
                fumble_recovery_1_yards = int(play_arr[0][12])

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][14]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_1_team = defteam
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "fumbled by" in play["description"].lower() and
                "recovered by" in play["description"].lower() and
                "advances" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) advances ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4])
                fumbled_1_team = defteam
                fumbled_1_player_name = play_arr[0][6]
                fumble_recovery_1_team = play_arr[0][8]
                fumble_recovery_1_player_name = play_arr[0][9]
                fumble_recovery_1_yards = play_arr[0][11]

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][13]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_1_team = defteam
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "recovered by" in play["description"].lower() and
                "blocked by" in play["description"].lower() and
                "yards to the penalty" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) blocked by [\#0-9]+ ([a-zA-Z\.\s\-\']+) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                blocked_player_name = play_arr[0][3]
                fumble_recovery_1_team = play_arr[0][4]
                fumble_recovery_1_player_name = play_arr[0][5]
                fumble_recovery_1_yards = play_arr[0][8]
            elif (
                "recovered by" in play["description"].lower() and
                "fumbled by" in play["description"].lower() and
                "forced by" not in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) fumbled by [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) recovered by ([a-zA-Z]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) \([\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4])
                fumbled_1_team = defteam
                fumbled_1_player_name = play_arr[0][6]
                fumble_recovery_1_team = play_arr[0][8]
                fumble_recovery_1_player_name = play_arr[0][9]
                fumble_recovery_1_yards = int(play_arr[0][12])

                if fumble_recovery_1_team == posteam:
                    is_fumble_lost = True
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                    solo_tackle_1_team = defteam
                elif fumble_recovery_1_team == defteam:
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam
                    solo_tackle_1_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][5]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    # assist_tackle_1_team = defteam
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    # solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
            elif (
                "recovered by" in play["description"].lower() and
                "blocked by" in play["description"].lower() and
                "return" in play["description"].lower()
            ):
                is_punt_blocked = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) blocked by [\#0-9]+ ([a-zA-Z\.\s\-\']+) recovered by ([a-zA-Z]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                blocked_player_name = play_arr[0][3]

                if play_arr[0][4] == posteam:
                    fumble_recovery_1_team = posteam
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                    solo_tackle_1_team = defteam
                elif play_arr[0][4] == defteam:
                    fumble_recovery_1_team = defteam
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam
                    solo_tackle_1_team = posteam
                fumble_recovery_1_player_name = play_arr[0][5]
                fumble_recovery_1_yards = play_arr[0][8]

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][10]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif "recovered by" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"punt ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) recovered by ([a-zA-Z]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) " +
                    r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][7])
            elif (
                "return" in play["description"].lower() and
                "end of play" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return ([\-0-9]+) yard[s]? to the " +
                    r"([0-9a-zA-Z\-]+), End Of Play",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4])
            elif (
                "return" in play["description"].lower() and
                "), out of bounds" in play["description"].lower()
            ):
                is_punt_out_of_bounds = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) " +
                    r"yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) " +
                    r"yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\), out of bounds",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4])
                solo_tackle_1_team = posteam
                solo_tackle_1_player_name = play_arr[0][6]
            elif (
                "return" in play["description"].lower() and
                "touchdown nullified" in play["description"].lower()
            ):
                is_return_touchdown = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return ([\-0-9]+) yard[s]? to the " +
                    r"([0-9a-zA-Z\-]+) TOUCHDOWN nullified by penalty",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4])
                td_team = defteam
                td_player_name = punt_returner_player_name
            elif (
                "out of bounds" in play["description"].lower() and
                "return" in play["description"].lower() and
                "hold, return" not in play["description"].lower() and
                "illegal block, return" not in play["description"].lower()
            ):
                is_punt_out_of_bounds = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+), out of bounds at ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4])

            elif "out of bounds" in play["description"].lower():
                is_punt_out_of_bounds = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"punt ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+), " +
                    r"out of bounds at ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
            elif "punt" not in play["description"].lower():
                # If there's no punt in this play,
                # we don't need to parse the punt in this play.
                pass
            elif (
                "return" in play["description"].lower() and
                "yards to the penalty" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return ([\-0-9]+) yard[s]? to the PENALTY",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = play_arr[0][4]
            elif "yards to the (" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"punt ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return ([\-0-9]+) yard[s]? to the " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4])

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][5]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_1_team = defteam
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]
            elif (
                "return" in play["description"].lower() and
                "touchdown" in play["description"].lower()
            ):
                is_return_touchdown = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"punt ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return ([\-0-9]+) yard[s]? to " +
                    r"the ([0-9a-zA-Z\-]+) TOUCHDOWN",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4])
                td_team = defteam
                td_player_name = punt_returner_player_name
            elif "touchback" in play["description"].lower():
                is_touchback = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) punt ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+), Touchback",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
            else:
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"punt ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;\,]+)\)",
                    play["description"]
                )
                punter_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                punt_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4])

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][6]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_1_team = defteam
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0]

            if (
                play_arr is not None and
                len(play_arr[0]) >= 7 and
                "recovered by" not in play["description"].lower()
            ):
                solo_tackle_1_team = posteam
                solo_tackle_1_player_name = play_arr[0][6]

            penalty_arr = re.findall(
                r"PENALTY ([a-zA-Z0-9\s\(\)\#\.\,\-\']+)",
                play["description"]
            )[0]

            if (
                "yards from" in penalty_arr.lower() and
                "(" in penalty_arr.lower()
            ):
                try:
                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+) " +
                        r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\) " +
                        r"([\-0-9]+) yard[s]? from " +
                        r"([0-9a-zA-Z\-]+)? to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_player_name = play_arr[0][2]

                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+) " +
                        r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\) " +
                        r"([\-0-9]+) yard[s]? " +
                        r"from ([0-9a-zA-Z\-]+)? to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_player_name = play_arr[0][2]
                    penalty_yards = int(play_arr[0][3])
                except Exception:
                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+)\s" +
                        r"([\-0-9]+) yard[s]? from ([0-9a-zA-Z\-]+) " +
                        r"to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_yards = int(play_arr[0][2])
            elif (
                "yards from" in penalty_arr
            ):
                play_arr = re.findall(
                    r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+)\s" +
                    r"([\-0-9]+) yard[s]? from ([0-9a-zA-Z\-]+) " +
                    r"to ([0-9a-zA-Z\-]+)",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = play_arr[0][1]
                penalty_yards = int(play_arr[0][2])
            elif (
                "yards" not in penalty_arr and
                "loss of down" in penalty_arr.lower()
            ):
                play_arr = re.findall(
                    r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+) - " +
                    r"Loss of Down \([\#0-9]+ ([a-zA-Z\.\-\s\']+)\)",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = play_arr[0][1]
                penalty_player_name = play_arr[0][2]
            elif (
                "hold, return" in penalty_arr.lower() and
                "(" not in penalty_arr.lower()
            ):
                play_arr = re.findall(
                    r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+) ([\-0-9]) yard",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = play_arr[0][1]
                penalty_yards = int(play_arr[0][2])
            else:
                play_arr = re.findall(
                    r"([A-Z]{2,4}) ([a-zA-Z\-\s\,0-9]+) " +
                    r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = play_arr[0][1]
                penalty_player_name = play_arr[0][2]
                # penalty_yards = int(play_arr[0][3])
            del penalty_arr

            if "downed" in play["description"].lower():
                is_punt_downed = True

            if "safety" in play["description"].lower():
                is_safety = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
                except Exception:
                    play_arr = re.findall(
                        r" ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]

        # Kickoff
        elif (
            play["type"].lower() == "kickoff"
            and play["subType"] is None
        ):
            is_kickoff_attempt = True
            is_special_teams_play = True
            special_teams_play_type = "kickoff"

            if (
                "onside kickoff" in play["description"].lower() and
                "out of bounds" in play["description"].lower() and
                "return" not in play["description"].lower()
            ):
                is_kickoff_out_of_bounds = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) onside kickoff " +
                    r"([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+), " +
                    r"out of bounds at ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                kickoff_end_yl = get_yardline(play_arr[0][3], posteam)

                if kickoff_end_yl < 30:
                    kickoff_end_yl = 30
            elif (
                "onside kickoff" in play["description"].lower() and
                "end of play" in play["description"].lower() and
                "return" not in play["description"].lower()
            ):
                is_kickoff_downed = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"onside kickoff ([\-0-9]+) " +
                    r"yard[s]? to the ([0-9a-zA-Z\-]+), End Of Play",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                kickoff_end_yl = get_yardline(play_arr[0][2], posteam)
            elif "onside kickoff" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"onside kickoff ([\-0-9]+) yard[s]? to the " +
                    r"([0-9a-zA-Z\-]+) (\#[0-9]+) " +
                    r"([a-zA-Z\.\-\s\']+) return ([\-0-9]+) " +
                    r"yard[s]? to the ([0-9a-zA-Z\-]+)" +
                    r"( \([\#0-9]+ ([a-zA-Z\.\s\-\']+)\))?",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                kickoff_returner_player_name = play_arr[0][3]
                solo_tackle_1_player_name = play_arr[0][7]
                kickoff_end_yl = get_yardline(play_arr[0][6], posteam)
            elif (
                "out of bounds at" in play["description"].lower() and
                "return" not in play["description"].lower()
            ):
                is_kickoff_out_of_bounds = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"kickoff ([\-0-9]+) yard[s]? " +
                    r"to the ([0-9a-zA-Z\-]+), out of bounds " +
                    r"at ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                kickoff_end_yl = get_yardline(play_arr[0][2], posteam)

                if kickoff_end_yl < 30:
                    kickoff_end_yl = 30
            elif "return for loss of" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) kickoff ([\-0-9]+) " +
                    r"yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) return for loss of " +
                    r"([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+)" +
                    r"( \([\#0-9]+ ([a-zA-Z\.\s\-\']+)\))?",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                kickoff_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4]) * -1
                solo_tackle_1_player_name = play_arr[0][7]
                kickoff_end_yl = get_yardline(play_arr[0][5], posteam)
            else:
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) kickoff ([\-0-9]+) " +
                    r"yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) return " +
                    r"([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+)" +
                    r"( \([\#0-9]+ ([a-zA-Z\.\s\-\']+)\))?",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                kickoff_returner_player_name = play_arr[0][3]
                return_yards = int(play_arr[0][4])
                solo_tackle_1_player_name = play_arr[0][7]
                kickoff_end_yl = get_yardline(play_arr[0][5], posteam)

            if "downed" in play["description"].lower():
                is_kickoff_downed = True

            if play["teamId"] == home_team_id:
                return_team = home_team_abv
                solo_tackle_1_team = away_team_abv
            elif play["teamId"] == away_team_id:
                return_team = away_team_abv
                solo_tackle_1_team = home_team_abv
            else:
                raise ValueError(
                    f"Unhandled return team in the play:\n{play}"
                )
            if "safety" in play["description"].lower():
                is_safety = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
                except Exception:
                    play_arr = re.findall(
                        r" ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
        elif (
            play["type"].lower() == "kickoff" and
            play["subType"].lower() == "penalty"
        ):
            is_kickoff_attempt = True
            is_special_teams_play = True
            special_teams_play_type = "kickoff"

            if (
                "onside kickoff" in play["description"].lower() and
                "return" not in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"onside kickoff ([\-0-9]+) yard[s]? to the " +
                    r"([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                kickoff_end_yl = get_yardline(play_arr[0][2], posteam)
            elif "onside kickoff" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"onside kickoff ([\-0-9]+) yard[s]? to the " +
                    r"([0-9a-zA-Z\-]+) (\#[0-9]+) " +
                    r"([a-zA-Z\.\-\s\']+) return ([\-0-9]+) " +
                    r"yard[s]? to the ([0-9a-zA-Z\-]+)" +
                    r"( \([\#0-9]+ ([a-zA-Z\.\s\-\']+)\))?",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                kickoff_returner_player_name = play_arr[0][4]
                solo_tackle_1_player_name = play_arr[0][8]
                kickoff_end_yl = get_yardline(play_arr[0][6], posteam)
            elif "return for loss of" in play["description"]:
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) kickoff ([\-0-9]+) " +
                    r"yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) return for loss of " +
                    r"([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+)" +
                    r"( \([\#0-9]+ ([a-zA-Z\.\s\-\']+)\))?",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                kickoff_returner_player_name = play_arr[0][3]
                return_yards = play_arr[0][4]
                solo_tackle_1_player_name = play_arr[0][7]
                kickoff_end_yl = get_yardline(play_arr[0][5], posteam)
            elif (
                "out of bounds at" in play["description"].lower() and
                "illegal kickoff" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) kickoff ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+), out of bounds at ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                kickoff_end_yl = get_yardline(play_arr[0][3], posteam)
            else:
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) kickoff ([\-0-9]+) " +
                    r"yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) return " +
                    r"([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+)" +
                    r"( \([\#0-9]+ ([a-zA-Z\.\s\-\']+)\))?",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                kickoff_returner_player_name = play_arr[0][4]
                solo_tackle_1_player_name = play_arr[0][7]
                kickoff_end_yl = get_yardline(play_arr[0][4], posteam)

            if play["teamId"] == home_team_id:
                return_team = home_team_abv
                solo_tackle_1_team = away_team_abv
            elif play["teamId"] == away_team_id:
                return_team = away_team_abv
                solo_tackle_1_team = home_team_abv
            else:
                raise ValueError(
                    f"Unhandled return team in the play:\n{play}"
                )

            penalty_arr = re.findall(
                r"PENALTY ([a-zA-Z0-9\s\(\)\#\.\,\-\']+)",
                play["description"]
            )[0]

            if (
                "yards from" in penalty_arr.lower() and
                "(" in penalty_arr.lower()
            ):
                try:
                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+) " +
                        r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\) " +
                        r"([\-0-9]+) yard[s]? from " +
                        r"([0-9a-zA-Z\-]+)? to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_player_name = play_arr[0][2]

                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+) " +
                        r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\) " +
                        r"([\-0-9]+) yard[s]? " +
                        r"from ([0-9a-zA-Z\-]+)? to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_player_name = play_arr[0][2]
                    penalty_yards = int(play_arr[0][3])
                except Exception:
                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+)\s" +
                        r"([\-0-9]+) yard[s]? from ([0-9a-zA-Z\-]+) " +
                        r"to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_yards = int(play_arr[0][2])
            elif (
                "yards from" in penalty_arr
            ):
                play_arr = re.findall(
                    r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+)\s" +
                    r"([\-0-9]+) yard[s]? from ([0-9a-zA-Z\-]+) " +
                    r"to ([0-9a-zA-Z\-]+)",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = play_arr[0][1]
                penalty_yards = int(play_arr[0][2])
            else:
                play_arr = re.findall(
                    r"([A-Z]{2,4}) ([a-zA-Z\-\s\,0-9]+) " +
                    r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = play_arr[0][1]
                penalty_player_name = play_arr[0][2]
                # penalty_yards = int(play_arr[0][3])
            del penalty_arr

            if "safety" in play["description"].lower():
                is_safety = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
                except Exception:
                    play_arr = re.findall(
                        r" ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
        elif (
            play["type"].lower() == "kickoff"
            and play["subType"].lower() == "single"
        ):
            is_kickoff_attempt = True
            is_special_teams_play = True
            is_kickoff_in_endzone = True
            is_rouge = True
            special_teams_play_type = "kickoff"

            if "return" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) kickoff ([\-0-9]+) yards " +
                    r"to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"return ([\-0-9]+) yard[s]? to the " +
                    r"([0-9a-zA-Z\-]+)(, End Of Play)? SINGLE",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                kickoff_returner_player_name = play_arr[0][4]
                return_yards = play_arr[0][4]
            elif "touchback single" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) kickoff ([\-0-9]+) yards to the ([0-9a-zA-Z\-]+), Touchback SINGLE",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
            elif "out of bounds at" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) kickoff ([\-0-9]+) yards to the ([0-9a-zA-Z\-]+), out of bounds at ([0-9a-zA-Z\-]+) SINGLE",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
            else:
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) kickoff ([\-0-9]+) yards to the ([0-9a-zA-Z\-]+) SINGLE",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
            if "safety" in play["description"].lower():
                is_safety = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
                except Exception:
                    play_arr = re.findall(
                        r" ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]

        # Field Goal
        elif (
            play["type"].lower() == "fieldgoal" and
            play["subType"].lower() == "success"
        ):
            is_field_goal_attempt = True
            is_special_teams_play = True
            special_teams_play_type = "fg"

            if (
                "penalty" in play["description"].lower() and
                "field goal" not in play["description"].lower()
            ):
                penalty_arr = re.findall(
                    r"PENALTY ([a-zA-Z0-9\s\(\)\#\.\,\-\']+)",
                    play["description"]
                )[0]

                if "illegal sub (too many men)" in penalty_arr.lower():
                    play_arr = re.findall(
                        r"([A-Z{2|3}]+) Illegal sub \(too many men\)[ ]? " +
                        r"([\-0-9]+) yards from " +
                        r"([0-9a-zA-Z\-]+)? to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = "Illegal sub (too many men)"
                    penalty_yards = play_arr[0][1]
                elif (
                    "time count after 3min warning on 1st"
                    in penalty_arr.lower()
                ):
                    play_arr = re.findall(
                        r"([A-Z{2|3}]+) Time count after 3min warning " +
                        r"on 1st or 2nd down - Loss of Down " +
                        r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = "Time count after 3min warning on " +\
                        "1st or 2nd down - Loss of Down"
                    penalty_player_name = play_arr[0][1]
                elif (
                    "yards from" in penalty_arr.lower() and
                    "(" in penalty_arr.lower()
                ):
                    try:
                        play_arr = re.findall(
                            r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+) " +
                            r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\) " +
                            r"([\-0-9]+) yard[s]? from " +
                            r"([0-9a-zA-Z\-]+)? to ([0-9a-zA-Z\-]+)",
                            penalty_arr
                        )
                        penalty_team = play_arr[0][0]
                        penalty_type = play_arr[0][1]
                        penalty_player_name = play_arr[0][2]

                        play_arr = re.findall(
                            r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+) " +
                            r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\) " +
                            r"([\-0-9]+) yard[s]? " +
                            r"from ([0-9a-zA-Z\-]+)? to ([0-9a-zA-Z\-]+)",
                            penalty_arr
                        )
                        penalty_team = play_arr[0][0]
                        penalty_type = play_arr[0][1]
                        penalty_player_name = play_arr[0][2]
                        penalty_yards = int(play_arr[0][3])
                    except Exception:
                        play_arr = re.findall(
                            r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+)\s" +
                            r"([\-0-9]+) yard[s]? from ([0-9a-zA-Z\-]+) " +
                            r"to ([0-9a-zA-Z\-]+)",
                            penalty_arr
                        )
                        penalty_team = play_arr[0][0]
                        penalty_type = play_arr[0][1]
                        penalty_yards = int(play_arr[0][2])
                elif (
                    "yards from" in penalty_arr
                ):
                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+)\s" +
                        r"([\-0-9]+) yard[s]? from ([0-9a-zA-Z\-]+) " +
                        r"to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_yards = int(play_arr[0][2])
                elif (
                    "(" not in penalty_arr and "declined" in penalty_arr
                ):
                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([a-zA-Z\-\s\,0-9]+) declined",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    # penalty_player_name = play_arr[0][2]
                    # penalty_yards = int(play_arr[0][3])
                else:
                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([a-zA-Z\-\s\,0-9]+) " +
                        r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_player_name = play_arr[0][2]
                    # penalty_yards = int(play_arr[0][3])
                del penalty_arr
            else:
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) field goal attempt from " +
                    r"([\-0-9]+) yard[s]? ([a-zA-Z\s]+) " +
                    r"\(H: [\#0-9]+ ([a-zA-Z\.\s\-\']+), " +
                    r"LS: [\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                field_goal_result = play_arr[0][2]

            if "safety" in play["description"].lower():
                is_safety = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
                except Exception:
                    play_arr = re.findall(
                        r" ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
        elif (
            play["type"].lower() == "fieldgoal" and
            play["subType"].lower() == "failed"
        ):
            is_field_goal_attempt = True
            is_special_teams_play = True
            special_teams_play_type = "fg"

            if (
                "blocked by" in play["description"].lower() and
                "touchdown" in play["description"].lower()
            ):
                is_return_touchdown = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) field goal attempt from " +
                    r"([\-0-9]+) yard[s]? NO GOOD blocked by " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"\(H: [\#0-9]+ ([a-zA-Z\.\s\-\']+), LS: " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+)\), clock ([\:0-9]+) " +
                    r"recovered by ([A-Z{2|3}]+) [\#0-9]+ " +
                    r"([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) " +
                    r"yard[s]? to the ([0-9a-zA-Z\-]+) TOUCHDOWN",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                field_goal_result = "blocked"
                blocked_player_name = play_arr[0][2]
                missed_fg_return_team = play_arr[0][6]
                missed_fg_return_player_name = play_arr[0][7]
                missed_fg_return_yards = int(play_arr[0][10])
                td_team = missed_fg_return_team
                td_player_name = missed_fg_return_player_name

            elif (
                "blocked by" in play["description"].lower() and
                "out of bounds at" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) field goal attempt from ([\-0-9]+) yard[s]? NO GOOD blocked by [\#0-9]+ ([a-zA-Z\.\s\-\']+) \(H: [\#0-9]+ ([a-zA-Z\.\s\-\']+), LS: [\#0-9]+ ([a-zA-Z\.\s\-\']+)\), clock ([\:0-9]+) recovered by ([A-Z{2|3}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+), out of bounds at ([0-9a-zA-Z\-]+)",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                field_goal_result = "blocked"
                blocked_player_name = play_arr[0][2]
                missed_fg_return_team = play_arr[0][6]
                missed_fg_return_player_name = play_arr[0][7]
                missed_fg_return_yards = 0
            elif (
                "blocked by" in play["description"].lower() and
                "return" not in play["description"].lower() and
                "end of play single" in play["description"].lower()
            ):
                is_rouge = True
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) field goal attempt from ([\-0-9]+) yard[s]? NO GOOD blocked by [\#0-9]+ ([a-zA-Z\.\s\-\']+) \(H: [\#0-9]+ ([a-zA-Z\.\s\-\']+), LS: [\#0-9]+ ([a-zA-Z\.\s\-\']+)\), clock ([\:0-9]+) recovered by ([A-Z{2|3}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+), End Of Play SINGLE, clock ([\:0-9]+)",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                field_goal_result = "blocked"
                blocked_player_name = play_arr[0][2]
                missed_fg_return_team = play_arr[0][6]
                missed_fg_return_player_name = play_arr[0][7]
                missed_fg_return_yards = 0
            elif (
                "blocked by" in play["description"].lower() and
                "return" not in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) field goal attempt from ([\-0-9]+) yard[s]? NO GOOD blocked by [\#0-9]+ ([a-zA-Z\.\s\-\']+) \(H: [\#0-9]+ ([a-zA-Z\.\s\-\']+), LS: [\#0-9]+ ([a-zA-Z\.\s\-\']+)\), clock ([\:0-9]+) recovered by ([A-Z{2|3}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                field_goal_result = "blocked"
                blocked_player_name = play_arr[0][2]
                missed_fg_return_team = play_arr[0][6]
                missed_fg_return_player_name = play_arr[0][7]
                missed_fg_return_yards = 0

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][9]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_1_team = defteam
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "blocked by" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) field goal attempt from " +
                    r"([\-0-9]+) yard[s]? NO GOOD blocked by " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"\(H: [\#0-9]+ ([a-zA-Z\.\s\-\']+), LS: " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+)\), clock ([\:0-9]+) " +
                    r"recovered by ([A-Z{2|3}]+) [\#0-9]+ " +
                    r"([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) " +
                    r"yard[s]? to the ([0-9a-zA-Z\-]+) "
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                field_goal_result = "blocked"
                blocked_player_name = play_arr[0][2]
                missed_fg_return_team = play_arr[0][6]
                missed_fg_return_player_name = play_arr[0][7]
                missed_fg_return_yards = int(play_arr[0][10])

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][12]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_1_team = defteam
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            else:
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) field goal attempt from " +
                    r"([\-0-9]+) yard[s]? ([a-zA-Z\s]+) " +
                    r"\(H: [\#0-9]+ ([a-zA-Z\.\s\-\']+), " +
                    r"LS: [\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                kick_distance = int(play_arr[0][1])
                field_goal_result = play_arr[0][2]
            if "safety" in play["description"].lower():
                is_safety = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
                except Exception:
                    play_arr = re.findall(
                        r" ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]

            if "single" in play["description"]:
                is_rouge = True

        # XP
        elif (
            play["type"].lower() == "onepoint" and
            play["subType"].lower() == "success"
        ):
            special_teams_play_type = "xp"
            is_special_teams_play = True
            is_extra_point_attempt = True

            if (
                "defensive pat successful" in play["description"].lower() and
                "blocked" in play["description"].lower()
            ):
                is_defensive_extra_point_attempt = True
                is_defensive_extra_point_conv = True

                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) kick attempt ([a-zA-Z]+) \( ? blocked by [\#0-9]+ ([a-zA-Z\.\s\-\']+)\) \(H: [\#0-9]+ ([a-zA-Z\.\s\-\']+), LS: [\#0-9]+ ([a-zA-Z\.\s\-\']+)\) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) defensive PAT Successful",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                extra_point_result = play_arr[0][1]
                blocked_player_name = play_arr[0][2]
                missed_fg_return_team = play_arr[0][5]
                missed_fg_return_player_name = play_arr[0][7]
                missed_fg_return_yards = int(play_arr[0][9])
            elif "defensive pat successful" in play["description"].lower():
                is_defensive_extra_point_attempt = True
                is_defensive_extra_point_conv = True

                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) kick attempt ([a-zA-Z]+) \(H: [\#0-9]+ ([a-zA-Z\.\s\-\']+), LS: [\#0-9]+ ([a-zA-Z\.\s\-\']+)\) recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) defensive PAT Successful",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                extra_point_result = play_arr[0][1]
                # blocked_player_name = play_arr[0][2]
                missed_fg_return_team = play_arr[0][4]
                missed_fg_return_player_name = play_arr[0][6]
                missed_fg_return_yards = int(play_arr[0][8])
            else:
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) kick attempt ([a-zA-Z]+) " +
                    r"\(H: [\#0-9]+ ([a-zA-Z\.\s\-\']+), " +
                    r"LS: [\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                extra_point_result = play_arr[0][1]
            if "safety" in play["description"].lower():
                is_safety = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
                except Exception:
                    play_arr = re.findall(
                        r" ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
        elif (
            play["type"].lower() == "onepoint" and
            play["subType"].lower() == "failed"
        ):
            is_special_teams_play = True
            is_extra_point_attempt = True
            special_teams_play_type = "xp"
            if (
                "return" in play["description"].lower() and
                "blocked by" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) kick attempt ([a-zA-Z]+) \( ?blocked by [\#0-9]+ ([a-zA-Z\.\s\-\']+)\) \(H: [\#0-9]+ ([a-zA-Z\.\s\-\']+), LS: [\#0-9]+ ([a-zA-Z\.\s\-\']+)\) recovered by ([A-Z{2|3}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                extra_point_result = play_arr[0][1]
                blocked_player_name = play_arr[0][2]

                missed_fg_return_team = play_arr[0][5]
                missed_fg_return_player_name = play_arr[0][6]
                missed_fg_return_yards = int(play_arr[0][9])
                return_yards = missed_fg_return_yards

                if missed_fg_return_team == posteam:
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                    solo_tackle_1_team = defteam
                elif missed_fg_return_team == defteam:
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam
                    solo_tackle_1_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][11]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif (
                "return" in play["description"].lower() and
                " end of play" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) kick attempt ([a-zA-Z]+) \(H: [\#0-9]+ ([a-zA-Z\.\s\-\']+), LS: [\#0-9]+ ([a-zA-Z\.\s\-\']+)\) recovered by ([A-Z{2|3}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+)\, [END|end]+ [OF|of]+ [PLAY|play]+",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                extra_point_result = play_arr[0][1]

                missed_fg_return_team = play_arr[0][4]
                missed_fg_return_player_name = play_arr[0][5]
                missed_fg_return_yards = int(play_arr[0][8])
                return_yards = missed_fg_return_yards
            elif "return" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) kick attempt ([a-zA-Z]+) " +
                    r"\(H: [\#0-9]+ ([a-zA-Z\.\s\-\']+), " +
                    r"LS: [\#0-9]+ ([a-zA-Z\.\s\-\']+)\) " +
                    r"recovered by ([A-Z{2|3}]+) [\#0-9]+ " +
                    r"([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) " +
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) " +
                    r"yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"\(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                extra_point_result = play_arr[0][1]

                missed_fg_return_team = play_arr[0][4]
                missed_fg_return_player_name = play_arr[0][5]
                missed_fg_return_yards = int(play_arr[0][8])
                return_yards = missed_fg_return_yards

                if missed_fg_return_team == posteam:
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                    solo_tackle_1_team = defteam
                elif missed_fg_return_team == defteam:
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam
                    solo_tackle_1_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][10]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif "fumbled snap" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) Fumbled Snap at ([0-9a-zA-Z\-]+) for loss of ([\-0-9]+) yard[s]? recovered by ([A-Z{2,4}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) advances ([\-0-9]+) yards to ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\), Attempt Failed",
                    play["description"]
                )
                fumbled_1_team = posteam
                fumbled_1_player_name = play_arr[0][0]
                fumble_recovery_1_yards = int(play_arr[0][2]) * -1
                fumble_recovery_1_team = play_arr[0][3]
                fumble_recovery_1_player_name = play_arr[0][4]
                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][8]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_team = defteam
                    solo_tackle_1_player_name = tak_arr[0][0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
                extra_point_result = "failed"
            else:
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) kick attempt ([a-zA-Z]+) " +
                    r"\(H: [\#0-9]+ ([a-zA-Z\.\s\-\']+), " +
                    r"LS: [\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                extra_point_result = play_arr[0][1]
            if "safety" in play["description"].lower():
                is_safety = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
                except Exception:
                    play_arr = re.findall(
                        r" ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
        elif (
            play["type"].lower() == "onepoint" and
            play["subType"].lower() == "penalty"
        ):
            is_special_teams_play = True
            is_extra_point_attempt = True
            special_teams_play_type = "xp"

            if "kick attempt good" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) kick attempt good " +
                    r"\(H: [\#0-9]+ ([a-zA-Z\.\s\-\']+), " +
                    r"LS: [\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                extra_point_result = "good"
            elif (
                "return" in play["description"].lower() and
                "blocked by" in play["description"].lower()
            ):
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) kick attempt ([a-zA-Z]+) \( ?blocked by [\#0-9]+ ([a-zA-Z\.\s\-\']+)\) \(H: [\#0-9]+ ([a-zA-Z\.\s\-\']+), LS: [\#0-9]+ ([a-zA-Z\.\s\-\']+)\) recovered by ([A-Z{2|3}]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at ([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return ([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) \(([a-zA-Z0-9\#\.\-\s\'\;]+)\)",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                extra_point_result = play_arr[0][1]
                blocked_player_name = play_arr[0][2]

                missed_fg_return_team = play_arr[0][5]
                missed_fg_return_player_name = play_arr[0][6]
                missed_fg_return_yards = int(play_arr[0][9])
                return_yards = missed_fg_return_yards

                if missed_fg_return_team == posteam:
                    assist_tackle_1_team = defteam
                    assist_tackle_2_team = defteam
                    solo_tackle_1_team = defteam
                elif missed_fg_return_team == defteam:
                    assist_tackle_1_team = posteam
                    assist_tackle_2_team = posteam
                    solo_tackle_1_team = posteam

                tak_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+)",
                    play_arr[0][11]
                )
                if len(tak_arr) == 2:
                    is_assist_tackle = True
                    assist_tackle_1_player_name = tak_arr[0][0]
                    assist_tackle_2_player_name = tak_arr[1][0]
                elif len(tak_arr) == 1:
                    solo_tackle_1_player_name = tak_arr[0]
                else:
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif "recovered by" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) kick attempt ([a-zA-Z]+) " +
                    r"\(H: [\#0-9]+ ([a-zA-Z\.\s\-\']+), " +
                    r"LS: [\#0-9]+ ([a-zA-Z\.\s\-\']+)\) recovered by " +
                    r"([A-Z]{2,4}) [\#0-9]+ ([a-zA-Z\.\s\-\']+) at " +
                    r"([0-9a-zA-Z\-]+) [\#0-9]+ ([a-zA-Z\.\s\-\']+) return " +
                    r"([\-0-9]+) yard[s]? to the ([0-9a-zA-Z\-]+) " +
                    r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                extra_point_result = play_arr[0][1]
                missed_fg_return_team = play_arr[0][4]
                missed_fg_return_player_name = play_arr[0][6]
                missed_fg_return_yards = int(play_arr[0][8])
                return_yards = missed_fg_return_yards

                if "touchdown" in play["description"].lower():
                    raise ValueError(
                        f"Unhandled play {play}"
                    )
            elif "kick attempt failed" in play["description"].lower():
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) kick attempt failed " +
                    r"\(H: [\#0-9]+ ([a-zA-Z\.\s\-\']+), " +
                    r"LS: [\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                    play["description"]
                )
                kicker_player_name = play_arr[0][0]
                extra_point_result = "failed"
            elif (
                "kick" not in play["description"].lower()
            ):
                # If there's no kick to parse,
                # go to the next part of this play.
                pass
            else:
                raise ValueError(
                    f"Unhandled play {play}"
                )

            penalty_arr = re.findall(
                r"PENALTY ([a-zA-Z0-9\s\(\)\#\.\,\-\']+)",
                play["description"]
            )[0]

            if (
                "yards from" in penalty_arr.lower() and
                "(" in penalty_arr.lower()
            ):
                try:
                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+) " +
                        r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\) " +
                        r"([\-0-9]+) yard[s]? from " +
                        r"([0-9a-zA-Z\-]+)? to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_player_name = play_arr[0][2]

                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+) " +
                        r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\) " +
                        r"([\-0-9]+) yard[s]? " +
                        r"from ([0-9a-zA-Z\-]+)? to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_player_name = play_arr[0][2]
                    penalty_yards = int(play_arr[0][3])
                except Exception:
                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+)\s" +
                        r"([\-0-9]+) yard[s]? from ([0-9a-zA-Z\-]+) " +
                        r"to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_yards = int(play_arr[0][2])
            elif (
                "yards from" in penalty_arr.lower()
            ):
                play_arr = re.findall(
                    r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+)\s" +
                    r"([\-0-9]+) yard[s]? from ?([0-9a-zA-Z\-]+)? " +
                    r"to ([0-9a-zA-Z\-]+)",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = play_arr[0][1]
                penalty_yards = int(play_arr[0][2])
            elif "(" not in penalty_arr.lower():
                play_arr = re.findall(
                    r"([A-Z]{2,4}) ([a-zA-Z\-\s\,0-9]+)",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = play_arr[0][1]
            else:
                play_arr = re.findall(
                    r"([A-Z]{2,4}) ([a-zA-Z\-\s\,0-9]+) " +
                    r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = play_arr[0][1]
                penalty_player_name = play_arr[0][2]
                # penalty_yards = int(play_arr[0][3])
            del penalty_arr
            if "safety" in play["description"].lower():
                is_safety = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
                except Exception:
                    play_arr = re.findall(
                        r" ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
        elif (
            play["type"].lower() == "twopoints" and
            play["subType"].lower() == "success"
        ):
            is_two_point_attempt = True
            two_point_conv_result = "success"

            if "rush attempt" in play["description"]:
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"rush attempt [Successful|successful]+",
                    play["description"]
                )
                rusher_player_name = play_arr[0]
            elif "pass attempt" in play["description"]:
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"pass attempt [Successful|successful]+",
                    play["description"]
                )
                passer_player_name = play_arr[0]
            else:
                raise ValueError(
                    f"Unhandled 2PC play {play}"
                )
            if "safety" in play["description"].lower():
                is_safety = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
                except Exception:
                    play_arr = re.findall(
                        r" ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
        elif (
            play["type"].lower() == "twopoints" and
            play["subType"].lower() == "failed"
        ):
            is_two_point_attempt = True
            two_point_conv_result = "failed"

            if "safety" in play["description"].lower():
                raise NotImplementedError(
                    "TODO: Implement safety logic for the following play:" +
                    f"\n{play}"
                )

            if "rush attempt" in play["description"]:
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"rush attempt [Failed|failed]+",
                    play["description"]
                )
                rusher_player_name = play_arr[0]
            elif "pass attempt" in play["description"]:
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) " +
                    r"pass attempt [Failed|failed]+",
                    play["description"]
                )
                passer_player_name = play_arr[0]
            else:
                raise ValueError(
                    f"Unhandled 2PC play {play}"
                )
            if "safety" in play["description"].lower():
                is_safety = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
                except Exception:
                    play_arr = re.findall(
                        r" ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
        elif (
            play["type"].lower() == "twopoints" and
            play["subType"].lower() == "penalty"
        ):
            is_two_point_attempt = True

            if "pass attempt failed" in play["description"].lower():
                two_point_conv_result = "failure"
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) pass attempt failed",
                    play["description"]
                )
                passer_player_name = play_arr[0]
            elif "pass attempt successful" in play["description"].lower():
                two_point_conv_result = "success"
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) pass attempt Successful",
                    play["description"]
                )
                passer_player_name = play_arr[0]
            elif "rush attempt failed" in play["description"].lower():
                two_point_conv_result = "failure"
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush attempt failed",
                    play["description"]
                )
                rusher_player_name = play_arr[0]
            elif "rush attempt successful" in play["description"].lower():
                two_point_conv_result = "success"
                play_arr = re.findall(
                    r"[\#0-9]+ ([a-zA-Z\.\s\-\']+) rush attempt Successful",
                    play["description"]
                )
                rusher_player_name = play_arr[0]
            elif (
                "pass" not in play["description"].lower() and
                "rush" not in play["description"].lower()
            ):
                # No pass or run play specified? No reason to parse this.
                pass
            else:
                raise ValueError(
                    f"Unhandled 2PC play {play}"
                )

            penalty_arr = re.findall(
                r"PENALTY ([a-zA-Z0-9\s\(\)\#\.\,\-\']+)",
                play["description"]
            )[0]

            if (
                "yards from" in penalty_arr.lower() and
                "(" in penalty_arr.lower()
            ):
                try:
                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+) " +
                        r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\) " +
                        r"([\-0-9]+) yard[s]? from " +
                        r"([0-9a-zA-Z\-]+)? to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_player_name = play_arr[0][2]

                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+) " +
                        r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\) " +
                        r"([\-0-9]+) yard[s]? " +
                        r"from ([0-9a-zA-Z\-]+)? to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_player_name = play_arr[0][2]
                    penalty_yards = int(play_arr[0][3])
                except Exception:
                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+)\s" +
                        r"([\-0-9]+) yard[s]? from ([0-9a-zA-Z\-]+) " +
                        r"to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_yards = int(play_arr[0][2])

            elif (
                "yards from" in penalty_arr
            ):
                play_arr = re.findall(
                    r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+)\s" +
                    r"([\-0-9]+) yard[s]? from ([0-9a-zA-Z\-]+) " +
                    r"to ([0-9a-zA-Z\-]+)",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = play_arr[0][1]
                penalty_yards = int(play_arr[0][2])
            else:
                play_arr = re.findall(
                    r"([A-Z]{2,4}) ([a-zA-Z\-\s\,0-9]+) " +
                    r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = play_arr[0][1]
                penalty_player_name = play_arr[0][2]
                # penalty_yards = int(play_arr[0][3])
            del penalty_arr
            if "safety" in play["description"].lower():
                is_safety = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
                except Exception:
                    play_arr = re.findall(
                        r" ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]

        # Penalty
        elif (
            play["type"].lower() == "penalty" and
            play["subType"].lower() == "penalty"
        ):
            penalty_arr = re.findall(
                r"PENALTY ([a-zA-Z0-9\s\(\)\#\.\,\-\']+)",
                play["description"]
            )[0]

            if "illegal sub (too many men)" in penalty_arr.lower():
                play_arr = re.findall(
                    r"([A-Z{2|3}]+) Illegal sub \(too many men\)[ ]? " +
                    r"([\-0-9]+) yards from " +
                    r"([0-9a-zA-Z\-]+)? to ([0-9a-zA-Z\-]+)",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = "Illegal sub (too many men)"
                penalty_yards = play_arr[0][1]
            elif (
                "time count after 3min warning on 1st"
                in penalty_arr.lower() and
                "loss 10 yards" in penalty_arr.lower()
            ):
                play_arr = re.findall(
                    r"([A-Z{2|3}]+) Time count after 3min warning " +
                    r"on 1st or 2nd down - Loss 10 yards " +
                    r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = "Time count after 3min warning on " +\
                    "1st or 2nd down - Loss of Down"
                penalty_player_name = play_arr[0][1]
                penalty_yards = 10
            elif (
                "time count after 3min warning on 1st"
                in penalty_arr.lower()
            ):
                play_arr = re.findall(
                    r"([A-Z{2|3}]+) Time count after 3min warning " +
                    r"on 1st or 2nd down - Loss of Down " +
                    r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = "Time count after 3min warning on " +\
                    "1st or 2nd down - Loss of Down"
                penalty_player_name = play_arr[0][1]
            elif (
                "yards from" in penalty_arr.lower() and
                "(" in penalty_arr.lower()
            ):
                try:
                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+) " +
                        r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\) " +
                        r"([\-0-9]+) yard[s]? from " +
                        r"([0-9a-zA-Z\-]+)? to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_player_name = play_arr[0][2]

                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+) " +
                        r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\) " +
                        r"([\-0-9]+) yard[s]? " +
                        r"from ([0-9a-zA-Z\-]+)? to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_player_name = play_arr[0][2]
                    penalty_yards = int(play_arr[0][3])
                except Exception:
                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+)\s" +
                        r"([\-0-9]+) yard[s]? from ([0-9a-zA-Z\-]+) " +
                        r"to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_yards = int(play_arr[0][2])
            elif (
                "yards from" in penalty_arr
            ):
                play_arr = re.findall(
                    r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+)\s" +
                    r"([\-0-9]+) yard[s]? from ([0-9a-zA-Z\-]+) " +
                    r"to ([0-9a-zA-Z\-]+)",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = play_arr[0][1]
                penalty_yards = int(play_arr[0][2])
            elif (
                "(" not in penalty_arr and "declined" in penalty_arr
            ):
                play_arr = re.findall(
                    r"([A-Z]{2,4}) ([a-zA-Z\-\s\,0-9]+) declined",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = play_arr[0][1]
                # penalty_player_name = play_arr[0][2]
                # penalty_yards = int(play_arr[0][3])
            elif " , 1st down" in penalty_arr.lower():
                play_arr = re.findall(
                    r"([A-Z]{2,4}) ([a-zA-Z\-\s\,0-9]+) , 1ST DOWN",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = play_arr[0][1]
            else:
                play_arr = re.findall(
                    r"([A-Z]{2,4}) ([a-zA-Z\-\s\,0-9]+) " +
                    r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = play_arr[0][1]
                penalty_player_name = play_arr[0][2]
                # penalty_yards = int(play_arr[0][3])
            del penalty_arr

            if "safety" in play["description"].lower():
                is_safety = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
                except Exception:
                    play_arr = re.findall(
                        r" ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
        elif (
            play["type"].lower() == "kneel" and
            play["subType"].lower() == "penalty"
        ):
            penalty_arr = re.findall(
                r"PENALTY ([a-zA-Z0-9\s\(\)\#\.\,\-\']+)",
                play["description"]
            )[0]

            if "illegal sub (too many men)" in penalty_arr.lower():
                play_arr = re.findall(
                    r"([A-Z{2|3}]+) Illegal sub \(too many men\)[ ]? " +
                    r"([\-0-9]+) yards from " +
                    r"([0-9a-zA-Z\-]+)? to ([0-9a-zA-Z\-]+)",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = "Illegal sub (too many men)"
                penalty_yards = play_arr[0][1]
            elif (
                "time count after 3min warning on 1st"
                in penalty_arr.lower() and
                "loss 10 yards" in penalty_arr.lower()
            ):
                play_arr = re.findall(
                    r"([A-Z{2|3}]+) Time count after 3min warning " +
                    r"on 1st or 2nd down - Loss 10 yards " +
                    r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = "Time count after 3min warning on " +\
                    "1st or 2nd down - Loss of Down"
                penalty_player_name = play_arr[0][1]
                penalty_yards = 10
            elif (
                "time count after 3min warning on 1st"
                in penalty_arr.lower()
            ):
                play_arr = re.findall(
                    r"([A-Z{2|3}]+) Time count after 3min warning " +
                    r"on 1st or 2nd down - Loss of Down " +
                    r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = "Time count after 3min warning on " +\
                    "1st or 2nd down - Loss of Down"
                penalty_player_name = play_arr[0][1]
            elif (
                "yards from" in penalty_arr.lower() and
                "(" in penalty_arr.lower()
            ):
                try:
                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+) " +
                        r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\) " +
                        r"([\-0-9]+) yard[s]? from " +
                        r"([0-9a-zA-Z\-]+)? to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_player_name = play_arr[0][2]

                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+) " +
                        r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\) " +
                        r"([\-0-9]+) yard[s]? " +
                        r"from ([0-9a-zA-Z\-]+)? to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_player_name = play_arr[0][2]
                    penalty_yards = int(play_arr[0][3])
                except Exception:
                    play_arr = re.findall(
                        r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+)\s" +
                        r"([\-0-9]+) yard[s]? from ([0-9a-zA-Z\-]+) " +
                        r"to ([0-9a-zA-Z\-]+)",
                        penalty_arr
                    )
                    penalty_team = play_arr[0][0]
                    penalty_type = play_arr[0][1]
                    penalty_yards = int(play_arr[0][2])
            elif (
                "yards from" in penalty_arr
            ):
                play_arr = re.findall(
                    r"([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+)\s" +
                    r"([\-0-9]+) yard[s]? from ([0-9a-zA-Z\-]+) " +
                    r"to ([0-9a-zA-Z\-]+)",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = play_arr[0][1]
                penalty_yards = int(play_arr[0][2])
            elif (
                "(" not in penalty_arr and "declined" in penalty_arr
            ):
                play_arr = re.findall(
                    r"([A-Z]{2,4}) ([a-zA-Z\-\s\,0-9]+) declined",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = play_arr[0][1]
                # penalty_player_name = play_arr[0][2]
                # penalty_yards = int(play_arr[0][3])
            elif " , 1st down" in penalty_arr.lower():
                play_arr = re.findall(
                    r"([A-Z]{2,4}) ([a-zA-Z\-\s\,0-9]+) , 1ST DOWN",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = play_arr[0][1]
            else:
                play_arr = re.findall(
                    r"([A-Z]{2,4}) ([a-zA-Z\-\s\,0-9]+) " +
                    r"\([\#0-9]+ ([a-zA-Z\.\s\-\']+)\)",
                    penalty_arr
                )
                penalty_team = play_arr[0][0]
                penalty_type = play_arr[0][1]
                penalty_player_name = play_arr[0][2]
                # penalty_yards = int(play_arr[0][3])
            del penalty_arr

            if "safety" in play["description"].lower():
                is_safety = True
                try:
                    play_arr = re.findall(
                        r"[\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]
                except Exception:
                    play_arr = re.findall(
                        r" ([a-zA-Z\.\-\s\']+) SAFETY TOUCH",
                        play["description"]
                    )
                    safety_player_name = play_arr[0]

        else:
            play_type = play["type"]
            play_sub_type = play["subType"]
            raise ValueError(
                f"Unhandled play type: {play_type} {play_sub_type}"
            )

        if (
            down == 1 and
            ((yards_gained/yds_to_go) > 0.4) and
            is_no_play == False
        ):
            is_successful_play = True
        elif (
            down == 2 and
            ((yards_gained/yds_to_go) > 0.6) and
            is_no_play == False
        ):
            is_successful_play = True
        elif (
            down == 3 and
            ((yards_gained/yds_to_go) > 1.0) and
            is_no_play == False
        ):
            is_third_down_converted = True
            is_successful_play = True
        elif down == 3:
            is_third_down_failed = True
        # Following is done so I don't have to retroactively fix this potential bug.
        elif (
            down == 4 and
            ((yards_gained/yds_to_go) > 1.0) and
            is_no_play == False
        ):
            is_successful_play = True
            is_fourth_down_converted = True
        elif down == 4:
            is_fourth_down_failed = True
        # if yds_net

        if punt_end_yl is not None and punt_end_yl < 20:
            is_punt_inside_twenty = True

        if kickoff_end_yl is not None and kickoff_end_yl < 20:
            is_kickoff_inside_twenty = True
        receiving_yards = passing_yards

        if td_team == posteam and is_no_play is False:
            posteam_score_post += 6
        elif td_team == defteam and is_no_play is False:
            defteam_score_post += 6

        if field_goal_result.lower() == "good" and is_no_play is False:
            posteam_score_post += 3

        if two_point_conv_result.lower() == "good" and is_no_play is False:
            posteam_score_post += 2

        if is_defensive_extra_point_conv is True and is_no_play is False:
            defteam_score_post += 2

        if extra_point_result.lower() == "good" and is_no_play is False:
            posteam_score_post += 1

        if is_rouge is True and is_no_play is False:
            posteam_score_post += 1

        score_differential_post = posteam_score_post - defteam_score_post

        if play["teamId"] == home_team_id:
            total_home_score = posteam_score_post
            total_away_score = defteam_score_post
        elif play["teamId"] == away_team_id:
            total_home_score = defteam_score_post
            total_away_score = posteam_score_post

        # if posteam == home_team_abv:
        #     total_home_score = posteam_score_post
        #     total_away_score = defteam_score_post
        # elif posteam == away_team_abv:
        #     total_home_score = defteam_score_post
        #     total_away_score = posteam_score_post

        temp_df = pd.DataFrame(
            {
                "play_id": play_id,
                "game_id": None,
                "home_team": home_team_abv,
                "away_team": away_team_abv,
                "season_type": None,
                "week": None,
                "posteam": posteam,
                "posteam_type": posteam_type,
                "defteam": defteam,
                "side_of_field": side_of_field,
                "yardline_100": yardline_100,
                "game_date": play_date,
                "quarter_seconds_remaining": quarter_seconds_remaining,
                "half_seconds_remaining": half_seconds_remaining,
                "game_seconds_remaining": game_seconds_remaining,
                "game_half": game_half,
                "quarter_end": None,
                "drive": drive_num,
                "sp": play["isScoring"],  # scoring play
                "qtr": int(play["phaseQualifier"]),
                "down": down,
                "goal_to_go": is_goal_to_go,
                "time": play["clock"],
                "yrdln": yrdln,
                "yds_to_go": yds_to_go,
                "yds_net": yds_net,
                "desc": play["description"],
                "play_type": play["type"],
                "yards_gained": yards_gained,
                "shotgun": is_shotgun,
                "no_huddle": is_no_huddle,
                "qb_dropback": is_qb_dropback,
                "qb_kneel": is_qb_kneel,
                "qb_spike": is_qb_spike,
                "qb_scramble": None,
                "pass_length": pass_length,
                "pass_location": pass_location,
                "air_yards": air_yards,
                "yards_after_catch": yards_after_catch,
                "run_location": run_location,
                "run_gap": None,
                "field_goal_result": field_goal_result,
                "kick_distance": kick_distance,
                "extra_point_result": extra_point_result,
                "two_point_conv_result": two_point_conv_result,
                "home_timeouts_remaining": home_timeouts_remaining,
                "away_timeouts_remaining": away_timeouts_remaining,
                "timeout": None,
                "timeout_team": None,
                "td_team": td_team,
                "td_player_name": td_player_name,
                "td_player_id": None,
                "posteam_timeouts_remaining": posteam_timeouts_remaining,
                "defteam_timeouts_remaining": defteam_timeouts_remaining,
                "total_home_score": total_home_score,
                "total_away_score": total_away_score,
                "posteam_score": posteam_score,
                "defteam_score": defteam_score,
                "score_differential": score_differential,
                "posteam_score_post": posteam_score_post,
                "defteam_score_post": defteam_score_post,
                "score_differential_post": score_differential_post,
                "punt_blocked": is_punt_blocked,
                "first_down_rush": is_first_down_rush,
                "first_down_pass": is_first_down_pass,
                "first_down_penalty": is_first_down_penalty,
                "second_down_converted": is_second_down_converted,
                "second_down_failed": is_second_down_failed,
                "third_down_converted": is_third_down_converted,
                "third_down_failed": is_third_down_failed,
                "fourth_down_converted": is_fourth_down_converted,
                "fourth_down_failed": is_fourth_down_failed,
                "incomplete_pass": is_incomplete_pass,
                "is_no_play": is_no_play,
                "touchback": is_touchback,
                "interception": is_interception,
                "punt_inside_twenty": is_punt_inside_twenty,
                "punt_in_endzone": is_punt_in_endzone,
                "punt_out_of_bounds": is_punt_out_of_bounds,
                "punt_downed": is_punt_downed,
                "punt_fair_catch": False,
                "kickoff_inside_twenty": is_kickoff_inside_twenty,
                "kickoff_in_endzone": is_kickoff_in_endzone,
                "kickoff_out_of_bounds": is_kickoff_out_of_bounds,
                "kickoff_downed": is_kickoff_downed,
                "kickoff_fair_catch": is_kickoff_fair_catch,
                "fumble_forced": is_fumble_forced,
                "fumble_not_forced": is_fumble_not_forced,
                "fumble_out_of_bounds": is_fumble_out_of_bounds,
                "solo_tackle": True,
                "safety": is_safety,
                "penalty": is_penalty,
                "tackled_for_loss": None,
                "fumble_lost": is_fumble_lost,
                "own_kickoff_recovery": None,
                "own_kickoff_recovery_td": None,
                "qb_hit": None,
                "rush_attempt": is_rush,
                "pass_attempt": is_pass,
                "is_rouge": is_rouge,
                "sack": is_sack,
                "touchdown": is_touchdown,
                "pass_touchdown": is_pass_touchdown,
                "rush_touchdown": is_rush_touchdown,
                "return_touchdown": is_return_touchdown,
                "extra_point_attempt": is_extra_point_attempt,
                "two_point_attempt": is_two_point_attempt,
                "field_goal_attempt": is_field_goal_attempt,
                "kickoff_attempt": is_kickoff_attempt,
                "punt_attempt": is_punt,
                "fumble": is_fumble,
                "complete_pass": is_complete_pass,
                "assist_tackle": is_assist_tackle,
                "lateral_reception": None,
                "lateral_rush": None,
                "lateral_return": is_lateral_return,
                "lateral_recovery": is_lateral_recovery,
                "passer_player_id": None,
                "passer_player_name": passer_player_name,
                "passing_yards": passing_yards,
                "receiver_player_id": None,
                "receiver_player_name": receiver_player_name,
                "receiving_yards": receiving_yards,
                "rusher_player_id": None,
                "rusher_player_name": rusher_player_name,
                "rushing_yards": rushing_yards,
                "lateral_receiver_player_id": None,
                "lateral_receiver_player_name": None,
                "lateral_receiving_yards": None,
                "lateral_rusher_player_id": None,
                "lateral_rusher_player_name": lateral_rusher_player_name,
                "lateral_rushing_yards": lateral_rusher_yards,
                "lateral_return_yards": lateral_return_yards,
                "lateral_sack_player_id": None,
                "lateral_sack_player_name": None,
                "interception_player_id": None,
                "interception_player_name": interception_player_name,
                "lateral_interception_player_id": None,
                "lateral_interception_player_name": lateral_interception_player_name,
                "punt_returner_player_id": None,
                "punt_returner_player_name": punt_returner_player_name,
                "lateral_punt_returner_player_id": None,
                "lateral_punt_returner_player_name":
                lateral_punt_returner_player_name,
                "kickoff_returner_player_name": kickoff_returner_player_name,
                "kickoff_returner_player_id": None,
                "lateral_kickoff_returner_player_id": None,
                "lateral_kickoff_returner_player_name": None,
                "punter_player_id": None,
                "punter_player_name": punter_player_name,
                "kicker_player_id": None,
                "kicker_player_name": kicker_player_name,
                "own_kickoff_recovery_player_id": None,
                "own_kickoff_recovery_player_name": None,
                "blocked_player_id": None,
                "blocked_player_name": blocked_player_name,
                "tackle_for_loss_1_player_id": None,
                "tackle_for_loss_1_player_name": tackle_for_loss_1_player_name,
                "tackle_for_loss_2_player_id": None,
                "tackle_for_loss_2_player_name": tackle_for_loss_2_player_name,
                "qb_hit_1_player_id": None,
                "qb_hit_1_player_name": None,
                "qb_hit_2_player_id": None,
                "qb_hit_2_player_name": None,
                "forced_fumble_player_1_team": forced_fumble_player_1_team,
                "forced_fumble_player_1_player_id": None,
                "forced_fumble_player_1_player_name":
                forced_fumble_player_1_player_name,
                "forced_fumble_player_2_team": None,
                "forced_fumble_player_2_player_id": None,
                "forced_fumble_player_2_player_name": None,
                "solo_tackle_1_team": solo_tackle_1_team,
                "solo_tackle_2_team": solo_tackle_2_team,
                "solo_tackle_1_player_id": None,
                "solo_tackle_2_player_id": None,
                "solo_tackle_1_player_name": solo_tackle_1_player_name,
                "solo_tackle_2_player_name": solo_tackle_2_player_name,
                "assist_tackle_1_player_id": None,
                "assist_tackle_1_player_name": assist_tackle_1_player_name,
                "assist_tackle_1_team": assist_tackle_1_team,
                "assist_tackle_2_player_id": None,
                "assist_tackle_2_player_name": assist_tackle_2_player_name,
                "assist_tackle_2_team": assist_tackle_2_team,
                "assist_tackle_3_player_id": None,
                "assist_tackle_3_player_name": None,
                "assist_tackle_3_team": None,
                "assist_tackle_4_player_id": None,
                "assist_tackle_4_player_name": None,
                "assist_tackle_4_team": None,
                "tackle_with_assist": is_assist_tackle,
                "tackle_with_assist_1_player_id": None,
                "tackle_with_assist_1_player_name":
                assist_tackle_1_player_name,
                "tackle_with_assist_1_team": assist_tackle_1_team,
                "tackle_with_assist_2_player_id": None,
                "tackle_with_assist_2_player_name":
                assist_tackle_2_player_name,
                "tackle_with_assist_2_team": assist_tackle_2_team,
                "pass_defense_1_player_id": None,
                "pass_defense_1_player_name": pass_defense_1_player_name,
                "pass_defense_2_player_id": None,
                "pass_defense_2_player_name": None,
                "fumbled_1_team": fumbled_1_team,
                "fumbled_1_player_id": None,
                "fumbled_1_player_name": fumbled_1_player_name,
                "fumbled_2_player_id": None,
                "fumbled_2_player_name": None,
                "fumbled_2_team": None,
                "fumble_recovery_1_team": fumble_recovery_1_team,
                "fumble_recovery_1_yards": fumble_recovery_1_yards,
                "fumble_recovery_1_player_id": None,
                "fumble_recovery_1_player_name": fumble_recovery_1_player_name,
                "fumble_recovery_2_team": None,
                "fumble_recovery_2_yards": None,
                "fumble_recovery_2_player_id": None,
                "fumble_recovery_2_player_name": None,
                "lateral_fumble_recovery_team": lateral_fumble_recovery_team,
                "lateral_fumble_recovery_player_id": None,
                "lateral_fumble_recovery_player_name":
                lateral_fumble_recovery_player_name,
                "sack_player_id": None,
                "sack_player_name": sack_player_name,
                "half_sack_1_player_id": None,
                "half_sack_1_player_name": half_sack_1_player_name,
                "half_sack_2_player_id": None,
                "half_sack_2_player_name": half_sack_2_player_name,
                "missed_fg_return_team": missed_fg_return_team,
                "missed_fg_return_player_id": None,
                "missed_fg_return_player_name": missed_fg_return_player_name,
                "missed_fg_return_yards": missed_fg_return_yards,
                "return_team": return_team,
                "return_yards": return_yards,
                "penalty_team": penalty_team,
                "penalty_player_id": None,
                "penalty_player_name": penalty_player_name,
                "penalty_yards": penalty_yards,
                "replay_or_challenge": is_replay_or_challenge,
                "replay_or_challenge_result": replay_or_challenge_result,
                "penalty_type": penalty_type,
                "defensive_two_point_attempt": None,
                "defensive_two_point_conv": None,
                "defensive_extra_point_attempt":
                is_defensive_extra_point_attempt,
                "defensive_extra_point_conv": is_defensive_extra_point_conv,
                "safety_player_name": safety_player_name,
                "safety_player_id": None,
                "season": None,
                # "series": None,
                # "series_success": None,
                # "series_result": None,
                "order_sequence": None,
                "start_time": None,
                "time_of_day": None,
                "stadium": None,
                "weather": None,
                "play_clock": None,
                "special_teams_play": is_special_teams_play,
                "st_play_type": special_teams_play_type,
                "end_clock_time": play_timestamp,
                "end_yard_line": None,
                "fixed_drive": drive_num,
                # "fixed_drive_result": None,
                # "drive_real_start_time": None,
                # "drive_play_count": None,
                # "drive_time_of_possession": None,
                # "drive_first_downs": None,
                # "drive_inside20": None,
                # "drive_ended_with_score": None,
                # "drive_quarter_start": None,
                # "drive_quarter_end": None,
                # "drive_yards_penalized": None,
                # "drive_start_transition": None,
                # "drive_end_transition": None,
                # "drive_game_clock_start": None,
                # "drive_game_clock_end": None,
                # "drive_start_yard_line": None,
                # "drive_end_yard_line": None,
                # "drive_play_id_started": None,
                # "drive_play_id_ended": None,
                "away_score": None,
                "home_score": None,
                "location": None,
                "result": None,
                "total": None,
                "spread_line": None,
                "total_line": None,
                "div_game": None,
                "roof": None,
                "surface": None,
                "temp": None,
                "wind": None,
                "home_coach": None,
                "away_coach": None,
                "stadium_id": None,
                "game_stadium": None,
                "aborted_play": is_aborted_play,
                "success": is_successful_play,
                "pass": is_pass,
                "rush": is_rush,
                "first_down": is_first_down,
                "special": is_special_teams_play,
                "play": is_scrimmage_play,
                "out_of_bounds": is_out_of_bounds,
                "home_opening_kickoff": home_opening_kickoff,
            },
            index=[0],
        )
        pbp_df_arr.append(temp_df)
        # posteam_score = posteam_score_post
        # defteam_score = defteam_score_post
        # score_differential = score_differential_post

    if len(pbp_df_arr) > 0:
        pbp_df = pd.concat(pbp_df_arr, ignore_index=True)
    else:
        pbp_df = pd.DataFrame()
    return pbp_df, home_opening_kickoff, total_home_score, total_away_score


def get_cfl_pbp_data(fixture_id: int, season: int) -> pd.DataFrame:
    pbp_df = pd.DataFrame()
    pbp_df_arr = []
    quarter_df = pd.DataFrame()


    player_name_columns = [
        "td_player_name",
        "passer_player_name",
        "receiver_player_name",
        "rusher_player_name",
        "lateral_receiver_player_name",
        "lateral_rusher_player_name",
        "lateral_sack_player_name",
        "interception_player_name",
        "lateral_interception_player_name",
        "punt_returner_player_name",
        "lateral_punt_returner_player_name",
        "kickoff_returner_player_name",
        "lateral_kickoff_returner_player_name",
        "punter_player_name",
        "kicker_player_name",
        "own_kickoff_recovery_player_name",
        "blocked_player_name",
        "tackle_for_loss_1_player_name",
        "tackle_for_loss_2_player_name",
        "qb_hit_1_player_name",
        "qb_hit_2_player_name",
        "forced_fumble_player_1_player_name",
        "forced_fumble_player_2_player_name",
        "solo_tackle_1_player_name",
        "solo_tackle_2_player_name",
        "assist_tackle_1_player_name",
        "assist_tackle_2_player_name",
        "assist_tackle_3_player_name",
        "assist_tackle_4_player_name",
        "tackle_with_assist_1_player_name",
        "tackle_with_assist_2_player_name",
        "pass_defense_1_player_name",
        "pass_defense_2_player_name",
        "fumbled_1_player_name",
        "fumbled_2_player_name",
        "fumble_recovery_1_player_name",
        "fumble_recovery_2_player_name",
        "lateral_fumble_recovery_player_name",
        "sack_player_name",
        "half_sack_1_player_name",
        "half_sack_2_player_name",
        "missed_fg_return_player_name",
        "penalty_player_name",
        "safety_player_name",
    ]
    player_id_columns = [
        "td_player_id",
        "passer_player_id",
        "receiver_player_id",
        "rusher_player_id",
        "lateral_receiver_player_id",
        "lateral_rusher_player_id",
        "lateral_sack_player_id",
        "interception_player_id",
        "lateral_interception_player_id",
        "punt_returner_player_id",
        "lateral_punt_returner_player_id",
        "kickoff_returner_player_id",
        "lateral_kickoff_returner_player_id",
        "punter_player_id",
        "kicker_player_id",
        "own_kickoff_recovery_player_id",
        "blocked_player_id",
        "tackle_for_loss_1_player_id",
        "tackle_for_loss_2_player_id",
        "qb_hit_1_player_id",
        "qb_hit_2_player_id",
        "forced_fumble_player_1_player_id",
        "forced_fumble_player_2_player_id",
        "solo_tackle_1_player_id",
        "solo_tackle_2_player_id",
        "assist_tackle_1_player_id",
        "assist_tackle_2_player_id",
        "assist_tackle_3_player_id",
        "assist_tackle_4_player_id",
        "tackle_with_assist_1_player_id",
        "tackle_with_assist_2_player_id",
        "pass_defense_1_player_id",
        "pass_defense_2_player_id",
        "fumbled_1_player_id",
        "fumbled_2_player_id",
        "fumble_recovery_1_player_id",
        "fumble_recovery_2_player_id",
        "lateral_fumble_recovery_player_id",
        "sack_player_id",
        "half_sack_1_player_id",
        "half_sack_2_player_id",
        "missed_fg_return_player_id",
        "penalty_player_id",
        "safety_player_id",
    ]

    played_phases = []
    away_team_abv = ""
    home_team_abv = ""
    home_points = 0
    away_points = 0

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4)"
        + " AppleWebKit/537.36 (KHTML, like Gecko) "
        + "Chrome/138.0.0.0 Safari/537.36",
    }
    home_opening_kickoff = False

    for q in range(1, 5):
        url = (
            "https://gsm-widgets.betstream.betgenius.com/widget-data/"
            + "multisportgametracker?productName=democfl_light"
            + f"&fixtureId={fixture_id}"
            + "&activeContent=playByPlay&sport=AmericanFootball&sportId=17&"
            + f"competitionId=1035&isUsingBetGeniusId=true&phase=Q{q}"
        )
        response = requests.get(url=url, headers=headers)
        time.sleep(1)
        json_data = json.loads(response.text)
        json_data = json_data["data"]
        # with open("test.json", "w+") as f:
        #     f.write(json.dumps(json_data, indent=4))
        try:
            played_phases = json_data["matchInfo"]["playedPhases"]
        except Exception:
            logging.warning(
                f"Issue found when attempting to parse {fixture_id}. " +
                "Attempting re-download."
            )
            time.sleep(15)
            response = requests.get(url=url, headers=headers)
            json_data = json.loads(response.text)
            json_data = json_data["data"]
            played_phases = json_data["matchInfo"]["playedPhases"]

        away_team_abv = json_data["matchInfo"]["awayTeam"]["details"][
            "abbreviation"
        ]
        away_team_id = json_data["matchInfo"]["awayTeam"]["competitorId"]
        home_team_abv = json_data["matchInfo"]["homeTeam"]["details"][
            "abbreviation"
        ]
        home_team_id = json_data["matchInfo"]["homeTeam"]["competitorId"]

        # pbp_data = {}

        if "Q1" in json_data["playByPlayInfo"]:
            logging.info("Parsing Q1 play-by-play data.")
            quarter_df, home_opening_kickoff, home_points, away_points = parser(
                pbp_data=json_data["playByPlayInfo"]["Q1"],
                # quarter_num=1,
                away_team_abv=away_team_abv,
                home_team_abv=home_team_abv,
                home_team_id=home_team_id,
                away_team_id=away_team_id,
                total_home_score=home_points,
                total_away_score=away_points
            )
            home_opening_kickoff= quarter_df["home_opening_kickoff"][0]
        elif "Q2" in json_data["playByPlayInfo"]:
            logging.info("Parsing Q2 play-by-play data.")
            quarter_df, home_opening_kickoff, home_points, away_points = parser(
                pbp_data=json_data["playByPlayInfo"]["Q2"],
                # quarter_num=2,
                away_team_abv=away_team_abv,
                home_team_abv=home_team_abv,
                home_team_id=home_team_id,
                away_team_id=away_team_id,
                total_home_score=home_points,
                total_away_score=away_points
            )
        elif "Q3" in json_data["playByPlayInfo"]:
            logging.info("Parsing Q3 play-by-play data.")
            quarter_df, home_opening_kickoff, home_points, away_points = parser(
                pbp_data=json_data["playByPlayInfo"]["Q3"],
                # quarter_num=3,
                away_team_abv=away_team_abv,
                home_team_abv=home_team_abv,
                home_team_id=home_team_id,
                away_team_id=away_team_id,
                total_home_score=home_points,
                total_away_score=away_points
            )
        elif "Q4" in json_data["playByPlayInfo"]:
            logging.info("Parsing Q4 play-by-play data.")
            quarter_df, home_opening_kickoff, home_points, away_points = parser(
                pbp_data=json_data["playByPlayInfo"]["Q4"],
                # quarter_num=4,
                away_team_abv=away_team_abv,
                home_team_abv=home_team_abv,
                home_team_id=home_team_id,
                away_team_id=away_team_id,
                total_home_score=home_points,
                total_away_score=away_points
            )
        pbp_df_arr.append(quarter_df)
        del quarter_df

    if len(played_phases) > 5:
        url = (
            "https://gsm-widgets.betstream.betgenius.com/widget-data/"
            + "multisportgametracker?productName=democfl_light"
            + f"&fixtureId={fixture_id}"
            + "&activeContent=playByPlay&sport=AmericanFootball&sportId=17&"
            + "competitionId=1035&isUsingBetGeniusId=true&phase=OT"
        )
        response = requests.get(url=url, headers=headers)
        time.sleep(1)
        json_data = json.loads(response.text)
        json_data = json_data["data"]

        played_phases = json_data["matchInfo"]["playedPhases"]

        if "OT" in json_data["playByPlayInfo"]:
            logging.info("Parsing OT play-by-play data.")
            quarter_df, home_opening_kickoff, home_points, away_points = parser(
                pbp_data=json_data["playByPlayInfo"]["OT"],
                # quarter_num=5,
                away_team_abv=away_team_abv,
                home_team_abv=home_team_abv,
                home_team_id=home_team_id,
                away_team_id=away_team_id,
                total_home_score=home_points,
                total_away_score=away_points
            )
            pbp_df_arr.append(quarter_df)
            del quarter_df
        else:
            raise ValueError(
                "The play-by-play data for OT could not be found " +
                f"at the following url {url}"
            )

    elif len(played_phases) > 6:
        raise NotImplementedError(
            "There is now a need to implement logic for a 2OT game."
        )

    pbp_df = pd.concat(pbp_df_arr, ignore_index=True)
    pbp_df["away_score"] = json_data["scoreboardInfo"]["awayScore"]
    pbp_df["home_score"] = json_data["scoreboardInfo"]["homeScore"]

    # player_chain = get_player_chain(
    #     season=season,
    #     away_team_abv=away_team_abv,
    #     home_team_abv=home_team_abv
    # )

    # for i in range(0, len(player_name_columns)):
    #     p_name_column = player_name_columns[i]
    #     p_id_column = player_id_columns[i]

    #     pbp_df[p_id_column] = pbp_df[p_name_column].map(
    #         player_chain
    #     )

    pbp_df["home_opening_kickoff"] = home_opening_kickoff
    return pbp_df


def get_cfl_season_pbp_data(season: int) -> pd.DataFrame:
    """ """
    pbp_df = pd.DataFrame()
    pbp_df_arr = []
    temp_df = pd.DataFrame()

    try:
        os.mkdir("pbp")
    except FileExistsError:
        logging.info("`./pbp` already exists.")

    schedule_df = get_cfl_schedules(season=season)
    schedule_df = schedule_df[schedule_df["eventStatus_name"] != "Pre-Game"]

    fixture_ids_arr = schedule_df["fixtureId"].to_numpy()
    season_types_arr = schedule_df["eventTypeName"].to_numpy()
    weeks_arr = schedule_df["week"].to_numpy()
    pbp_df["season"] = season

    now = datetime.now()
    timestamp_str = now.isoformat()
    with open("pbp/timestamp.json", "w+") as f:
        f.write(
            f"{{\"timestamp\":\"{timestamp_str}\"}}"
        )

    for i in tqdm(range(0, len(fixture_ids_arr))):
        game_id = fixture_ids_arr[i]
        temp_df = get_cfl_pbp_data(game_id, season)
        temp_df["game_id"] = game_id
        temp_df["season_type"] = season_types_arr[i]
        temp_df["week"] = weeks_arr[i]
        pbp_df_arr.append(temp_df)
        del temp_df
    pbp_df = pd.concat(pbp_df_arr, ignore_index=True)
    pbp_df["season"] = season
    pbp_df["order_sequence"] = pbp_df.index
    pbp_df.to_csv(f"pbp/{season}_cfl_pbp.csv", index=False)
    return pbp_df


if __name__ == "__main__":
    now = datetime.now()
    year = now.year

    if now.month < 5:
        year -= 1
    for i in range(year, year + 1):
        get_cfl_season_pbp_data(i)
    # get_cfl_season_pbp_data(now.year)
    # df = get_cfl_pbp_data(9888990, 2023)
    # df.to_csv("test.csv")
