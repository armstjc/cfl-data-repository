import json
import pandas as pd
import requests
from bs4 import BeautifulSoup


def parse_player_id(html_string: str) -> str:
    """ """
    if len(html_string) > 0:
        soup = BeautifulSoup(html_string, features="lxml")
        # player_name = soup.text.replace("\n", "")
        player_id = soup.find("a").get("href")
        player_id = player_id.split("/")[-2]
        # print(player_id, player_name)
    else:
        player_id = ""
    return player_id

def parse_player_name(html_string: str) -> str:
    """ """
    if len(html_string) > 0:
        soup = BeautifulSoup(html_string, features="lxml")
        player_name = soup.text.replace("\n", "")
        # player_id = soup.find("a").get("href")
        # player_id = player_id.split("/")[-2]
        # print(player_id, player_name)
    else:
        player_name = ""
    return player_name


def get_cfl_transactions(season: int) -> pd.DataFrame:
    """ """
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4)"
        + " AppleWebKit/537.36 (KHTML, like Gecko) "
        + "Chrome/125.0.0.0 Safari/537.36",
        # "Referer": "https://www.theufl.com/",
    }
    transactions_df = pd.DataFrame()
    transaction_df_arr = []
    url = (
        "https://www.cfl.ca/wp-content/themes/cfl.ca/inc/admin-ajax.php?"
        + f"action=get_transactions&season={season}"
    )

    response = requests.get(url=url, headers=headers)

    json_data = json.loads(response.text)
    json_data = json_data["data"]
    transactions_df = pd.DataFrame(
        data=json_data,
        columns=[
            "date",
            "team_id",
            "player_html",
            "position",
            "status",
            "college",
            "transaction_id",
            "transaction_desc",
        ],
    )
    transactions_df["player_id"] = transactions_df["player_html"].map(
        lambda x: parse_player_id(x)
    )
    transactions_df["player_name"] = transactions_df["player_html"].map(
        lambda x: parse_player_name(x)
    )
    transactions_df = transactions_df.drop(
        columns=["player_html"]
    )
    print(transactions_df)
    # transactions_df.to_csv("test.csv", index=False)
    # print(json_data)
    return transactions_df


if __name__ == "__main__":
    for i in range(1990, 2000):
        df = get_cfl_transactions(i)
        df.to_csv(f"transactions/{i}_cfl_transactions.csv",index=False)
