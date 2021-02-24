from ohmysportsfeedspy import MySportsFeeds
import sqlalchemy
import json
from tqdm import tqdm
from datetime import datetime


def flatten(d,sep="_"):
    import collections

    obj = {}

    def recurse(t,parent_key=""):
        
        if isinstance(t,list):
            for i in range(len(t)):
                recurse(t[i],parent_key + sep + str(i) if parent_key else str(i))
        elif isinstance(t,dict):
            for k,v in t.items():
                recurse(v,parent_key + sep + k if parent_key else k)
        else:
            obj[parent_key] = t

    recurse(d)

    return obj


def parse_game(game):
    output = {key: game.get(key) for key in game.keys() if key not in ("broadcasters", "officials")}
    output = flatten(output)
    output["game_id"] = output.pop("id")
    return output


def parse_team(team):
    output = flatten(team)
    output["team_id"] = output.pop("id")
    output["team_abbreviation"] = output.pop("abbreviation")
    return output


def parse_lineup(lineup):
    print(lineup)
    team = flatten(lineup["team"])
    output = {**team}
    return output
    

def load_games(templates_dict, **kwargs):
    engine = sqlalchemy.create_engine(templates_dict["postgres_connection_str"])
    msf = MySportsFeeds(version="2.1")
    msf.authenticate(templates_dict["msf_api_key"], "MYSPORTSFEEDS")

    season = templates_dict["season"]

    output = msf.msf_get_data(league='nba', season=season, feed='seasonal_games', format='json', force=True)

    upload = [
        {
            "season": season,
            **flatten({
                key: value for key, value in game["schedule"].items()
                if key not in ("officials", "broadcasters")
            }),
            **flatten({
                key: value for key, value in game["score"].items()
                if key not in ("quarters",)
            }),
            "quarters": game["score"]["quarters"],
            "officials": game["schedule"]["officials"],
            "broadcasters": game["schedule"]["broadcasters"]
        } for game in output["games"]
    ]

    with engine.begin() as conn:
        conn.execute("delete from games where season = %s", (season,))
        for game in tqdm(upload, mininterval=60, desc="Uploading games"):
            game = {key: value for key, value in game.items() if value is not None}
            conn.execute(
                "insert into games ({columns}) values ({values})".format(
                    columns=", ".join(game.keys()),
                    values=", ".join(["%s"] * len(game))
                ),
                tuple(json.dumps(x) if type(x) in (dict, list) else x for x in game.values())
            )

    return f'Loaded games for {season}'


def load_game_logs(templates_dict, **kwargs):
    engine = sqlalchemy.create_engine(templates_dict["postgres_connection_str"])
    msf = MySportsFeeds(version="2.1")
    msf.authenticate(templates_dict["msf_api_key"], "MYSPORTSFEEDS")
    
    season = templates_dict["season"]
    last_n_days = templates_dict["last_n_days"]

    with engine.connect() as conn:
        sqlq = """
            with completed_games as (
                select
                    distinct (starttime at time zone 'EST')::date as game_date
                from games
                where playedStatus = 'COMPLETED'
                and season = %s
            )
            , date_rank as (
                select
                    game_date
                    , row_number() over(order by game_date desc) as date_rank
                from
                    completed_games
            )
            select game_date
            from date_rank
            where date_rank <= %s
            order by 1 desc;
        """
        completed_game_dates = list(map(lambda x: x[0], conn.execute(sqlq, (season, last_n_days)).fetchall()))
        for date in completed_game_dates:
            assert date <= datetime.today().date(), f"{date} must be before current date"

    output = [
        msf.msf_get_data(league='nba', season=season, feed='daily_player_gamelogs', date=game_date.strftime("%Y%m%d"), format='json', force=True)
        for game_date in tqdm(completed_game_dates, mininterval=60, desc="Downloadign game logs")
    ]

    upload = [
        flatten(log)
        for day in output for log in day["gamelogs"]
    ]

    with engine.begin() as conn:
        for gamelog in tqdm(upload, mininterval=60, desc="Uploading game logs"):
            gamelog = {key: value for key, value in gamelog.items() if value is not None}
            conn.execute(
                "delete from player_gamelogs where game_id = %s and player_id = %s and team_id = %s",
                (gamelog["game_id"], gamelog["player_id"], gamelog["team_id"])
            )
            conn.execute(
                "insert into player_gamelogs ({columns}) values ({values})".format(
                    columns=", ".join(gamelog.keys()),
                    values=", ".join(["%s"] * len(gamelog))
                ),
                tuple(gamelog.values())
            )
    return f'Loaded game logs for {season}'


def load_lineup(templates_dict, **kwargs):
    engine = sqlalchemy.create_engine(templates_dict["postgres_connection_str"])
    msf = MySportsFeeds(version="2.1")
    msf.authenticate(templates_dict["msf_api_key"], "MYSPORTSFEEDS")
    
    season = templates_dict["season"]
    last_n_days = templates_dict["last_n_days"]
    
    with engine.connect() as conn:
        sqlq = """
            with season_games as (
                select *
                from games
                where season = %s
            )
            , completed_games as (
                select
                    distinct (starttime at time zone 'EST')::date as game_date
                from season_games
                where playedStatus = 'COMPLETED'
            )
            , date_rank as (
                select
                    game_date
                    , row_number() over(order by game_date desc) as date_rank
                from
                    completed_games
            )
            select distinct id
            from season_games
            where
                (starttime at time zone 'EST')::date
                    >= coalesce(
                        (select min(game_date) from date_rank where date_rank <= %s),
                        '1970-01-01'
                    )
            order by 1 desc;
        """
        game_ids = list(map(lambda x: x[0], conn.execute(sqlq, (season, last_n_days)).fetchall()))

    output = [
        msf.msf_get_data(league='nba', season=season, feed='game_lineup', game=game_id, format='json', force=True)
        for game_id in tqdm(game_ids, mininterval=60, desc="Downloading lineups")
    ]

    upload = [
        {**parse_game(game["game"]), **parse_team(home_away["team"]), "type": expected_actual, **flatten(player)}
        for game in output
        for home_away in game["teamLineups"]
        for expected_actual in ("expected", "actual") if home_away[expected_actual] is not None
        for player in home_away[expected_actual]["lineupPositions"] if player["player"] is not None
    ]

    with engine.begin() as conn:
        for player in tqdm(upload, mininterval=60, desc="Uploading lineups"):
            player = {key: value for key, value in player.items() if value is not None}
            conn.execute(
                "delete from lineups where game_id = %s and player_id = %s and team_id = %s and type = %s",
                (player["game_id"], player["player_id"], player["team_id"], player["type"])
            )
            conn.execute(
                "insert into lineups ({columns}) values ({values})".format(
                    columns=", ".join(player.keys()),
                    values=", ".join(["%s"] * len(player))
                ),
                tuple(player.values())
            )
    
    return f'Loaded lineups for {season}'


def load_dfs_stats(templates_dict, **kwargs):
    engine = sqlalchemy.create_engine(templates_dict["postgres_connection_str"])
    msf = MySportsFeeds(version="2.1")
    msf.authenticate(templates_dict["msf_api_key"], "MYSPORTSFEEDS")
    
    season = templates_dict["season"]
    last_n_days = templates_dict["last_n_days"]
    
    with engine.connect() as conn:
        sqlq = """
            with season_games as (
                select *
                from games
                where season = %s
            )
            , completed_games as (
                select
                    distinct (starttime at time zone 'EST')::date as game_date
                from season_games
                where playedStatus = 'COMPLETED'
            )
            , date_rank as (
                select
                    game_date
                    , row_number() over(order by game_date desc) as date_rank
                from
                    completed_games
            )
            select distinct (starttime at time zone 'EST')::date as game_date
            from season_games
            where
                (starttime at time zone 'EST')::date
                    >= coalesce(
                        (select min(game_date) from date_rank where date_rank <= %s),
                        '1970-01-01'
                    )
            order by 1 desc;
        """
        game_dates = list(map(lambda x: x[0], conn.execute(sqlq, (season, last_n_days)).fetchall()))
    
    output = [
        msf.msf_get_data(league='nba', season=season, feed='daily_dfs', date=game_date.strftime("%Y%m%d"), format='json', force=True)
        for game_date in tqdm(game_dates, mininterval=60, desc="Downloading dfs stats")
    ]
    output = [x for x in output if x.get("sources") is not None]   

    upload = [
        {
            "source": source["source"],
            "slate_minGameStart": slate["minGameStart"],
            "slate_date": slate["forDate"],
            "slate_id": str(slate["identifier"]),
            "label": slate["label"],
            "player_id": player["player"]["id"],
            "rosterSlots": player["rosterSlots"],
        }
        for day in output
        for source in day["sources"]
        for slate in source["slates"]
        for player in slate["players"] if player["player"] is not None
    ]    

    with engine.begin() as conn:
        for player in tqdm(upload, mininterval=60, desc="Uploading dfs stats"):
            conn.execute(
                """
                    delete from dfs
                    where
                        source = %s and slate_id = %s and player_id = %s 
                        and slate_minGameStart = %s and slate_date = %s and label = %s
                """,
                (player["source"], player["slate_id"], player["player_id"], player["slate_minGameStart"], player["slate_date"], player["label"])
            )
            conn.execute(
                "insert into dfs ({columns}) values ({values})".format(
                    columns=", ".join(player.keys()),
                    values=", ".join(["%s"] * len(player))
                ),
                [f"{{{','.join(x)}}}" if type(x) == list else x for x in player.values()]
            )
    return f'Loaded dfs for {season}'


def load_play_by_play(templates_dict, **kwargs):
    engine = sqlalchemy.create_engine(templates_dict["postgres_connection_str"])
    msf = MySportsFeeds(version="2.1")
    msf.authenticate(templates_dict["msf_api_key"], "MYSPORTSFEEDS")
    
    season = templates_dict["season"]

    with engine.connect() as conn:
        sqlq = """
            select distinct id
            from games
            where playedStatus = 'COMPLETED'
            and season = %s
            order by 1 desc;
        """
        completed_game_ids = list(map(lambda x: x[0], conn.execute(sqlq, (season,)).fetchall()))

    output = [
        msf.msf_get_data(league='nba', season=season, feed='game_playbyplay', game=game_id, format='json', force=True)
        for game_id in tqdm(completed_game_ids, mininterval=60, desc="Download play by play")
    ]

    upload = [
        {
            "game_id": game["game"]["id"],
            "playStatus": play["playStatus"],
            "description": play["description"],
            "playType": [x for x in play.keys() if x not in ("description", "playStatus")][0],
            "play": play[[x for x in play.keys() if x not in ("description", "playStatus")][0]],
        }
        for game in output
        for play in game["plays"]
    ]

    with engine.begin() as conn:
        for play in tqdm(upload, mininterval=60, desc="Upload play by play"):
            conn.execute(
                "delete from playbyplay where game_id = %s", (play["game_id"],)
            )
            conn.execute(
                "insert into playbyplay ({columns}) values ({values})".format(
                    columns=", ".join(play.keys()),
                    values=", ".join(["%s"] * len(play))
                ),
                tuple(json.dumps(x) if type(x) == dict else x for x in play.values())
            )

    return f'Loaded play-by-play for {season}'


def load_players(templates_dict, **kwargs):
    engine = sqlalchemy.create_engine(templates_dict["postgres_connection_str"])
    msf = MySportsFeeds(version="2.1")
    msf.authenticate(templates_dict["msf_api_key"], "MYSPORTSFEEDS")
    
    season = templates_dict["season"]

    output = msf.msf_get_data(league='nba', season=season, feed='players', format='json', force=True)

    upload = [
        {
            **flatten({
                key: value 
                for key, value in player["player"].items()
                if key not in ("externalMappings", "socialMediaAccounts")
            }),
            "externalMappings": {src["source"]: src["id"] for src in player["player"].get("externalMappings", [])},
            "socialMediaAccounts": {media["mediaType"]: media["value"] for media in player["player"].get("socialMediaAccounts", [])},
        }
        for player in output["players"]
    ]

    with engine.begin() as conn:
        for player in tqdm(upload, mininterval=60, desc="Upload players"):
            player = {key: value for key, value in player.items() if value is not None}
            conn.execute(
                "delete from players where id = %s",
                (player["id"],)
            )
            conn.execute(
                "insert into players ({columns}) values ({values})".format(
                    columns=", ".join(player.keys()),
                    values=", ".join(["%s"] * len(player))
                ),
                tuple(json.dumps(x) if type(x) == dict else x for x in player.values())
            )
    return f'Loaded players'


def load_game_lines(templates_dict, **kwargs):
    engine = sqlalchemy.create_engine(templates_dict["postgres_connection_str"])
    msf = MySportsFeeds(version="2.1")
    msf.authenticate(templates_dict["msf_api_key"], "MYSPORTSFEEDS")
    
    season = templates_dict["season"]

    with engine.connect() as conn:
        sqlq = """
            select distinct (starttime at time zone 'EST')::date
            from games
            where season = %s
            order by 1 desc;
        """
        game_dates = list(map(lambda x: x[0], conn.execute(sqlq, (season,)).fetchall()))

    output = [
        msf.msf_get_data(league='nba', season=season, feed='daily_game_lines', date=game_date.strftime("%Y%m%d"), format='json', force=True)
        for game_date in tqdm(game_dates, mininterval=60, desc="Download game lines")
    ]

    upload = [
        {
            "game_id": game["game"]["id"],
            "source": source["source"]["name"],
            "moneyLines": source["moneyLines"],
            "pointSpreads": source["pointSpreads"],
        }
        for day in output
        for game in day["gameLines"]
        for source in game["lines"]
    ]

    with engine.begin() as conn:
        for gameline in tqdm(upload, mininterval=60, desc="Upload game lines"):
            gameline = {key: value for key, value in gameline.items() if value is not None}
            conn.execute(
                "delete from gamelines where game_id = %s and source = %s",
                (gameline["game_id"], gameline["source"])
            )
            conn.execute(
                "insert into gamelines ({columns}) values ({values})".format(
                    columns=", ".join(gameline.keys()),
                    values=", ".join(["%s"] * len(gameline))
                ),
                tuple(json.dumps(x) if type(x) == list else x for x in gameline.values())
            )
    
    return f'Loaded game lines for {season}'