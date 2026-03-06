import os
import pandas as pd

raw_directory = './data/raw/'
processed_directory = './data/processed/'

def main():
    prepare_directory()
    process_brackets()
    process_teams()

def prepare_directory():
    # Make the output directory as needed
    os.makedirs(processed_directory, exist_ok=True)

def process_brackets():
    # Where to find the brackets
    brackets_raw = os.path.join(raw_directory, 'brackets')

    # What's being output
    df_main = pd.DataFrame()

    # For each data file in /data/raw
    for filename in os.listdir(brackets_raw):
        file_year = filename.split('.')[0][-4:]
        # CSVs only
        if filename.endswith(".csv"):
            # Load the CSV into a df
            df = pd.read_csv(os.path.join(brackets_raw, filename))

            # Specifiy the Year
            df['year'] = file_year

            # Simplify which round it is
            df['round_of'] = df.apply(__determine_round, axis=1)

            # Drop unneeded columns
            df = df.drop(columns=['bracket_type', 'games_in_round'])

            df_main = pd.concat([df_main, df])

    df_main.to_csv(os.path.join(processed_directory, 'brackets.csv'), index=False)

def process_teams():
    teams_raw = os.path.join(raw_directory, 'teams')

    # roster_file_name = 'roster.csv'
    team_info_file_name = 'team-info.csv'

    # Each data frame
    # df_stats = pd.DataFrame()
    df_info  = pd.DataFrame()

    # Go through each year
    for year in os.listdir(teams_raw):
        # Specify the year's directory
        year_dir = os.path.join(teams_raw, year)

        # Go through each team for that year
        for team in os.listdir(year_dir):
            # Root dir for each team that year
            team_dir = os.path.join(year_dir, team)

            # Load the dataframe
            team_info_df = pd.read_csv(os.path.join(team_dir, team_info_file_name))

            # Note the team and year
            team_info_df['team'] = team
            team_info_df['year'] = year

            # Concat into the big data frame - we'll finish cleaning later
            df_info = pd.concat([df_info, team_info_df])

    df_info = __clean_team_info(df_info)
    df_info.to_csv(os.path.join(processed_directory, 'team-info.csv'), index=False)

    # Implement this later
    # df_stats.loc[df_stats.index == 0, 'Type'] = "Team Stat"
    # df_stats.loc[df_stats.index == 1, 'Type'] = "Team Rank"
    # df_stats.loc[df_stats.index == 2, 'Type'] = "Opponent Stat"
    # df_stats.loc[df_stats.index == 3, 'Type'] = "Opponent Rank"
    # df_stats = df_stats.reset_index()
    # df_stats.drop(columns=['index', 'Unnamed: 0']).head()

def __clean_team_info(team_info_df):
    # TODO: Fix the crawler for the Records column
    team_info_df = team_info_df.drop(columns=['Records'])

    # Extract SOS, SRS, ORtg, and DRtg
    team_info_df['SOS']  = team_info_df['SOS'].str.strip().str.extract(r'^([^ (]*)', expand=False)
    team_info_df['SRS']  = team_info_df['SRS'].str.strip().str.extract(r'^([^ (]*)', expand=False)
    team_info_df['ORtg'] = team_info_df['ORtg'].str.strip().str.extract(r'(\d+\.\d+)', expand=False)
    team_info_df['DRtg'] = team_info_df['DRtg'].str.strip().str.extract(r'(\d+\.\d+)', expand=False)

    return team_info_df


def __determine_round(row):
    """
    Determines which round of the tournament for that game based on which bracket type and number of games in the round
    """
    # I dislike super python-y statements like this
    multiplier = 4 if row['bracket_type'] == 'team16' else 1
    
    # multiplier is 4 in the regional brackets because there are 4 of them
    # theres only 1 final 4 bracket
    return row['games_in_round'] * 2 * multiplier

if __name__ == '__main__':
    main()