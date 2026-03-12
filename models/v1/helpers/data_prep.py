import pandas as pd
import numpy as np

def data_clean():
    # Load the processed data
    processed_brackets = pd.read_csv('./data/processed/brackets.csv')
    processed_teams = pd.read_csv('./data/processed/team-info.csv')

    # Random state for team a/b
    rng = np.random.default_rng(42)
    flip = rng.integers(0, 2, size=len(processed_brackets)).astype(bool)

    # Output
    df = pd.DataFrame()

    # Add columns to output for Team A/B 
    df['year'] = processed_brackets['year']
    df['team_a'] = np.where(flip, processed_brackets['winning_team_name'], processed_brackets['losing_team_name'])
    df['team_a_seed'] = np.where(flip, processed_brackets['winning_team_seed'], processed_brackets['losing_team_seed'])
    df['team_b'] = np.where(flip, processed_brackets['losing_team_name'], processed_brackets['winning_team_name'])
    df['team_b_seed'] = np.where(flip, processed_brackets['losing_team_seed'], processed_brackets['winning_team_seed'])
    df['team_a_won'] = flip.astype(int)

    # Pull stats from the other dataframe
    df = pd.merge(df, processed_teams.add_prefix('team_a_'), left_on=['year', 'team_a'], right_on=['team_a_year', 'team_a_team'])
    df = pd.merge(df, processed_teams.add_prefix('team_b_'), left_on=['year', 'team_b'], right_on=['team_b_year', 'team_b_team'])

    # Compute the feature diffs
    for col in ['seed', 'SOS', 'SRS', 'ORtg', 'DRtg'] :
        df[f'diff_{col}'] = df[f'team_a_{col}'] - df[f'team_b_{col}']

    # Remove unneeded columns
    for col in ['team', 'year', 'seed', 'SOS', 'SRS', 'ORtg', 'DRtg']:
        df = df.drop(columns=[f'team_a_{col}', f'team_b_{col}'])

    return df


def prepare_features(df):
    return df.drop(columns=['team_a', 'team_b', 'team_a_won', 'year'])
    

def prepare_targets(df):
    return df['team_a_won']