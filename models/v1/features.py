import pandas as pd
import numpy as np
import yaml

def load_config():
    with open("models/v1/config.yaml") as f:
        return yaml.safe_load(f)

def prepare_data():
    config = load_config()

    # Load the processed data
    processed_brackets = pd.read_csv('./data/raw/brackets.csv')
    processed_teams = pd.read_csv('./data/raw/team-stats.csv')

    # Pull stats from the other dataframe
    df = pd.merge(processed_brackets, processed_teams.add_prefix('team_a_'), left_on=['year', 'team_a'], right_on=['team_a_year', 'team_a_team'])
    df = pd.merge(df, processed_teams.add_prefix('team_b_'), left_on=['year', 'team_b'], right_on=['team_b_year', 'team_b_team'])

    # Compute the feature diffs
    for feature in config['features']:
        df[f'diff_{feature}'] = df[f'team_a_{feature}'] - df[f'team_b_{feature}']

    return df


def prepare_features(df):
    # Load the features
    features = load_config()['features']
    # Only return columns that match `diff_{feature}`
    cols_to_select = []
    for feature in features:
        if f'diff_{feature}' in df.columns:
            cols_to_select.append(f'diff_{feature}')
    
    return df[cols_to_select]
    

def prepare_targets(df):
    return df['team_a_won']