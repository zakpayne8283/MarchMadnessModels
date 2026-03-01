import os
import pandas as pd

raw_directory = './data/raw/'
processed_directory = './data/processed/'

def main():

    process_brackets()


def process_brackets():
    brackets_raw = os.path.join(raw_directory, 'brackets')
    brackets_processed = os.path.join(processed_directory, 'brackets')

    # Make the folder if it's needed
    os.makedirs(brackets_raw, exist_ok=True)
    os.makedirs(brackets_processed, exist_ok=True)

    # For each data file in /data/raw
    for filename in os.listdir(brackets_raw):
        # CSVs only
        if filename.endswith(".csv"):
            # Load the CSV into a df
            df = pd.read_csv(os.path.join(brackets_raw, filename))
            # Simplify which round it is
            df['round_of'] = df.apply(__determine_round, axis=1)
            # Drop unneeded columns
            df = df.drop(columns=['bracket_type', 'games_in_round'])

            df.to_csv(os.path.join(brackets_processed, filename))


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