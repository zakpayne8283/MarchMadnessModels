import csv
import os
import pandas as pd
import scrapy

class BracketsSpider(scrapy.Spider):
    # scrapy settings
    name = "brackets"
    custom_settings = {
        'DOWNLOAD_DELAY': 5
    }

    # my settings
    ROOT_URL = 'https://www.sports-reference.com'
    RAW_DATA_DIR = '../data/raw/'
    FILE_NAMES = {
        'roster': 'roster.csv',
        'team-info': 'team-info.csv'
    }

    # data out
    df_bracket = pd.DataFrame(columns=[
        'year',
        'bracket_type',
        'games_in_round',
        'winning_team_seed',
        'winning_team_name',
        'winning_team_score',
        'losing_team_seed',
        'losing_team_name', 
        'losing_team_score'
    ])
    df_teamstats = pd.DataFrame(columns=[
        'team',
        'year'
        'SOS',
        'SRS',
        'Records',
        'ORtg',
        'DRtg'
    ])

    # scrapy method
    async def start(self):
        # Tournament years to scrape
        years = [2021]#, 2022, 2023, 2024, 2025]

        for year in years:
            url = f'{self.ROOT_URL}/cbb/postseason/men/{year}-ncaa.html'
            yield scrapy.Request(url=url, callback=self.parse_brackets)

    # scrapy method
    def close(self, reason):
        # temp cache the dfs so I don't hit 429s or take forever for testing
        self.df_bracket.to_csv('temp_bracket.csv')
        self.df_teamstats.to_csv('temp_teamstats.csv')

        self.__create_csvs()

    def parse_brackets(self, response):
        # TODO: Also handle scraping the latest year, which will have incomplete brackets

        # File name indicator
        year = response.url.split("/")[-1][:4]

        # Page has 5 `div#bracket`s for UI nav
        regional_brackets = response.css('div#bracket')

        # 5 brackets are given for each tournament by region and final 4
        for bracket in regional_brackets:

            # regional or final 4
            bracket_type = bracket.css('::attr(class)').get()
            # get the rounds from the bracket
            rounds = bracket.css('div.round')

            # Extract each round of that bracket
            for round in rounds:

                # Pull each game
                # xpath because games are just like `div.round > div`
                games = round.xpath('div')

                # games in round tells us what round it is paired with bracket_type
                games_in_round = len(games)

                for game in games:
                    teams = game.xpath('div')

                    # Winner of the bracket
                    # No data to extract
                    if len(teams) == 1:
                        continue

                    # Outputs
                    winning_team_seed = ""
                    winning_team_score = ""
                    winning_team_slug = ""
                    losing_team_seed = ""
                    losing_team_score = ""
                    losing_team_slug = ""

                    # Extract the game info from the teams
                    for team in teams:
                        
                        try:

                            # Pull the team URL out to scrape later
                            team_link = team.css('a:nth-child(2)::attr(href)').get()

                            # Winning team
                            if team.css('::attr(class)').get() == 'winner':
                                winning_team_seed = team.css('span::text').get()
                                winning_team_score = team.css('a:nth-child(3)::text').get()
                                winning_team_slug = team_link.split('/')[3]
                            # Losing team
                            else:
                                losing_team_seed = team.css('span::text').get()
                                losing_team_score = team.css('a:nth-child(3)::text').get()
                                losing_team_slug = team_link.split('/')[3]

                            if team_link:
                                yield response.follow(team_link, callback=self.parse_teams)
                            else:
                                raise Exception()
                            
                        except:
                            print("Error parsing team(s)")

                    # Append the data to the df
                    new_row = {
                        'year': year,
                        'bracket_type': bracket_type,
                        'games_in_round': games_in_round,
                        'winning_team_seed': winning_team_seed,
                        'winning_team_slug': winning_team_slug,
                        'winning_team_score': winning_team_score,
                        'losing_team_seed': losing_team_seed,
                        'losing_team_slug': losing_team_slug,
                        'losing_team_score': losing_team_score
                    }

                    self.df_bracket.loc[len(self.df_bracket)] = new_row

    def parse_teams(self, response):
        team_slug = response.url.split('/')[-3]
        team_year = response.url.split('/')[-1][:4]

        # Extract basic team info from the summary of the page
        strength_of_schedule = response.xpath('//a[@href="/cbb/about/glossary.html#sos"]/../../text()').get()
        srs = response.xpath('//a[@href="/cbb/about/glossary.html#srs"]/../../text()').get()
        team_records = response.xpath('//strong[text()="Record:"]/../text()').getall()[2]   # TODO: Something isn't working with this
        o_rtg = response.xpath('//strong[text()="ORtg:"]/../text()').getall()
        d_rtg = response.xpath('//strong[text()="DRtg:"]/../text()').getall()

        new_row = {
            'team': team_slug,
            'year': team_year,
            'sos': strength_of_schedule,
            'srs': srs,
            'records': team_records,
            'ortg': o_rtg,
            'drtg': d_rtg
        }

        self.df_teamstats.loc[len(self.df_teamstats)] = new_row

    def __create_csvs(self):
        print("== Raw Data ==")
        print(self.df_bracket.head())
        print('='*20)
        print(self.df_teamstats.head())

        print("== Cleaned Data ==")
        self.__clean_brackets_df()
        self.__clean_teamstats_df()
        print(self.df_bracket.head())
        print('='*20)
        print(self.df_teamstats.head())

    def __clean_brackets_df(self):
        # Simplify which round it is
        self.df_bracket['round_of'] = self.df_bracket.apply(self.__determine_round, axis=1)

        # Drop unneeded columns
        self.df_bracket = self.df_bracket.drop(columns=['bracket_type', 'games_in_round'])
    
    def __clean_teamstats_df(self):
        # TODO: Fix the crawler for the Records column
        self.df_teamstats = self.df_teamstats.drop(columns=['Records'])

        # Extract SOS, SRS, ORtg, and DRtg
        self.df_teamstats['SOS']  = self.df_teamstats['SOS'].str.strip().str.extract(r'^([^ (]*)', expand=False)
        self.df_teamstats['SRS']  = self.df_teamstats['SRS'].str.strip().str.extract(r'^([^ (]*)', expand=False)
        self.df_teamstats['ORtg'] = self.df_teamstats['ORtg'].str.strip().str.extract(r'(\d+\.\d+)', expand=False)
        self.df_teamstats['DRtg'] = self.df_teamstats['DRtg'].str.strip().str.extract(r'(\d+\.\d+)', expand=False)

    def __determine_round(row):
        """
        Determines which round of the tournament for that game based on which bracket type and number of games in the round
        """
        # I dislike super python-y statements like this
        multiplier = 4 if row['bracket_type'] == 'team16' else 1
        
        # multiplier is 4 in the regional brackets because there are 4 of them
        # theres only 1 final 4 bracket
        return row['games_in_round'] * 2 * multiplier