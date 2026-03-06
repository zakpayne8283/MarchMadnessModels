from io import StringIO
from pathlib import Path
from scrapy.selector import Selector

import csv
import os
import pandas as pd
import scrapy

class BracketsSpider(scrapy.Spider):
    name = "brackets"
    custom_settings = {
        'DOWNLOAD_DELAY': 10
    }

    root_url = 'https://www.sports-reference.com'

    async def start(self):
        urls = [
            "https://www.sports-reference.com/cbb/postseason/men/2025-ncaa.html",
            "https://www.sports-reference.com/cbb/postseason/men/2024-ncaa.html",
            "https://www.sports-reference.com/cbb/postseason/men/2023-ncaa.html",
            "https://www.sports-reference.com/cbb/postseason/men/2022-ncaa.html",
            "https://www.sports-reference.com/cbb/postseason/men/2021-ncaa.html",
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse_brackets)

    def parse_brackets(self, response):
        # The data going to the CSV
        data_to_write = [[
            'bracket_type',
            'games_in_round',
            'winning_team_seed',
            'winning_team_name',
            'winning_team_score',
            'losing_team_seed',
            'losing_team_name', 
            'losing_team_score'
            ]]

        # File name indicator
        file_name = response.url.split("/")[-1][:4]

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

                    # Save the data for write
                    data_to_write.append([
                        bracket_type,
                        games_in_round,
                        winning_team_seed,
                        winning_team_slug,
                        winning_team_score,
                        losing_team_seed,
                        losing_team_slug,
                        losing_team_score
                        ])

        folder_path = '../data/raw/brackets/'

        # Make the folder if it's needed
        os.makedirs(folder_path, exist_ok=True)

        # Open the file in write mode ('w') with newline=''
        with open(os.path.join(folder_path, f'brackets-{file_name}.csv'), 'w', newline='') as csvfile:
            # Create a writer object
            writer = csv.writer(csvfile)

            # Loop through your data and write each row one by one
            for row in data_to_write:
                writer.writerow(row)


    def parse_teams(self, response):
        team_slug = response.url.split('/')[-3]
        team_year = response.url.split('/')[-1][:4]

        # Create the file path for this team's data
        folder_path = f'../data/raw/teams/{team_year}/{team_slug}'

        # Extract basic team info from the summary of the page
        strength_of_schedule = response.xpath('//a[@href="/cbb/about/glossary.html#sos"]/../../text()').get()
        srs = response.xpath('//a[@href="/cbb/about/glossary.html#srs"]/../../text()').get()
        team_records = response.xpath('//strong[text()="Record:"]/../text()').getall()[2]   # TODO: Something isn't working with this
        o_rtg = response.xpath('//strong[text()="ORtg:"]/../text()').getall()
        d_rtg = response.xpath('//strong[text()="DRtg:"]/../text()').getall()

        # Make the folder if it's needed
        os.makedirs(folder_path, exist_ok=True)
        
        # Create the team info CSV TODO: Maybe rework this or something? Seems a lot of have extra files for one line
        with open(os.path.join(folder_path, f'team-info.csv'), 'w', newline='') as csvfile:
            # Create a writer object
            writer = csv.writer(csvfile)

            writer.writerow(['SOS', 'SRS', 'Records', 'ORtg', 'DRtg'])
            writer.writerow([
                strength_of_schedule,
                srs,
                team_records,
                o_rtg,
                d_rtg
            ])

        # Pull the roster
        roster = response.css('table#roster').get()
        df = pd.read_html(StringIO(roster))[0]
        df.to_csv(os.path.join(folder_path, 'roster.csv'), index=False)

        # Pull `season-total_per_game`
        season_total_per_game = response.css('table#season-total_per_game').get()
        df = pd.read_html(StringIO(season_total_per_game))[0]
        df.to_csv(os.path.join(folder_path, 'season_total_per_game.csv'), index=False)

        season_total_totals = response.css('table#season-total_totals').get()
        df = pd.read_html(StringIO(season_total_totals))[0]
        df.to_csv(os.path.join(folder_path, 'season_total_totals.csv'), index=False)