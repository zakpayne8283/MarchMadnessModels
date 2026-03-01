from pathlib import Path
from scrapy.selector import Selector

import csv
import scrapy


class BracketsSpider(scrapy.Spider):
    name = "brackets"

    async def start(self):
        urls = [
            "https://www.sports-reference.com/cbb/postseason/men/2025-ncaa.html",
            "https://www.sports-reference.com/cbb/postseason/men/2024-ncaa.html",
            "https://www.sports-reference.com/cbb/postseason/men/2023-ncaa.html",
            "https://www.sports-reference.com/cbb/postseason/men/2022-ncaa.html",
            "https://www.sports-reference.com/cbb/postseason/men/2021-ncaa.html",
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        # The data going to the CSV
        data_to_write = []

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
                    winning_team_name = ""
                    winning_team_score = ""
                    losing_team_seed = ""
                    losing_team_name = ""
                    losing_team_score = ""

                    # Extract the game info from the teams
                    for team in teams:
                        # Winning team
                        if team.css('::attr(class)').get() == 'winner':
                            winning_team_seed = team.css('span::text').get()
                            winning_team_name = team.css('a:nth-child(2)::text').get()
                            winning_team_score = team.css('a:nth-child(3)::text').get()
                        # Losing team
                        else:
                            losing_team_seed = team.css('span::text').get()
                            losing_team_name = team.css('a:nth-child(2)::text').get()
                            losing_team_score = team.css('a:nth-child(3)::text').get()

                    # Save the data for write
                    data_to_write.append([bracket_type, games_in_round, winning_team_seed, winning_team_name, winning_team_score, losing_team_seed, losing_team_name, losing_team_score])

        # Open the file in write mode ('w') with newline=''
        with open(f'../data/raw/brackets-{file_name}.csv', 'w', newline='') as csvfile:
            # Create a writer object
            writer = csv.writer(csvfile)

            # Loop through your data and write each row one by one
            for row in data_to_write:
                writer.writerow(row)
