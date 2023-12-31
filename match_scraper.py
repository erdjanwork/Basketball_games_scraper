import pandas as pd
from bs4 import BeautifulSoup as bs
import psycopg2

# HTML script that we are going to scrape
tab_script = """insert your script here"""

# We parse the script and extract the table from it
soup = bs(tab_script, "html.parser")
my_table = soup.find('table', attrs={'class': 'tbl'})

# We create a list of column names that we will need for our pandas dataframe
COL_NAMES = ["name", 'min', "twoP", "threeP", "free throws", "offensive rebounds", "defensive rebounds",
             "total rebounds", "assists", "personal fouls", "turnovers", "steals", "blocks", "points", "value"]

# player_names = []
player_stats = []

# We iterate over all the rows in the table and extract the data
for row in my_table.find_all('tr'):
    cells = row.find_all('td')
    if len(cells) > 1:
        name_cell = cells[1].find('b')  # Locate the player name within the second column
        if name_cell:
            # player_names.append(name_cell.text.strip())  # Append the player name to the list
            stats = [cell.text.strip() for cell in cells[1:]]  # Extract player stats
            player_stats.append(stats)  # Append the player stats to the list

# Display the player names and their respective stats

# for name, stats in zip(player_names, player_stats):
#     print(f"Name: {name}")
#     print(f"Stats: {stats}")


# Creating the Dataframe
df = pd.DataFrame(data=player_stats, columns=COL_NAMES)
print(df.columns)

# Connection with postgres database
conn = psycopg2.connect(
    database='database_name',
    host='localhost',
    user='username',
    password='password',
    port='5432'
)

curr = conn.cursor()

table_name = 'your_table_name'

# Inserting the data into the table in the database
for index, row in df.iterrows():
    columns = ', '.join(['"' + col + '"' for col in df.columns])  # Column names with double quotes
    values = ', '.join([f"'{val}'" for val in row])  # Values as string
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
    curr.execute(insert_query)
    conn.commit()

curr.close()
conn.close()
