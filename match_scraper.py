import pandas as pd
from bs4 import BeautifulSoup as bs
import psycopg2

# HTML script that we are going to scrape
tab_script = """<table align="center" border="0" width="100%" cellspacing="0" cellpadding="5" class="back_white tbl"><tbody><tr class="care_tit"><td colspan="16"><a href="team.php?id=28">Шумен</a></td></tr><tr class="tr_header">
			<td rowspan="2">#</td>
			<td rowspan="2" style="text-align: left; padding-left:5px;">Стартова петица</td>
			<td rowspan="2">Мин</td>
			<td rowspan="2">2Т</td>
			<td rowspan="2">3Т</td>
			<td rowspan="2">НУ</td>
			<td colspan="3">БОРБИ</td>
			<td rowspan="2">АС</td>
			<td rowspan="2">ЛН</td>
			<td rowspan="2">ГР</td>
			<td rowspan="2">ОТ</td>
			<td rowspan="2">БЛ</td>
			<td rowspan="2">ТОЧ</td>
			<td rowspan="2">КОЕФ</td>
		</tr>
		<tr class="tr_header">
			<td>Н</td>
			<td>З</td>
			<td>Общо</td>
		</tr><tr>
				<td>2</td>
				<td style="text-align: left; padding-left:5px;"><a href="player.php?id=34213"><b>Браян Камърън</b></a></td><td>37</td>
				<td>2/7</td>
				<td>2/8</td>
				<td>1/3</td>
				<td>1</td>
				<td>0</td>
				<td>1</td>
				<td>5</td>
				<td>3</td>
				<td>3</td>
				<td>1</td>
				<td>0</td>
				<td>11</td>
				<td>2</td></tr><tr>
				<td>11</td>
				<td style="text-align: left; padding-left:5px;"><a href="player.php?id=21472"><b>Ивайло Тонев</b></a></td><td>35</td>
				<td>4/7</td>
				<td>4/6</td>
				<td>9/11</td>
				<td>2</td>
				<td>5</td>
				<td>7</td>
				<td>1</td>
				<td>3</td>
				<td>4</td>
				<td>0</td>
				<td>0</td>
				<td>29</td>
				<td>26</td></tr><tr>
				<td>21</td>
				<td style="text-align: left; padding-left:5px;"><a href="player.php?id=34215"><b>Уилфрид Ликей</b></a></td><td>33</td>
				<td>5/8</td>
				<td>1/4</td>
				<td>2/3</td>
				<td>2</td>
				<td>3</td>
				<td>5</td>
				<td>6</td>
				<td>5</td>
				<td>1</td>
				<td>1</td>
				<td>0</td>
				<td>15</td>
				<td>19</td></tr><tr>
				<td>31</td>
				<td style="text-align: left; padding-left:5px;"><a href="player.php?id=20222"><b>Жюстен Коажда</b></a></td><td>27</td>
				<td>4/6</td>
				<td>0/0</td>
				<td>0/2</td>
				<td>2</td>
				<td>7</td>
				<td>9</td>
				<td>2</td>
				<td>2</td>
				<td>3</td>
				<td>1</td>
				<td>0</td>
				<td>8</td>
				<td>13</td></tr><tr>
				<td>22</td>
				<td style="text-align: left; padding-left:5px;"><a href="player.php?id=13006"><b>Росен Иванов</b></a></td><td>22</td>
				<td>0/0</td>
				<td>0/1</td>
				<td>1/2</td>
				<td>1</td>
				<td>2</td>
				<td>3</td>
				<td>1</td>
				<td>2</td>
				<td>2</td>
				<td>2</td>
				<td>0</td>
				<td>1</td>
				<td>3</td></tr><tr class="tr_header">
			<td rowspan="2">#</td>
			<td rowspan="2" style="text-align: left; padding-left:5px;">Резерви</td>
			<td rowspan="2">Мин</td>
			<td rowspan="2">2Т</td>
			<td rowspan="2">3Т</td>
			<td rowspan="2">НУ</td>
			<td colspan="3">БОРБИ</td>
			<td rowspan="2">АС</td>
			<td rowspan="2">ЛН</td>
			<td rowspan="2">ГР</td>
			<td rowspan="2">ОТ</td>
			<td rowspan="2">БЛ</td>
			<td rowspan="2">ТОЧ</td>
			<td rowspan="2">КОЕФ</td>
		</tr>
		<tr class="tr_header">
			<td>Н</td>
			<td>З</td>
			<td>Общо</td>
		</tr><tr>
				<td>3</td>
				<td style="text-align: left; padding-left:5px;"><a href="player.php?id=34214"><b>Зак Ролинс</b></a></td><td>19</td>
				<td>2/2</td>
				<td>2/5</td>
				<td>2/2</td>
				<td>0</td>
				<td>3</td>
				<td>3</td>
				<td>3</td>
				<td>1</td>
				<td>3</td>
				<td>0</td>
				<td>0</td>
				<td>12</td>
				<td>12</td></tr><tr>
				<td>17</td>
				<td style="text-align: left; padding-left:5px;"><a href="player.php?id=21468"><b>Емилиян Грудов</b></a></td><td>15</td>
				<td>1/2</td>
				<td>2/4</td>
				<td>0/0</td>
				<td>1</td>
				<td>2</td>
				<td>3</td>
				<td>1</td>
				<td>2</td>
				<td>1</td>
				<td>0</td>
				<td>0</td>
				<td>8</td>
				<td>8</td></tr><tr>
				<td>4</td>
				<td style="text-align: left; padding-left:5px;"><a href="player.php?id=32061"><b>Веселин Байчев</b></a></td><td>7</td>
				<td>0/0</td>
				<td>1/2</td>
				<td>0/0</td>
				<td>0</td>
				<td>0</td>
				<td>0</td>
				<td>0</td>
				<td>3</td>
				<td>0</td>
				<td>0</td>
				<td>0</td>
				<td>3</td>
				<td>2</td></tr><tr>
				<td>13</td>
				<td style="text-align: left; padding-left:5px;"><a href="player.php?id=32058"><b>Станислав Бънков</b></a></td><td>4</td>
				<td>0/0</td>
				<td>0/0</td>
				<td>0/0</td>
				<td>0</td>
				<td>0</td>
				<td>0</td>
				<td>0</td>
				<td>2</td>
				<td>0</td>
				<td>0</td>
				<td>0</td>
				<td>0</td>
				<td>0</td></tr><tr>
				<td>7</td>
				<td style="text-align: left; padding-left:5px;"><a href="player.php?id=32052"><b>Баръш Мустафа</b></a></td><td>1</td>
				<td>0/0</td>
				<td>0/0</td>
				<td>0/0</td>
				<td>0</td>
				<td>0</td>
				<td>0</td>
				<td>0</td>
				<td>0</td>
				<td>0</td>
				<td>0</td>
				<td>0</td>
				<td>0</td>
				<td>0</td></tr><tr>
				<td>10</td>
				<td style="text-align: left; padding-left:5px;"><a href="player.php?id=32057"><b>Окан Хасанов</b></a></td><td colspan="14">Не е играл</td></tr><tr>
				<td>88</td>
				<td style="text-align: left; padding-left:5px;"><a href="player.php?id=32881"><b>Владислав Станев</b></a></td><td colspan="14">Не е играл</td></tr><tr class="second-row" style="background-color: #E7EBFF;">
				<td></td>
				<td style="text-align: left; padding-left: 5px;">Общо</td>
				<td>200</td>
				<td>18/32</td>
				<td>12/30</td>
				<td>15/23</td>
				<td>9</td>
				<td>22</td>
				<td>31</td>
				<td>19</td>
				<td>23</td>
				<td>17</td>
				<td>5</td>
				<td>0</td>
				<td>87</td>
				<td>85</td>
			</tr><tr class="second-row" style="background-color: #E7EBFF;">
				<td></td>
				<td colspan="2"></td>
				<td>56.25%</td>
			<td>40%</td>
			<td>65.22%</td>
			<td colspan="10"></td>
		</tr><tr><td colspan="16"></td></tr><tr class="care_tit"><td colspan="16"><a href="team.php?id=13">Рилски спортист</a></td></tr><tr class="tr_header">
			<td rowspan="2">#</td>
			<td rowspan="2" style="text-align: left; padding-left:5px;">Стартова петица</td>
			<td rowspan="2">Мин</td>
			<td rowspan="2">2Т</td>
			<td rowspan="2">3Т</td>
			<td rowspan="2">НУ</td>
			<td colspan="3">БОРБИ</td>
			<td rowspan="2">АС</td>
			<td rowspan="2">ЛН</td>
			<td rowspan="2">ГР</td>
			<td rowspan="2">ОТ</td>
			<td rowspan="2">БЛ</td>
			<td rowspan="2">ТОЧ</td>
			<td rowspan="2">КОЕФ</td>
		</tr>
		<tr class="tr_header">
			<td>Н</td>
			<td>З</td>
			<td>Общо</td>
		</tr><tr>
				<td>8</td>
				<td style="text-align: left; padding-left:5px;"><a href="player.php?id=2038"><b>Чавдар Костов</b></a></td><td>28</td>
				<td>0/1</td>
				<td>1/2</td>
				<td>4/8</td>
				<td>0</td>
				<td>5</td>
				<td>5</td>
				<td>1</td>
				<td>2</td>
				<td>1</td>
				<td>0</td>
				<td>0</td>
				<td>7</td>
				<td>6</td></tr><tr>
				<td>32</td>
				<td style="text-align: left; padding-left:5px;"><a href="player.php?id=33577"><b>Джон Флорвиъс</b></a></td><td>25</td>
				<td>8/10</td>
				<td>0/0</td>
				<td>3/4</td>
				<td>4</td>
				<td>2</td>
				<td>6</td>
				<td>0</td>
				<td>4</td>
				<td>0</td>
				<td>0</td>
				<td>0</td>
				<td>19</td>
				<td>22</td></tr><tr>
				<td>12</td>
				<td style="text-align: left; padding-left:5px;"><a href="player.php?id=5060"><b>Александър Янев</b></a></td><td>24</td>
				<td>4/6</td>
				<td>1/5</td>
				<td>1/2</td>
				<td>2</td>
				<td>2</td>
				<td>4</td>
				<td>3</td>
				<td>5</td>
				<td>2</td>
				<td>1</td>
				<td>0</td>
				<td>12</td>
				<td>11</td></tr><tr>
				<td>0</td>
				<td style="text-align: left; padding-left:5px;"><a href="player.php?id=34225"><b>Майкълин Скот</b></a></td><td>23</td>
				<td>2/4</td>
				<td>1/4</td>
				<td>0/0</td>
				<td>0</td>
				<td>4</td>
				<td>4</td>
				<td>7</td>
				<td>3</td>
				<td>1</td>
				<td>3</td>
				<td>0</td>
				<td>7</td>
				<td>15</td></tr><tr>
				<td>3</td>
				<td style="text-align: left; padding-left:5px;"><a href="player.php?id=12507"><b>Крис Минков</b></a></td><td>17</td>
				<td>1/1</td>
				<td>2/3</td>
				<td>1/2</td>
				<td>0</td>
				<td>0</td>
				<td>0</td>
				<td>2</td>
				<td>1</td>
				<td>1</td>
				<td>1</td>
				<td>0</td>
				<td>9</td>
				<td>9</td></tr><tr class="tr_header">
			<td rowspan="2">#</td>
			<td rowspan="2" style="text-align: left; padding-left:5px;">Резерви</td>
			<td rowspan="2">Мин</td>
			<td rowspan="2">2Т</td>
			<td rowspan="2">3Т</td>
			<td rowspan="2">НУ</td>
			<td colspan="3">БОРБИ</td>
			<td rowspan="2">АС</td>
			<td rowspan="2">ЛН</td>
			<td rowspan="2">ГР</td>
			<td rowspan="2">ОТ</td>
			<td rowspan="2">БЛ</td>
			<td rowspan="2">ТОЧ</td>
			<td rowspan="2">КОЕФ</td>
		</tr>
		<tr class="tr_header">
			<td>Н</td>
			<td>З</td>
			<td>Общо</td>
		</tr><tr>
				<td>9</td>
				<td style="text-align: left; padding-left:5px;"><a href="player.php?id=8686"><b>Алекс Симеонов</b></a></td><td>26</td>
				<td>4/6</td>
				<td>2/3</td>
				<td>4/4</td>
				<td>3</td>
				<td>3</td>
				<td>6</td>
				<td>4</td>
				<td>2</td>
				<td>1</td>
				<td>3</td>
				<td>0</td>
				<td>18</td>
				<td>27</td></tr><tr>
				<td>5</td>
				<td style="text-align: left; padding-left:5px;"><a href="player.php?id=28048"><b>Алан Арнет</b></a></td><td>23</td>
				<td>5/9</td>
				<td>0/2</td>
				<td>1/4</td>
				<td>1</td>
				<td>4</td>
				<td>5</td>
				<td>4</td>
				<td>0</td>
				<td>0</td>
				<td>1</td>
				<td>0</td>
				<td>11</td>
				<td>12</td></tr><tr>
				<td>1</td>
				<td style="text-align: left; padding-left:5px;"><a href="player.php?id=12506"><b>Деян Карамфилов</b></a></td><td>17</td>
				<td>2/3</td>
				<td>1/1</td>
				<td>0/0</td>
				<td>1</td>
				<td>1</td>
				<td>2</td>
				<td>1</td>
				<td>4</td>
				<td>2</td>
				<td>0</td>
				<td>0</td>
				<td>7</td>
				<td>7</td></tr><tr>
				<td>23</td>
				<td style="text-align: left; padding-left:5px;"><a href="player.php?id=17595"><b>Мирослав Васов</b></a></td><td>13</td>
				<td>4/7</td>
				<td>0/4</td>
				<td>0/0</td>
				<td>0</td>
				<td>1</td>
				<td>1</td>
				<td>1</td>
				<td>1</td>
				<td>0</td>
				<td>0</td>
				<td>0</td>
				<td>8</td>
				<td>3</td></tr><tr>
				<td>35</td>
				<td style="text-align: left; padding-left:5px;"><a href="player.php?id=31120"><b>Христо Бъчков</b></a></td><td>4</td>
				<td>0/0</td>
				<td>0/0</td>
				<td>0/0</td>
				<td>0</td>
				<td>1</td>
				<td>1</td>
				<td>1</td>
				<td>0</td>
				<td>0</td>
				<td>0</td>
				<td>0</td>
				<td>0</td>
				<td>2</td></tr><tr>
				<td>11</td>
				<td style="text-align: left; padding-left:5px;"><a href="player.php?id=22849"><b>Георги Адирков</b></a></td><td colspan="14">Не е играл</td></tr><tr>
				<td>15</td>
				<td style="text-align: left; padding-left:5px;"><a href="player.php?id=27206"><b>Васил Попов</b></a></td><td colspan="14">Не е играл</td></tr><tr class="second-row" style="background-color: #E7EBFF;">
				<td></td>
				<td style="text-align: left; padding-left: 5px;">Общо</td>
				<td>200</td>
				<td>30/47</td>
				<td>8/24</td>
				<td>14/24</td>
				<td>11</td>
				<td>23</td>
				<td>34</td>
				<td>24</td>
				<td>22</td>
				<td>8</td>
				<td>9</td>
				<td>0</td>
				<td>98</td>
				<td>114</td>
			</tr><tr class="second-row" style="background-color: #E7EBFF;">
				<td></td>
				<td colspan="2"></td>
				<td>63.83%</td>
			<td>33.33%</td>
			<td>58.33%</td>
			<td colspan="10"></td>
		</tr></tbody></table>"""

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
