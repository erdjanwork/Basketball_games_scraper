# Scraping and Storing Basketball Stats

This Python script demonstrates how to scrape data from an HTML table, process it using Pandas, and store it in a PostgreSQL database.

## Prerequisites

- Python 3.x
- Libraries:
  - pandas
  - BeautifulSoup (`bs4`)
  - psycopg2

## Setup

1. **Install the required dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

2. **Set up your PostgreSQL database:**
   - Create a database.
   - Replace `database_name`, `username`, `password`, `localhost`, `5432`, and `your_table_name` in the script with your database credentials and table name.

## Usage

1. **Replace `tab_script` variable with the HTML script containing the table you want to scrape.**

2. **Run the script:**

    ```bash
    match_scraper.py
    ```

## Description

- `match_scraper.py`: Python script that scrapes a specified HTML table, processes it using Pandas, and stores the data into a PostgreSQL database.

## How it works

1. The script uses BeautifulSoup to parse the HTML script and extract the table.
2. It defines column names and iterates through the table to extract player stats into a Pandas DataFrame.
3. Connects to a PostgreSQL database and inserts the data into a specified table.

## Note

- Ensure your HTML script's structure matches the parsing logic in the code.
- Handle the database credentials securely, preferably using environment variables.

## Contributing

Contributions are welcome! Feel free to open issues or submit pull requests.
