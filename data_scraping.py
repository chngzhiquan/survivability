# Practice scrapping

from bs4 import BeautifulSoup
import requests
import pandas as pd
from database_operations import create_connection, insert_df_to_mysql, fetch_all_rows

# Scraping table
url = 'https://en.wikipedia.org/wiki/List_of_largest_companies_in_the_United_States_by_revenue'
page = requests.get(url)
soup = BeautifulSoup(page.text, features="html.parser")

# Finding titles of table
table = soup.find_all('table')[0]
world_titles = table.find_all('th')
world_table_titles = [title.text.strip() for title in world_titles]

# Adding titles to Dataframe
df = pd.DataFrame(columns = world_table_titles)

# Finding data in table (tr is for rows and td is for data)
# Also appends data into Dataframe
column_data = table.find_all('tr')
for row in column_data[1:]:
    row_data = row.find_all('td')
    individual_row_data = [data.text.strip() for data in row_data]
    length = len(df)
    df.loc[length] = individual_row_data

# Example usage:
if __name__ == "__main__":
    connection = create_connection()
    if connection:
        # Example: Insert df
        insert_df_to_mysql(df, "scraped_data")

        # Example: Fetch data
        select_query = "SELECT * FROM scraped_data"
        rows = fetch_all_rows(connection, select_query)
        for row in rows:
            print(row)

        connection.close()



