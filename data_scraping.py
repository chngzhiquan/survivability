from bs4 import BeautifulSoup
import requests
import pandas as pd
from utils import copy_from

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

# Remove commas using string replacement
df['Revenue (USD millions)'] = df['Revenue (USD millions)'].str.replace(',', '')
df['Revenue growth'] = df['Revenue growth'].str.replace('%', '')
df['Employees'] = df['Employees'].str.replace(',', '')
df['Rank'] = df['Rank'].astype(int)
df['Name'] = df['Name'].astype(str)
df['Industry'] = df['Industry'].astype(str)
df['Revenue (USD millions)'] = df['Revenue (USD millions)'].astype(int)
df['Revenue growth'] = df['Revenue growth'].astype(float)
df['Employees'] = df['Employees'].astype(int)
df['Headquarters'] = df['Headquarters'].astype(str)
print(df.dtypes)

copy_from(df, 'scraped_data')