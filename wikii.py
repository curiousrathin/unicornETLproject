import pandas as pd
import requests
from bs4 import BeautifulSoup

url = 'https://en.wikipedia.org/wiki/List_of_unicorn_startup_companies'
page = requests.get(url)
soup = BeautifulSoup(page.text, 'html.parser')

table = soup.find_all('table')[3]

titles = table.find_all('th')
table_titles = [title.text.strip() for title in titles]

df = pd.DataFrame(columns=table_titles)

column_data = table.find_all('tr')
for row in column_data[1:]:
    row_data = row.find_all('td')
    individual_row_data = [data.text.strip() for data in row_data]

# Print the lengths for debugging
# print(f"Length of row data: {len(individual_row_data)}, Length of table titles: {len(table_titles)}")

    if len(individual_row_data) == len(table_titles):
        df.loc[len(df)] = individual_row_data

print(df)
df.to_csv('unicorn_startup_data.csv', index=False)

# Establish a connection to the SQLite database
import sqlite3
conn = sqlite3.connect('unicornn.db')

# Load the CSV data into a Pandas DataFrame
csv_file = 'unicorn_startup_data.csv'
df = pd.read_csv(csv_file)

# Create a table in the database
df.to_sql('table', conn, if_exists='replace', index=False)

# Commit the changes and close the connection
conn.commit()

# Create a cursor after committing the changes
cur = conn.cursor()

print("Database populated with CSV data.")

# Execute the query
for row in cur.execute('SELECT * FROM "table"'):
    print(row)


#query to see how many companies are in the cryptocurrency industry
query = '''
SELECT COUNT(*) 
FROM "table" 
WHERE industry = 'Cryptocurrency'
'''

cur.execute(query)
count = cur.fetchone()[0]

print(f"Number of entries in 'cryptocurrency' industry: {count}")


#query to see how many companies are in the software industry

query = '''
SELECT COUNT(*) 
FROM "table" 
WHERE industry = 'Software'
'''
# how many in software
cur.execute(query)
count = cur.fetchone()[0]

print(f"Number of entries in 'software' industry: {count}")


#query to see how many unique industries there are
query = '''
SELECT DISTINCT "Industry"
FROM "table"
'''

result = cur.execute(query).fetchall()

if result:
    print("Unique industries:")
    for industry in result:
        print(industry[0])
else:
    print("No data found.")


#query to find the top 20 most common industries
query = '''
SELECT "Industry", COUNT(*) AS "Count"
FROM "table"
GROUP BY "Industry"
ORDER BY "Count" DESC
LIMIT 20
'''

result = cur.execute(query).fetchall()

if result:
    print("Top 20 most common industries:")
    for industry, count in result:
        print(f"{industry} - Count: {count}")
else:
    print("No data found.")



conn.close()


