import pandas as pd
import requests
from bs4 import BeautifulSoup

url = 'https://www.cbinsights.com/research-unicorn-companies'
page = requests.get(url)
soup = BeautifulSoup(page.text, 'html.parser')

#find table to scrape
table = soup.find_all('table')[0]

#get all columns
titles = table.find_all('th')
table_titles = [title.text.strip() for title in titles]

#put columns of table in dataframe
df = pd.DataFrame(columns=table_titles)

#extract all individual rows
column_data = table.find_all('tr')
for row in column_data[1:]:
    row_data = row.find_all('td')
    individual_row_data = [data.text.strip() for data in row_data]

    if len(individual_row_data) == len(table_titles):
        df.loc[len(df)] = individual_row_data

#save in csv format
df.to_csv('unicorn_startup_dataCB.csv', index=False)

print(df)

import sqlite3

conn = sqlite3.connect('unicornstartup.db')
cur = conn.cursor()


#load the data from the CSV file into a DataFrame
df = pd.read_csv('unicorn_startup_dataCB.csv')

#query to see how many companies sequoia capital is invested in
query = '''
SELECT COUNT(DISTINCT "Company")
FROM "table"
WHERE "select investors" LIKE '%Sequoia Capital%'
'''

result = cur.execute(query).fetchone()

print(f"Number of companies invested in by Sequoia Capital: {result[0]}")


#different approach by initializing investor name and seeing how many companies they are invested in
investor_name = "Sequoia Capital China"

query = f'''
SELECT COUNT(*)
FROM "table"
WHERE "select investors" LIKE '%{investor_name}%'
'''

result = cur.execute(query).fetchone()

print(f"{investor_name} appears {result[0]} times in the 'select investors' column.")


from collections import Counter


#define a function to split investors and handle NaN values
def split_investors(investors):
    if isinstance(investors, str):
        return [investor.strip() for investor in investors.split(',')]
    return []


#apply the split_investors function to each row and flatten the list
all_investors = [investor for sublist in df['Select Investors'].apply(split_investors) for investor in sublist]

#count the occurrences of each investor
investor_counter = Counter(all_investors)

#find the top 50 investors
top_investors = investor_counter.most_common(50)

print('Top 50 investors:')
for investor, count in top_investors:
    print(f"{investor} - Count: {count}")

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

#how many companies are in the financial services industry
industry_name = "Financial Services"

query = f'''
SELECT COUNT(*)
FROM "table"
WHERE "Industry" = ?
'''

result = cur.execute(query, (industry_name,)).fetchone()

print(f"Number of companies in {industry_name} industry: {result[0]}")


#how many companies are in the media and entertainment industry
industry_name = "Media & Entertainment"

query = f'''
SELECT COUNT(*)
FROM "table"
WHERE "Industry" = ?
'''

result = cur.execute(query, (industry_name,)).fetchone()

print(f"Number of companies in {industry_name} industry: {result[0]}")


#how many companies are in the Industrials industry
industry_name = "Industrials"

query = f'''
SELECT COUNT(*)
FROM "table"
WHERE "Industry" = ?
'''

result = cur.execute(query, (industry_name,)).fetchone()

print(f"Number of companies in {industry_name} industry: {result[0]}")


# how many companies are in the enterprise tech industry
industry_name = "Enterprise Tech"

query = f'''
SELECT COUNT(*)
FROM "table"
WHERE "Industry" = ?
'''

result = cur.execute(query, (industry_name,)).fetchone()

print(f"Number of companies in {industry_name} industry: {result[0]}")


# how many companies are in the healthcare and life sciences industry
industry_name = "Healthcare & Life Sciences"

query = f'''
SELECT COUNT(*)
FROM "table"
WHERE "Industry" = ?
'''
result = cur.execute(query, (industry_name,)).fetchone()
print(f"Number of companies in {industry_name} industry: {result[0]}")


# how many companies are in the insurance industry
industry_name = "Insurance"

query = f'''
SELECT COUNT(*)
FROM "table"
WHERE "Industry" = ?
'''

result = cur.execute(query, (industry_name,)).fetchone()
print(f"Number of companies in {industry_name} industry: {result[0]}")


#how many companies are in the consumer and retail industries:
industry_name = "Consumer & Retail"

query = f'''
SELECT COUNT(*)
FROM "table"
WHERE "Industry" = ?
'''

result = cur.execute(query, (industry_name,)).fetchone()
print(f"Number of companies in {industry_name} industry: {result[0]}")


#query to see the top 10 most common countries with unicorn startups
query = '''
SELECT "Country", COUNT(*) AS "Count"
FROM "table"
GROUP BY "Country"
ORDER BY "Count" DESC
LIMIT 10
'''

result = cur.execute(query).fetchall()

if result:
    print("Top 10 most common countries:")
    for country, count in result:
        print(f"{country} - Count: {count}")
else:
    print("No data found.")



conn.close()
