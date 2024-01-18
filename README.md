# Project: Extract, Transform, and Load GDP Data

## Used Libraries and Framework

- **requests:** Used for accessing information from the URL.
- **bs4:** Containing the BeautifulSoup function used for web scraping.
- **pandas:** Used for processing extracted data, storing it in required formats, and communicating with databases.
- **sqlite3:** Required to create a database server connection.
- **numpy:** Required for the mathematical rounding operation as required in the objectives.
- **datetime:** Containing the function `datetime` used for extracting the timestamp for logging purposes.


## Overview

In this project, I automated the extraction, transformation, and loading (ETL) process of GDP data. The goal is to create a script that extracts the list of all countries ordered by their GDPs in billion USDs, as reported by the International Monetary Fund (IMF). The script will be designed to handle the biannual updates released by the IMF.

## Features
### Data Extraction

Write a function to extract relevant information from the designated URL where the IMF logs GDP data.

### Data Transformation

Transform the available GDP information from 'Million USD' to 'Billion USD'.

### Data Loading

Load the transformed information into both a CSV file and a database file.

### Database Query

Run a query on the database to retrieve specific information as required.

### Logging

Log the progress of the code execution, including appropriate timestamps.
