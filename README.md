# Stock Analysis Project
This project aims to perform fundamental analysis on the selected companies and analyze how financial indicators relate to each company stock prices based on the financial data retrieved from alphavantage.co. The financial data are the stock prices and the data in the 3 financial statements (annual & quarterly) of the selected companies: income statement, balance sheet statement and cash flow statement. Currently, there are only 5 years of historical financial statements (2015-2019 and partly 2020) provided by alphavantage.co. 

Since a free API key from alphavantage.co is used here, there are limited API requests one can make per day (500 requests/day). Depending on the number of companies one wants to collect, the python script might need to be run once per day for a few days in order to collect data from all the companies as intended. As soon as an API request fails, due to the limitation of the free API key or error in the API requests such as unfound ticker symbol, the data collection will stop but this python script will continue data cleaning and save the collected data to the database. The next time this python script is run again, provided there is no error in the API request, the script will continue from the last imported ticker.

S&P 500 company tickers are used here as a baseline and the list of these tickers is downloaded from barchart.com.

## Getting Started

To get a copy of this project in the local project, please execute:
```
git clone https://github.com/ooisiang/stock-analysis.git
```

## Prerequisites

A requirements.txt file is provided in this repo to prepare the environment needed to run the code.
Please install the packages by executing:
```
pip install -r requirements.txt
```

## Instructions:

1. Run the following commands in the project's root directory to set up your database.

    - To python script that cleans data and stores in database, run:
        `python import_data.py ./data/CompanyData.db ./data/sp-500-tickers.csv <alphavantage.co free API Key>`

## Analysis

1. BasicEPS_vs_StockPrice.ipynb
	- This notebook shows the relationship of the companies' quarter basic EPS and their respective stock prices on the 45th and 90th day after quarter fiscal ending date.

## Authors

* **Ooi Siang Tan** - *Initial work* - [ooisiang](https://github.com/ooisiang)

## External Links

- Here is the [link](https://www.alphavantage.co/support/#api-key) to get a free API key from alphavantage.co.
- Here is the [link](https://www.alphavantage.co/documentation/) to the alphavantage.co API documentation.
- Here is the [link](https://www.barchart.com/stocks/indices/sp/sp500) to download all company tickers in S&P 500.