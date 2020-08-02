import sys
import requests
import json
import pandas as pd
from sqlalchemy import create_engine


def get_annual_financial_statement(ticker, statement_type, api_key):
    """
    This function aims to retrieve the selected annual financial statements of a selected company
    available in Financial Modeling Prep website. Depending on availability, all years will be retrieved.

    :param ticker: (str) listed company ticker symbol
    :param statement_type: (str) type of financial statement (income-, balance-sheet- or cash-flow-statement)
    :param api_key: (str) user's api key in Financial Modeling Prep
    :return: (pandas dataframe) annual financial statements of the selected company
    """

    annual_statement_response = requests.get("https://financialmodelingprep.com/api/v3/{}/{}?apikey={}"
                                             .format(statement_type, ticker, api_key))

    if annual_statement_response.status_code == 200:
        annual_statement = annual_statement_response.json()
    else:
        print('Get annual {} failed with {}'
              .format(statement_type, annual_statement_response.status_code))

    return pd.DataFrame(annual_statement)


def save_data(df, database_filename, table_name):
    """
    This function aims to save a dataset into a sqlite database with the provided name.

    :param df: (pandas dataframe) a dataframe that consists of disaster messages and corresponding categories.
    :param database_filename: (str) name of the sqlite database.
    :param table_name: (str) name of the SQL table.
    :return: none
    """

    engine = create_engine('sqlite:///{}'.format(database_filename))
    df.to_sql(table_name, engine, index=False, if_exists='replace')


def main():
    if len(sys.argv) == 3:

        database_filepath, fmp_api_key = sys.argv[1:]

        # Get Apple annual income statement
        appl_annual_income_statement = get_annual_financial_statement('AAPL', 'income-statement', fmp_api_key)

        # Get Apple annual balance sheet statement
        appl_annual_balance_sheet_statement = get_annual_financial_statement('AAPL', 'balance-sheet-statement',
                                                                             fmp_api_key)

        # Get Apple annual cash flow statement
        appl_annual_cash_flow_statement = get_annual_financial_statement('AAPL', 'cash-flow-statement', fmp_api_key)

        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        save_data(appl_annual_income_statement, database_filepath, 'AnnualIncomeStatementTable')

        print('Data saved to database!')

    else:
        print('Please provide the the file path and file name of the database as the first argument and '
              'the second argument is your Financial Modeling Prep API key.'
              '\n\nExample: python import_data.py ./data/FinancialStatements.db 01a2b3c789XYZ')


if __name__ == '__main__':
    main()
