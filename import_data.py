import sys
import requests
import json
import pandas as pd
from sqlalchemy import create_engine


def get_annual_financial_statement(ticker, statement_type, api_key):
    """
    This function aims to retrieve the selected annual financial statements of a selected company
    available in Financial Modeling Prep website. Depending on availability, all years will be retrieved.

    :param ticker: listed company ticker symbol
    :type ticker: str
    :param statement_type: type of financial statement (income-, balance-sheet- or cash-flow-statement)
    :type statement_type: str
    :param api_key: user's api key in Financial Modeling Prep
    :type api_key: str
    :return: annual financial statements of the selected company
    :rtype: pd.DataFrame
    """

    annual_statement_response = requests.get("https://financialmodelingprep.com/api/v3/{}/{}?apikey={}"
                                             .format(statement_type, ticker, api_key))

    if annual_statement_response.status_code == 200:
        annual_statement = annual_statement_response.json()
    else:
        print('Get annual {} failed with {}'
              .format(statement_type, annual_statement_response.status_code))

    return pd.DataFrame(annual_statement)


def get_company_profile_data(ticker, api_key):
    """
    This function aims to retrieve the selected company's profile data from financialmodelingprep.com like market cap,
    industry, sector, price, beta etc.

    :param ticker: listed company ticker symbol
    :type ticker: str
    :param api_key: user's api key in financialmodelingprep.com
    :type api_key: str
    :return: profile data of the selected company
    :rtype: pd.DataFrame
    """

    company_profile_response = requests.get("https://financialmodelingprep.com/api/v3/profile/{}?apikey={}"
                                            .format(ticker, api_key))

    if company_profile_response.status_code == 200:
        company_profile = company_profile_response.json()
    else:
        raise ValueError('Get {} company profile data failed with {}'
              .format(ticker, company_profile_response.status_code))

    # if company profile is empty, there might be something wrong while retrieving data. Throw ValueError.
    if pd.DataFrame(company_profile).empty:
        raise ValueError('No data retrieved! Check API!')

    return pd.DataFrame(company_profile)


def collect_companies_data(tickers_list, api_key):
    """
    This function aims to collect all financial data from the income, balance sheet and cash flow statement of all the
    selected companies in the tickers_list and combine them into a pandas dataset.
    Companies' profile data will be collected here and will be returned as a second pandas dataset

    :param tickers_list: a list of tickers that should be collected
    :type tickers_list: list
    :param api_key: user's api key in financialmodelingprep.com
    :type api_key: str
    :return: a dataframe with financial data from the selected companies
    :rtype: pd.DataFrame
    :return: a dataframe with profile data from the selected companies
    :rtype: pd.DataFrame
    """

    companies_profile_data = pd.DataFrame()
    companies_financial_data = pd.DataFrame()

    try:
        for ticker in tickers_list:
            companies_profile_data = companies_profile_data.append(get_company_profile_data(ticker, api_key),
                                                                   ignore_index=True)
            income_statement = get_annual_financial_statement(ticker, 'income-statement', api_key)
            balance_sheet_statement = get_annual_financial_statement(ticker, 'balance-sheet-statement', api_key)
            cash_flow_statement = get_annual_financial_statement(ticker, 'cash-flow-statement', api_key)
            financial_data = pd.concat([income_statement, balance_sheet_statement, cash_flow_statement], axis=1)
            companies_financial_data = companies_financial_data.append(financial_data, ignore_index=True)
    except ValueError:
        print('Data collection interrupted! Continuing rest of the process..')

    # remove duplicated columns which are retrieved everytime a financial statement is requested
    companies_financial_data = companies_financial_data.loc[:, ~companies_financial_data.columns.duplicated()]

    return companies_financial_data, companies_profile_data


def save_data(df, database_filename, table_name):
    """
    This function aims to save a dataset into a sqlite database with the provided name.

    :param df: a dataframe that consists of disaster messages and corresponding categories
    :type df: pd.DataFrame
    :param database_filename: name of the sqlite database
    :type database_filename: str
    :param table_name: name of the SQL table
    :type table_name: str
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

        # Get Apple company profile data
        appl_profile_data = get_company_profile_data('AAPL', fmp_api_key)

        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        save_data(appl_annual_income_statement, database_filepath, 'AnnualIncomeStatementTable')

        print('Data saved to database!')

    else:
        print('Please provide the the file path and file name of the database as the first argument and '
              'the second argument is your Financial Modeling Prep API key.'
              '\n\nExample: python import_data.py ./data/FinancialStatements.db 01a2b3c789XYZ')


if __name__ == '__main__':
    main()
