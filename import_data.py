import sys
import requests
import json
import pandas as pd
from sqlalchemy import create_engine


def alpha_get_annual_financial_statement(ticker, statement_type, api_key):
    """
    This function aims to retrieve the selected annual financial statements of a selected company
    available in alphavantage.co. Depending on availability, all years will be retrieved.

    :param ticker: listed company ticker
    :type ticker: str
    :param statement_type: type of financial statement (INCOME_STATEMENT, BALANCE_SHEET or CASH_FLOW)
    :type statement_type: str
    :param api_key: user's api key in alphavantage.co
    :type api_key: str
    :return: annual financial statements of the selected company
    :rtype: pd.DataFrame
    """

    annual_statement_response = requests.get("https://www.alphavantage.co/query?function={}&symbol={}&apikey={}"
                                             .format(statement_type, ticker, api_key))

    if annual_statement_response.status_code == 200:
        df_temp = pd.DataFrame({'Symbol': []})
        annual_statement = annual_statement_response.json().get('annualReports')
        df_annual_statement = pd.concat([df_temp, pd.DataFrame(annual_statement)], axis=1)
        df_annual_statement.iloc[:, 0] = annual_statement_response.json().get('symbol')
    else:
        print('Get annual {} failed with {}'
              .format(statement_type, annual_statement_response.status_code))

    return df_annual_statement


def alpha_get_company_profile_data(ticker, api_key):
    """
    This function aims to retrieve the selected company's profile data from alphavantage.co like market cap,
    industry, sector, beta etc.

    :param ticker: listed company ticker symbol
    :type ticker: str
    :param api_key: user's api key in alphavantage.co
    :type ticker: str
    :return: profile data of the selected company
    :rtype: pd.DataFrame
    """

    company_profile_response = requests.get("https://www.alphavantage.co/query?function=OVERVIEW&symbol={}&apikey={}"
                                            .format(ticker, api_key))

    if company_profile_response.status_code == 200:
        company_profile = [company_profile_response.json()]
    else:
        raise ValueError('Get {} company profile data failed with {}'
                         .format(ticker, company_profile_response.status_code))

    if pd.DataFrame(company_profile).empty:
        raise ValueError('No data retrieved! Check API!')

    return pd.DataFrame(company_profile)


def alpha_collect_companies_data(tickers_list, api_key):
    """
    This function aims to collect all financial data from the income, balance sheet and cash flow statement of all the
    selected companies in the tickers_list and combine them into a pandas dataset.
    Companies' profile data will be collected here and will be returned as a second pandas dataset

    :param tickers_list: a list of tickers that should be collected
    :type tickers_list: list
    :param api_key: user's api key in alphavantage.co
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
            print('        Getting {} data...'.format(ticker))
            # get current company's financial and profile data
            profile_data = alpha_get_company_profile_data(ticker, api_key)
            income_statement = alpha_get_annual_financial_statement(ticker, 'INCOME_STATEMENT', api_key)
            balance_sheet_statement = alpha_get_annual_financial_statement(ticker, 'BALANCE_SHEET', api_key)
            cash_flow_statement = alpha_get_annual_financial_statement(ticker, 'CASH_FLOW', api_key)

            # concatenate data from all 3 financial statements horizontally
            financial_data = pd.concat([income_statement, balance_sheet_statement, cash_flow_statement], axis=1)

            # append retrieved data of the current company to the final dataframes
            companies_financial_data = companies_financial_data.append(financial_data, ignore_index=True)
            companies_profile_data = companies_profile_data.append(profile_data, ignore_index=True)
            print('        {} data received!'.format(ticker))
    except ValueError:
        print('Data collection interrupted! Continuing rest of the process..')

    companies_financial_data = companies_financial_data.loc[:, ~companies_financial_data.columns.duplicated()]

    return companies_financial_data, companies_profile_data


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
            print('        Getting {} data...'.format(ticker))
            # get current company's financial and profile data
            profile_data = get_company_profile_data(ticker, api_key)
            income_statement = get_annual_financial_statement(ticker, 'income-statement', api_key)
            balance_sheet_statement = get_annual_financial_statement(ticker, 'balance-sheet-statement', api_key)
            cash_flow_statement = get_annual_financial_statement(ticker, 'cash-flow-statement', api_key)

            # concatenate data from all 3 financial statements horizontally
            financial_data = pd.concat([income_statement, balance_sheet_statement, cash_flow_statement], axis=1)

            # append retrieved data of the current company to the final dataframes
            companies_financial_data = companies_financial_data.append(financial_data, ignore_index=True)
            companies_profile_data = companies_profile_data.append(profile_data, ignore_index=True)
            print('        {} data received!'.format(ticker))
    except ValueError:
        print('    Data collection interrupted! Continuing rest of the process..')

    # remove duplicated columns which are retrieved everytime a financial statement is requested
    companies_financial_data = companies_financial_data.loc[:, ~companies_financial_data.columns.duplicated()]

    return companies_financial_data, companies_profile_data


def load_existing_data(database_filepath):
    """
    This functions aims to load and return the FinancialStatementTable and CompanyProfileTable saved in the database
    given in database_filepath as 2 separate dataframes. If the given database or the tables do not exist,
    this function will return empty dataframes.

    :param database_filepath: file path with the name of the sql database to access
    :type database_filepath: str
    :return: FinancialStatementsTable and CompanyProfile as dataframes, empty dataframes if they do not exist
    :rtype: pd.DataFrame
    """
    df_financial_statements = pd.DataFrame()
    df_profile = pd.DataFrame()


    try:
        engine = create_engine('sqlite:///{}'.format(database_filepath))
    except:
        print('    CompanyData.db does not exist')

    try:
        df_financial_statements = pd.read_sql_table('FinancialStatementsTable', engine)
    except:
        print('    FinancialStatementsTable does not exist in CompanyData.db')

    try:
        df_profile = pd.read_sql_table('CompanyProfileTable', engine)
    except:
        print('    CompanyProfileTable does not exist in CompanyData.db')

    return df_financial_statements, df_profile


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


def update_database(companies_list, database_filepath, api_key):
    """
    This function aims to update the given database with the financial and profile data of the selected companies
    in companies_list from alphavantage.co. If the companies' data already exist in the given database,
    the corresponding data will not be retrieved.

    :param companies_list: list of companies whose data should be retrieved from alphavantage.co
    :type companies_list: list
    :param database_filepath: path to the sqlite database
    :type database_filepath: str
    :param api_key: user's api key in alphavantage.co
    :type api_key: str
    :return: none
    """

    missing_companies_list = []

    # load companies' financial statements and profile data dataframes from the given database
    print('    Loading database...')
    df_financial_statements, df_profile = load_existing_data(database_filepath)

    # if no data exists in the database at all, collect data for all companies in companies_list
    if df_financial_statements.empty | df_profile.empty:
        print('    Tables are empty. Getting data from all companies...')
        df_financial_statements, df_profile = alpha_collect_companies_data(companies_list, api_key)

        # saving the updated financial statements and profile dataframes to the database
        save_data(df_financial_statements, database_filepath, 'FinancialStatementsTable')
        save_data(df_profile, database_filepath, 'CompanyProfileTable')
        print('    Tables updated!')
    # if some data exists in the database already, select only the missing ones
    else:
        # select the missing companies' symbols from companies_list.
        for symbol in companies_list:
            if symbol not in df_profile.Symbol.tolist():
                missing_companies_list.append(symbol)

        if not missing_companies_list:
            print('    No new company to be added to database. Tables are up-to-date!')
        else:
            # update the missing companies' financial statements and profile dataframes
            print('    Tables are out-of-date. Getting data from the missing companies...')
            new_financial_statements, new_profile = alpha_collect_companies_data(missing_companies_list, api_key)
            df_financial_statements = df_financial_statements.append(new_financial_statements, ignore_index=True)
            df_profile = df_profile.append(new_profile, ignore_index=True)

            # saving the updated financial statements and profile dataframes to the database
            save_data(df_financial_statements, database_filepath, 'FinancialStatementsTable')
            save_data(df_profile, database_filepath, 'CompanyProfileTable')
            print('    Tables updated!')


def main():
    if len(sys.argv) == 4:

        database_filepath, companies_symbol_filepath, api_key = sys.argv[1:]

        # load companies symbols list
        companies = pd.read_csv(companies_symbol_filepath, header=None).iloc[:, 0].unique()
        companies_list = companies.tolist()

        print('Updating database...\n    DATABASE: {}'.format(database_filepath))
        update_database(companies_list, database_filepath, api_key)

        print('Database updated!')

    else:
        print('Please provide the the file path and file name of the database as the first argument and '
              'the second argument is the file path to the csv file which stores the companies symbols that needs to be'
              ' analyzed. Third argument is your Alpha Vantage API key.'
              '\n\nExample: python import_data.py ./data/CompanyData.db ./data/sp-500-index.csv 01a2b3c789XYZ')


if __name__ == '__main__':
    main()
