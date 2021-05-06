import sys
import requests
import time
import csv
import os
import pandas as pd
import numpy as np
import datetime as dt
from sqlalchemy import create_engine


def alpha_get_financial_statement(ticker, statement_type, api_key):
    """
    This function aims to retrieve the selected annual and quarterly financial statements of a selected company
    available in alphavantage.co. Depending on availability, all years will be retrieved.

    :param ticker: listed company ticker
    :type ticker: str
    :param statement_type: type of financial statement (INCOME_STATEMENT, BALANCE_SHEET or CASH_FLOW)
    :type statement_type: str
    :param api_key: user's api key in alphavantage.co
    :type api_key: str
    :return: annual and quarterly financial statements of the selected company
    :rtype: pd.DataFrame
    """

    financial_statement_response = requests.get("https://www.alphavantage.co/query?function={}&symbol={}&apikey={}"
                                             .format(statement_type, ticker, api_key))

    if (financial_statement_response.status_code == 200) & ('symbol' in financial_statement_response.json()):
        df_temp = pd.DataFrame({'Symbol': [], 'type': []})
        annual_statement = financial_statement_response.json().get('annualReports')
        df_annual_statement = pd.concat([df_temp, pd.DataFrame(annual_statement)], axis=1)
        df_annual_statement.iloc[:, 1] = 'annual'

        quarterly_statement = financial_statement_response.json().get('quarterlyReports')
        df_quarterly_statement = pd.concat([df_temp, pd.DataFrame(quarterly_statement)], axis=1)
        df_quarterly_statement.iloc[:, 1] = 'quarterly'

        df_financial_statement = pd.concat([df_annual_statement, df_quarterly_statement], ignore_index=True)
        df_financial_statement.iloc[:, 0] = financial_statement_response.json().get('symbol')
    else:
        print('Status Code:{}'.format(financial_statement_response.status_code))
        if "Information" in financial_statement_response.json():
            print(financial_statement_response.json().get('Information'))
        else:
            print('Something is wrong with the response!')

        raise ValueError('Get annual {} failed with {}'
              .format(statement_type, financial_statement_response.status_code))

    return df_financial_statement


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

    if (company_profile_response.status_code == 200) & ('Symbol' in company_profile_response.json()):
        company_profile = [company_profile_response.json()]
    else:
        print('Status Code:{}'.format(company_profile_response.status_code))
        if "Information" in company_profile_response.json():
            print(company_profile_response.json().get('Information'))
        else:
            print('Something is wrong with the response!')

        raise ValueError('Get {} company profile data failed with {}'
                         .format(ticker, company_profile_response.status_code))

    if pd.DataFrame(company_profile).empty:
        raise ValueError('No data retrieved! Check API!')

    return pd.DataFrame(company_profile)


def alpha_get_company_stock_prices(ticker, api_key):
    """
    This function aims to retrieve the full stock prices of a selected company
    available in alphavantage.co.

    :param ticker: listed company ticker
    :type ticker: str
    :param api_key: user's api key in alphavantage.co
    :type api_key: str
    :return: full stock prices of the selected company
    :rtype: pd.DataFrame
    """

    stock_price_response = requests.get("https://www.alphavantage.co/query?"
                                        "function=TIME_SERIES_DAILY_ADJUSTED&symbol={}&outputsize=full&apikey={}"
                                        .format(ticker, api_key))

    if (stock_price_response.status_code == 200) & ('Meta Data' in stock_price_response.json()):
        df_temp = pd.DataFrame({'Symbol': []})
        # get only the daily stock prices
        stock_price = stock_price_response.json().get('Time Series (Daily)')
        # concatenate the symbol column with the data
        df_stock_price = pd.concat([df_temp, pd.DataFrame(stock_price, dtype=float).T], axis=1)
        # assign stock ticker symbol to the Symbol column
        df_stock_price.iloc[:, 0] = stock_price_response.json().get('Meta Data').get('2. Symbol')
        # rename the index name to Date and assign it as a column
        df_stock_price.index.names = ['Date']
        df_stock_price.reset_index(level=0, inplace=True)
    else:
        print('Status Code:{}'.format(stock_price_response.status_code))
        if "Information" in stock_price_response.json():
            print(stock_price_response.json().get('Information'))
        else:
            print('Something is wrong with the response!')

        raise ValueError('Get {} stock prices failed with {}'
              .format(ticker, stock_price_response.status_code))

    return df_stock_price


def alpha_get_company_earnings(ticker, api_key):
    """
    This function aims to retrieve the annual and quarterly earnings of a selected company
    available in alphavantage.co.

    :param ticker: listed company ticker
    :type ticker: str
    :param api_key: user's api key in alphavantage.co
    :type api_key: str
    :return: annual and quarterly earnings of the selected company
    :rtype: pd.DataFrame
    """

    earnings_response = requests.get("https://www.alphavantage.co/query?"
                                        "function=EARNINGS&symbol={}&apikey={}"
                                        .format(ticker, api_key))

    if (earnings_response.status_code == 200) & ('symbol' in earnings_response.json()):
        df_temp = pd.DataFrame({'Symbol': [], 'type': []})
        annual_earnings = earnings_response.json().get('annualEarnings')
        df_annual_earnings = pd.concat([df_temp, pd.DataFrame(annual_earnings)], axis=1)
        df_annual_earnings.iloc[:, 1] = 'annual'

        quarterly_earnings = earnings_response.json().get('quarterlyEarnings')
        df_quarterly_earnings = pd.concat([df_temp, pd.DataFrame(quarterly_earnings)], axis=1)
        df_quarterly_earnings.iloc[:, 1] = 'quarterly'

        df_earnings = pd.concat([df_annual_earnings, df_quarterly_earnings], ignore_index=True)
        df_earnings.iloc[:, 0] = earnings_response.json().get('symbol')
    else:
        print('Status Code:{}'.format(earnings_response.status_code))
        if "Information" in earnings_response.json():
            print(earnings_response.json().get('Information'))
        else:
            print('Something is wrong with the response!')

        raise ValueError('Get {} earnings failed with {}'
              .format(ticker, earnings_response.status_code))

    return df_earnings


def alpha_collect_companies_data(tickers_list, api_key, option):
    """
    This function aims to collect data from all companies in the tickers_list. The type of data that can be collected
    is decided by the argument, option, where:
    0 = company's profile data only
    1 = company's annual and quarterly financial data only which are in income, balance sheet and cash flow statement
    2 = company's historical stock prices only
    3 = company's annual and quarterly earnings only
    This function returns 4 different data frames that correspond to 4 different types of collected data. Only the
    dataframe with the chosen data type will be filled with data, the rest will be empty dataframes.
    Furthermore, there is a 60 seconds delay for every 5 API requests due to the limitation of free API key from
    alphavantage.co

    :param tickers_list: a list of tickers that should be collected
    :type tickers_list: list
    :param api_key: user's api key in alphavantage.co
    :type api_key: str
    :param option: type of data to retrieve:
    0 = only profile data; 1 = only financial data; 2 = only stock prices; 3 = earnings only
    :type option: int
    :return: a dataframe with profile data from the selected companies
    :rtype: pd.DataFrame
    :return: a dataframe with annual and quarterly financial data from the selected companies
    :rtype: pd.DataFrame
    :return: a dataframe with historical stock prices from the selected companies
    :rtype: pd.DataFrame
    :return: a dataframe with annual and quarterly earnings from the selected companies
    :rtype: pd.DataFrame
    """

    companies_profile_data = pd.DataFrame()
    companies_financial_data = pd.DataFrame()
    companies_stock_prices = pd.DataFrame()
    companies_earnings = pd.DataFrame()
    api_request_count = 0
    api_request_count_limit = 3
    last_updated_symbols = []
    is_interrupted = 0

    try:
        for ticker in tickers_list:
            if option == 0:
                print('        Getting {} profile data...'.format(ticker))
                # set api_request_count_limit to 5
                api_request_count_limit = 5
                # get current company's profile data
                profile_data = alpha_get_company_profile_data(ticker, api_key)
                # increase api count
                api_request_count += 1

                # append retrieved data of the current company to the final dataframe
                companies_profile_data = companies_profile_data.append(profile_data, ignore_index=True)

                last_updated_symbols = companies_profile_data.Symbol.unique().tolist()

            elif option == 1:
                print('        Getting {} financial data...'.format(ticker))
                # set api_request_count_limit to 3
                api_request_count_limit = 3
                # get current company's financial data
                income_statement = alpha_get_financial_statement(ticker, 'INCOME_STATEMENT', api_key)
                balance_sheet_statement = alpha_get_financial_statement(ticker, 'BALANCE_SHEET', api_key)
                cash_flow_statement = alpha_get_financial_statement(ticker, 'CASH_FLOW', api_key)
                # increase api count
                api_request_count += 3

                # concatenate data from all 3 financial statements horizontally
                financial_data = pd.concat([income_statement, balance_sheet_statement, cash_flow_statement], axis=1)

                # append retrieved data of the current company to the final dataframe
                companies_financial_data = companies_financial_data.append(financial_data, ignore_index=True)

                last_updated_symbols = companies_financial_data.Symbol.unique().tolist()

            elif option == 2:
                print('        Getting {} historical stock prices...'.format(ticker))
                # set api_request_count_limit to 5
                api_request_count_limit = 5
                # get current company's historical stock prices
                stock_prices = alpha_get_company_stock_prices(ticker, api_key)
                # increase api count
                api_request_count += 1

                # append retrieved data of the current company to the final dataframe
                companies_stock_prices = companies_stock_prices.append(stock_prices)

                last_updated_symbols = companies_stock_prices.Symbol.unique().tolist()

            elif option == 3:
                print('        Getting {} earnings...'.format(ticker))
                # set api_request_count_limit to 5
                api_request_count_limit = 5
                # get current company's earnings
                earnings = alpha_get_company_earnings(ticker, api_key)
                # increase api count
                api_request_count += 1

                # append retrieved data of the current company to the final dataframe
                companies_earnings = companies_earnings.append(earnings, ignore_index=True)

                last_updated_symbols = companies_earnings.Symbol.unique().tolist()

            print('        {} data received!'.format(ticker))

            if api_request_count >= api_request_count_limit:
                print('Sleeping 60 seconds before requesting next company data...')
                time.sleep(60)
                # reset the api_request_count after sleeping for 60 sec
                api_request_count = 0

    except ValueError:
        print('Data collection interrupted! Continuing rest of the process..')
        is_interrupted = 1

    if not is_interrupted:
        last_updated_symbols = []
        if os.path.isfile("last_updated_symbols.csv"):
            os.remove("last_updated_symbols.csv")
    else:
        with open('last_updated_symbols.csv', 'w', newline='') as my_file:
            wr = csv.writer(my_file, quoting=csv.QUOTE_ALL)
            wr.writerow(last_updated_symbols)

    companies_financial_data = companies_financial_data.loc[:, ~companies_financial_data.columns.duplicated()]

    return companies_profile_data, companies_financial_data, companies_stock_prices, companies_earnings


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
    This functions aims to load and return the FinancialStatementTable, CompanyProfileTable, StockPricesTable
    and EarningsTable saved in the database given in database_filepath as separate dataframes. If the given database
    or the tables do not exist, this function will return empty dataframes.

    :param database_filepath: file path with the name of the sql database to access.
    :type database_filepath: str
    :return: FinancialStatementsTable, CompanyProfileTable, StockPricesTable, EarningsTable as dataframes,
    empty dataframes if they do not exist.
    :rtype: pd.DataFrame
    """
    df_financial_statements = pd.DataFrame()
    df_profile = pd.DataFrame()
    df_stock_prices = pd.DataFrame()
    df_earnings = pd.DataFrame()

    try:
        engine = create_engine('sqlite:///{}'.format(database_filepath))
    except:
        print('    CompanyData.db does not exist')

    try:
        df_financial_statements = pd.read_sql_table('FinancialStatementsTable', engine)
    except:
        print('FinancialStatementsTable does not exist in CompanyData.db!')

    try:
        df_profile = pd.read_sql_table('CompanyProfileTable', engine)
    except:
        print('CompanyProfileTable does not exist in CompanyData.db!')

    try:
        df_stock_prices = pd.read_sql_table('StockPricesTable', engine)
    except:
        print('StockPricesTable does not exist in CompanyData.db!')

    try:
        df_earnings = pd.read_sql_table('EarningsTable', engine)
    except:
        print('EarningsTable does not exist in CompanyData.db!')

    return df_financial_statements, df_profile, df_stock_prices, df_earnings


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


def convert_columns_to_numeric(df, exclude_cols):
    """
    This function aims to convert columns in a pandas dataframe from object type to numeric type, excluding the columns
    given in the exclude_cols list.

    :param df: pandas DataFrame to be processed.
    :type df: pd.DataFrame
    :param exclude_cols: A list of column names that should be excluded from convertung to numeric type
    :type exclude_cols: list
    :return: None
    """

    all_cols = df.columns.tolist()

    for col in exclude_cols:
        all_cols.remove(col)

    for col in all_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')


def replace_cell_string(df, old_str, new_str):
    """
    This function aims to replace a string in a cell of a pandas dataframe with a new string

    :param df: pandas DataFrame to be processed
    :type df: pd.DataFrame
    :param old_str: original string in a cell that should be replaced
    :type old_str: str
    :param new_str: new string to be assigned to replace the old string
    :type new_str: str
    :return: None
    """

    for col in df.columns.tolist():
        if df.dtypes[col] == 'O' and df[col].str.contains(old_str).any():
            df.loc[df[col] == old_str, col] = new_str


def convert_str_to_datetime(df, cols):
    """
    This function aims to convert all strings in the selected columns of a dataframe to datetime.

    :param df: pandas dataframe to be processed
    :type df: pd.DataFrame
    :param cols: a list of column names that should be converted to datetime
    :type cols: list
    :return: None
    """
    for col in cols:
        df[col] = df[col].apply(lambda x: dt.datetime.strptime(x, '%Y-%m-%d') if type(x) == str else x)


def clean_data(df_profile, df_financial_data, df_stock_prices, df_earnings):
    """
    This function aims to perform the data cleaning/preprocessing steps on df_financial_data, df_profile,
    df_stock_prices and df_earnings returned from alpha_collect_companies_data().

    :param df_profile: First returned dataframe from alpha_collect_companies_data()
    :type df_profile: pd.DataFrame
    :param df_financial_data: Second returned dataframe from alpha_collect_companies_data()
    :type df_financial_data: pd.DataFrame
    :param df_stock_prices: Third returned dataframe from alpha_collect_companies_data()
    :type df_stock_prices: pd.DataFrame
    :param df_earnings: Fourth returned dataframe from alpha_collect_companies_data()
    :type df_earnings: pd.DataFrame
    :return: None
    """

    if not df_profile.empty:
        # remove all rows with no symbol which are added due to unhandled error in API
        df_profile.drop(df_profile[df_profile['Symbol'].apply(lambda x: x is None)].index,
                        inplace=True)

        # convert string type None to [None] and then converts [None] to np.nan
        replace_cell_string(df_profile, 'None', np.nan)
        df_profile.replace([None], np.nan, inplace=True)

        convert_str_to_datetime(df_profile, ['DividendDate', 'ExDividendDate', 'LastSplitDate'])
    else:
        print('Empty CompanyProfileTable!')

    if not df_financial_data.empty:
        convert_columns_to_numeric(df_financial_data,
                                   ['Symbol', 'type', 'fiscalDateEnding', 'reportedCurrency'])

        # remove all rows with no symbol which are added due to unhandled error in API
        df_financial_data.drop(df_financial_data[df_financial_data['Symbol'].apply(lambda x: x is None)].index,
                               inplace=True)

        convert_str_to_datetime(df_financial_data, ['fiscalDateEnding'])
    else:
        print('Empty FinancialStatementsTable!')

    if not df_stock_prices.empty:
        # remove all rows with no symbol which are added due to unhandled error in API
        df_stock_prices.drop(df_stock_prices[df_stock_prices['Symbol'].apply(lambda x: x is None)].index,
                             inplace=True)

        convert_str_to_datetime(df_stock_prices, ['Date'])
    else:
        print('Empty StockPricesTable!')

    if not df_earnings.empty:
        convert_columns_to_numeric(df_earnings,
                                   ['Symbol', 'type', 'fiscalDateEnding', 'reportedDate'])

        # remove all rows with no symbol which are added due to unhandled error in API
        df_earnings.drop(df_earnings[df_earnings['Symbol'].apply(lambda x: x is None)].index,
                         inplace=True)

        convert_str_to_datetime(df_earnings, ['fiscalDateEnding', 'reportedDate'])
    else:
        print('Empty EarningsTable!')


def update_table(companies_list, df, api_key, data_option):
    """
    This function aims to update the selected table. If the companies' data already exist in the table,
    the corresponding data will not be retrieved.
    0 = CompanyProfileTable
    1 = FinancialStatementsTable
    2 = StockPricesTable
    3 = EarningsTable

    :param companies_list: list of companies whose data should be retrieved from alphavantage.co
    :type companies_list: list
    :param df: a pandas DataFrame that represents the selected table in the database.
    :type df: pd.DataFrame
    :param api_key: user's api key in alphavantage.co
    :type api_key: str
    :param data_option: an integer which specifies which table in the database should be updated.
    :type data_option: integer
    :return: none
    """

    missing_companies_list = []

    # if no data exists in the database at all, collect data for all companies in companies_list
    if df.empty:
        print('Table is empty. Getting data from all companies...')
        df = alpha_collect_companies_data(companies_list, api_key, data_option)[data_option]

    # if some data exist in the database already, select only the missing ones
    else:
        # select the missing companies' symbols from companies_list.
        for symbol in companies_list:
            if symbol not in df.Symbol.tolist():
                missing_companies_list.append(symbol)

        if not missing_companies_list:
            print('No new company to be added to database. Table is up-to-date!')
        else:
            # update the missing companies' data
            print('Table is out-of-date. Getting data...')
            new_data = alpha_collect_companies_data(missing_companies_list, api_key, data_option)[data_option]
            df = df.append(new_data, ignore_index=True)

    return df


def update_database(companies_list, database_filepath, api_key, data_options):
    """
    This function aims to update the tables in the given database of the selected companies in companies_list
    from alphavantage.co. User can select which table to update by passing a list of integers to the data_options.
    0 = CompanyProfileTable
    1 = FinancialStatementsTable
    2 = StockPricesTable
    3 = EarningsTable

    :param companies_list: list of companies whose data should be retrieved from alphavantage.co
    :type companies_list: list
    :param database_filepath: path to the sqlite database
    :type database_filepath: str
    :param api_key: user's api key in alphavantage.co
    :type api_key: str
    :param data_options: a list of integers which specifies which table in the database should be updated.
    :type data_options: list
    :return: none
    """

    valid_data_options = [0, 1, 2, 3]

    if set(data_options).issubset(set(valid_data_options)):
        # load companies' financial statements, profile, stock prices and earnings dataframes from the given database
        print('-> Loading database...')
        df_financial_statements, df_profile, df_stock_prices, df_earnings = load_existing_data(database_filepath)

        # Update the selected tables in database
        if 0 in data_options:
            print('--> Updating CompanyProfileTable...')
            df_profile = update_table(companies_list, df_profile, api_key, 0)

        if 1 in data_options:
            print('--> Updating FinancialStatementsTable...')
            df_financial_statements = update_table(companies_list, df_financial_statements, api_key, 1)

        if 2 in data_options:
            print('--> Updating StockPricesTable...')
            df_stock_prices = update_table(companies_list, df_stock_prices, api_key, 2)

        if 3 in data_options:
            print('--> Updating EarningsTable...')
            df_earnings = update_table(companies_list, df_earnings, api_key, 3)

        # preprocess data before saving the final dataframes to the database
        print('--> Cleaning Data...')
        clean_data(df_profile, df_financial_statements, df_stock_prices, df_earnings)

        # save the dataframes to the database
        print('--> Saving Data...')
        if (0 in data_options) and (not df_profile.empty):
            save_data(df_profile, database_filepath, 'CompanyProfileTable')

        if 1 in data_options and (not df_financial_statements.empty):
            save_data(df_financial_statements, database_filepath, 'FinancialStatementsTable')

        if 2 in data_options and (not df_stock_prices.empty):
            save_data(df_stock_prices, database_filepath, 'StockPricesTable')

        if 3 in data_options and (not df_earnings.empty):
            save_data(df_earnings, database_filepath, 'EarningsTable')

    else:
        print('Err: Invalid data_options in update_database()')


def main():
    if len(sys.argv) == 4:

        database_filepath, companies_symbol_filepath, api_key = sys.argv[1:]

        # load companies symbols list
        companies = pd.read_csv(companies_symbol_filepath, header=None).iloc[:, 0].unique()
        companies_list = companies.tolist()

        print('Updating database...\n    DATABASE: {}'.format(database_filepath))
        update_database(companies_list, database_filepath, api_key, [3])

        print('Database updated!')

    else:
        print('Please provide the the file path and file name of the database as the first argument and '
              'the second argument is the file path to the csv file which stores the companies symbols that needs to be'
              ' analyzed. Third argument is your Alpha Vantage API key.'
              '\n\nExample: python import_data.py ./data/CompanyData.db ./data/sp-500-tickers.csv 01a2b3c789XYZ')


if __name__ == '__main__':
    main()