import sys
import os
import pandas as pd
import numpy as np
import datetime as dt

sys.path.append(os.path.abspath(os.path.join('..')))
from import_data import load_existing_data, save_data


def get_basic_eps(df_financial_statements, report_type):
    """
    This function aims to calculate the basic earning per share for each entry in the df_financial_statements.

    :param df_financial_statements: pandas dataframe with data imported from FinancialStatementsTable saved
    in import_data.py
    :type df_financial_statements: pd.DataFrame
    :param report_type: which type of financial report to use, 'annual' or 'quarterly' reports.
    :type report_type: str
    :return: pandas dataframe that consists of the earning per share of each entry in df_financial_statements
    :rtype: pd.DataFrame
    """

    # eps should be multiplied by 4 when it is based on the quarterly earnings, 1 if annual.
    if report_type == 'annual':
        eps_factor = 1.0
    elif report_type == 'quarterly':
        eps_factor = 4.0
    else:
        eps_factor = 0.0

    df_eps = (df_financial_statements.netIncomeApplicableToCommonShares /
              df_financial_statements.commonStockSharesOutstanding) * eps_factor

    return df_eps


def get_stock_prices(financial_ratios, stock_prices, stock_price_type, date_offset):
    """
    This function aims to get the stock price of a given date in each entry in financial_ratios.
    The dates are the values in the given column, date_col in financial_ratios.
    The stock price can be found by matching the date and ticker symbol in the stock_prices.
    User can also add an offset to the date to achieve a stock price on a different day based on the date in date_col.

    :param financial_ratios: the dataframe, df_financial_ratios constructed in calc_financial_ratios
    :type financial_ratios: pd.DataFrame
    :param stock_prices: the dataframe load from StockPricesTable in the database
    :type stock_prices: pd.DataFrame
    :param stock_price_type: type of stock price to retrieve (col name in stock_prices)
    :type stock_price_type: str
    :param date_offset: number of days to offset
    :type date_offset: int
    :return: a list of stock prices with the same number of columns as in financial_ratios
    :rtype: list
    """

    stock_price_list = []

    # loop through all entries in financial_ratios to find the matching stock price
    for idx in range(financial_ratios.shape[0]):
        symbol = financial_ratios.iloc[idx].Symbol
        # get the stock price on the fiscal ending date in the entry or a date offset set by the user
        date = financial_ratios.iloc[idx].fiscalDateEnding + dt.timedelta(days=date_offset)
        stock_price = stock_prices[(stock_prices.Symbol == symbol)
                                   & (stock_prices.Date == date)][stock_price_type].values

        # if no stock price is found on this date, try one day later and keep adding one day if not found.
        while_loop_count = 0
        while stock_price.size == 0:
            while_loop_count += 1
            date += dt.timedelta(days=1)
            stock_price = stock_prices[(stock_prices.Symbol == symbol)
                                       & (stock_prices.Date == date)][stock_price_type].values

            # if no stock price is available 5 days later, stop finding and set it to np.nan
            if while_loop_count > 4:
                stock_price = [np.nan]
                break

        # append the found stock price to the list
        stock_price_list.append(stock_price[0])

    return stock_price_list


def calc_financial_ratios(df_financial_statements, df_profile, df_stock_prices, report_type):
    """
    This function aims to calculate the financial ratios for the selected entries (annual or quarterly) in
    df_financial_statements and return a dataframe that consists of all the computed financial ratios.

    :param df_financial_statements: pandas dataframe with data imported from FinancialStatementsTable saved
    in import_data.py
    :param df_profile: pandas dataframe with data imported from CompanyProfileTable saved in import_data.py
    :param df_stock_prices: pandas dataframe with data imported from StockPricesTable saved in import_data.py
    :param report_type:
    :return:
    """

    # construct a base dataframe which consists of the the symbol and fiscal ending date
    df_selected_financial_statements = df_financial_statements[df_financial_statements.type == report_type]
    df_financial_ratios = df_selected_financial_statements[['Symbol', 'fiscalDateEnding']]

    # get stock prices based on the 'Symbol' and 'fiscalDateEnding' in df_financial_ratios and append to it
    print('-> Getting stock prices...')
    selected_stock_prices = get_stock_prices(df_financial_ratios, df_stock_prices, '4. close', 0)
    df_financial_ratios.loc[:, 'StockPriceOnFiscalDateEnding'] = selected_stock_prices
    print('-> Stock prices retrieved!')

    # Calculate basic eps and concatenate df_eps to the final dataframe, df_financial_ratios
    print('-> Getting EPS...')
    df_eps = get_basic_eps(df_selected_financial_statements, report_type)
    df_financial_ratios = pd.concat([df_financial_ratios, pd.DataFrame(df_eps, columns=['EPS'])], axis=1)
    print('-> EPS calculated!')

    return df_financial_ratios


def main():
    if len(sys.argv) == 4:

        financial_data_database_filepath, financial_ratios_database_filepath, report_type = sys.argv[1:]

        # load financial data from CompanyData.db
        print('Loading database...\n->DATABASE: {}'.format(financial_data_database_filepath))
        financial_statements, profile, stock_prices = load_existing_data(financial_data_database_filepath)

        print('Calculating financial ratios...')
        financial_ratios = calc_financial_ratios(financial_statements, profile, stock_prices, report_type)
        print('Done!')

        print('Saving data...\n->DATABASE: {}'.format(financial_ratios_database_filepath))
        save_data(financial_ratios, financial_ratios_database_filepath, 'FinancialRatiosTable')
        print('Database updated!')

    else:
        print('Please provide the file path and file name of the database (financial data) to be loaded as the '
              'first argument and the file path and file name of the database (financial ratios) to be saved as '
              'the second argument. The third argument would be the report type used to calculate the '
              'financial ratios, either annual or quarterly.'
              '\n\nExample: python calc_financial_ratios.py ./data/CompanyData.db ./data/FinancialRatios.db'
              ' quarterly')


if __name__ == '__main__':
    main()
