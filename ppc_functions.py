# want: set of functions that automate ETL process for two datasets 
# 1. week's transactions: pull, push and call
# 2. end of week portfolio: pull and push

import datetime 
import pyetrade
import pandas as pd 
import mysql.connector
from params import consumer, accountIDKey, path, connection_dict, tables, stored_procedure


def sunday_pg(tokens, sat2):
    '''Combines three main functions into a single one.'''
    ppc(tokens, sat2)
    pp2(tokens, sat2)
    pp3(tokens, sat2)


# 1. week's transactions

def ppc(tokens, sat2):
    '''Pulls weekly transactions from Etrade, prepares them for MySQL multi-record insertion, then inserts them into MySQL and calls a stored procedure.'''
    sat1 = sat2 - datetime.timedelta(weeks = 1)
    
    big_ls = pull_transactions(tokens, sat1, sat2)
    df = pdify(big_ls, sat1, sat2)
    ls_of_tuples = ls_of_tupify(df)
    push_n_call(ls_of_tuples, sat2)
    
    
def pull_transactions(tokens, sat1, sat2):
    '''Input: tokens and specified period -> output: extracted, combined and cleaned big list.
    Takes tokens and specified period and pulls transactions for period from Etrade, extracting the nested list from each json object (for multiple results) and adding it to a big list, which it then cleans by removing duplicate records.'''
    accounts = pyetrade.ETradeAccounts(
        consumer['key'],
        consumer['secret'],
        tokens['oauth_token'],
        tokens['oauth_token_secret'],
        dev = False
        )
    
    start_date = sat1.strftime('%m%d%Y')
    end_date = sat2.strftime('%m%d%Y')
    x = accounts.list_transactions(accountIDKey, resp_format = 'json', startDate = start_date, endDate = end_date)
    big_ls = x['TransactionListResponse']['Transaction']
    
    while 'next' in x['TransactionListResponse']:
        marker = x['TransactionListResponse']['marker']
        x = accounts.list_transactions(accountIDKey, resp_format = 'json', startDate = start_date, endDate = end_date, marker = marker)
        big_ls += x['TransactionListResponse']['Transaction']
        
    big_ls = [i for n, i in enumerate(big_ls)
                if i not in big_ls[n + 1 :]]
    
    return big_ls


def pdify(json_ob, sat1, sat2):
    '''Input: cleaned json object (big list) -> output: normalised, cleaned, converted and written-to-csv pandas dataframe.
    Takes (semi-structured) json object, normalises it, keeps only the desired columns, replaces missing values with the correct MySQL notation ('None'), converts the transactionDate column values from epoch time (UNIX timestamps) to standard date strings, writes the cleaned dataframe to a csv file in specified path, then returns the (thoroughly) cleaned dataframe.'''
    df = pd.json_normalize(json_ob)
    
    desired_cols = ['transactionDate', 'amount', 'description', 'transactionType', 'brokerage.product.securityType', 'brokerage.quantity', 'brokerage.price', 'brokerage.fee', 'brokerage.displaySymbol']
    df = df[desired_cols]
    
    df = df.where(pd.notnull(df), None) 
    
    df['transactionDate'] = df['transactionDate'].apply(lambda x: datetime.date.fromtimestamp((int(x) / 1000)).strftime('%m/%d/%y'))
    
    sat1_format = sat1.strftime('%m_%d_%Y')
    sat2_format = sat2.strftime('%m_%d_%Y')
    df.to_csv(f'{path}/transaction_data_{sat1_format}_{sat2_format}.csv', index = False)

    return df 


def ls_of_tupify(df):
    '''Input: cleaned pandas dataframe -> output: list of tuples ready for MySQL multi-record insertion.
    Simplifies existing method. Instead of checking that each record starts with a UNIX timestamp before inserting it as a tuple into an initially empty list, takes a dataframe and converts it to unnamed tuples (using itertuples()), then packages the tuples into a list. Returns the finished list of tuples.'''
    tuples = df.itertuples(index = False, name = None)
    ls_of_tuples = list(tuples)
    
    return ls_of_tuples


def push_n_call(ls_of_tuples, sat2):
    '''Input: list of tuples, second Saturday -> inserts list of tuples into MySQL table and calls stored procedure using Friday preceding second Saturday as parameter.
    Tries: make MySQL connection and cursor object, formulate insert query with specified table, executes it and checks success. Then, computes Friday preceding second Saturday and uses it as parameter to call stored procedure. Finally, commits changes. Can handle errors. Ends by closing cursor and connection objects.'''
    try:
        connection = mysql.connector.connect(**connection_dict)
        cursor = connection.cursor()
        
        insert_query = f'''insert into {tables[0]} (TransactionDate, Amount, Description, TransactionType, SecurityType, Quantity, Price, Commission, Symbol)
                            values (%s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        cursor.executemany(insert_query, ls_of_tuples)
        print(f'Success: {cursor.rowcount} records inserted into {tables[0]}.')
        
        fri = sat2 - datetime.timedelta(days = 1)
        fri_dt = datetime.datetime.strptime(str(fri), '%Y-%m-%d')
        arg = (fri_dt, )
        cursor.callproc(f'{stored_procedure}', arg)
        print(f'Success: {stored_procedure} was called.')
        
        connection.commit()
        
    except mysql.connector.Error as e:
        print(f'Failed to insert records into MySQL table or call stored procedure: {e}.')     
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print('MySQL connection is closed.')    
                   
                   
                   
                   
# 2. end of week portfolio
    # portfolio

def pp2(tokens, sat2):
    ''' Input: tokens, second Saturday -> pulls portfolio from Etrade and pushes it to MySQl.
    Pulls portfolio from Etrade, extracts the desired list nested inside, normalises it to a dataframe, cleans it and converts it to a list of tuples. Then, pushes the list of tuples to MySQL.'''
    ls = pull_portfolio(tokens)
    df = pdify2(ls, sat2)
    ls_of_tuples = ls_of_tupify(df)
    push2(ls_of_tuples)


def pull_portfolio(tokens):
    '''Input: tokens -> output: extracted (from big json object) portfolio list.
    Takes tokens, connects to Etrade, makes accounts object, pulls specified account's portfolio and extracts the nested list.'''
    accounts = pyetrade.ETradeAccounts(
        consumer['key'],
        consumer['secret'],
        tokens['oauth_token'],
        tokens['oauth_token_secret'],
        dev = False
        )

    x = accounts.get_account_portfolio(accountIDKey, resp_format = 'json')
    ls = x['PortfolioResponse']['AccountPortfolio'][0]['Position']
    # what if more than 50 results? implement analogous while loop.
    # relevant args: count and pageNumber
    if x['PortfolioResponse']['AccountPortfolio'][0]['totalPages'] > 1:
        print('*warning* There seem to be more than 50 results. Add some code (a while loop?) to page through the results using the page number. *warning*')
    
    return ls


def pdify2(json_ob, sat2):
    '''Takes (semi-structured) json object, normalises it, and keeps only the desired columns. Returns cleaned pandas dataframe.'''
    df = pd.json_normalize(json_ob)
    
    desired_cols = ['symbolDescription', 'adjPrevClose', 'Quick.change', 'Quick.changePct', 'quantity', 'pricePaid', 'daysGain', 'totalGain', 'totalGainPct', 'marketValue']
    df = df[desired_cols]
    
    df = add_timestamp(df, sat2) # integrates addition of timestamp column into pdify function
    
    return df
    
    
def add_timestamp(df, sat2):
    '''Input: pandas dataframe, second Saturday -> adds column with previous Friday's timestamp to dataframe.'''
    fri = sat2 - datetime.timedelta(days = 1)
    fri_dt = datetime.datetime.strptime(str(fri), '%Y-%m-%d')

    df['timestamp'] = fri_dt 
    
    return df
    

# same ls_of_tupify(): iterate df into unnamed tuples, then pass them to a list -> boom: a list of tuples.    
    
    
def to_mysql(insert_query, ls_of_tuples):
    '''Input: insert query, list of tuples -> multi-record MySQL insertion.
    Takes a specified insert query and appropriate Python object (a list of tuples) and inserts it into MySQL.'''
    try:
        connection = mysql.connector.connect(**connection_dict)
        cursor = connection.cursor()
        
        cursor.executemany(insert_query, ls_of_tuples)
        print(f'Success: {cursor.rowcount} records inserted into MySQL table.')
        connection.commit()
        
    except mysql.connector.Error as e:
        print(f'Failed to insert records into MySQL table: {e}.')
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print('MySQL connection is closed.')
    
    
def push2(ls_of_tuples):
    '''Input: list of tuples -> calls to_mysql() with appropriate query.
    Provides appropriate insert query and calls to_mysql().'''
    insert_query = f'''insert into {tables[1]} (Symbol, Last_Price, Change_, Change_Percent, Quantity, Price_Paid, Days_Gain, Total_Gain, Total_Gain_Percent, Market_Value, ImportDate) 
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
    to_mysql(insert_query, ls_of_tuples)
    
   
    # account balance
    
def pp3(tokens, sat2):
    '''Input: tokens and second Saturday -> pulls balance from Etrade and pushes it to MySQL.
    Pulls balance from Etrade, normalises it to a pandas dataframe (extraction unnecessary because comes as nested dictionaries and no list), cleans it, converts it to a list of a (singular) tuple, then pushes that list of (a) tuple to MySQL.'''
    json_ob = pull_balance(tokens) 
    df = pdify3(json_ob, sat2) 
    ls_of_tuple = ls_of_tupify(df)
    push3(ls_of_tuple)


def pull_balance(tokens):
    '''Input: tokens -> output: semi-structured json data of account balance.
    Pulls account balance from Etrade (as json object) and assigns it to variable. No need to extract a nested list because the json object is a dict-of-dicts, with no list.'''
    accounts = pyetrade.ETradeAccounts(
        consumer['key'],
        consumer['secret'],
        tokens['oauth_token'],
        tokens['oauth_token_secret'],
        dev = False
        )
    
    x = accounts.get_account_balance(accountIDKey, resp_format = 'json')
    # relevant arg: realTimeNAV (if true, fetches real time balance)
    
    return x
   
    
def pdify3(json_ob, sat2):
    '''Input: semi-structured json object, second Saturday -> output: cleaned and transformed pandas dataframe.
    Takes raw json object, normalises it to a pandas dataframe, keeps only those columns we desire, computes a third column and adds a timestamp column. Returns the transformed dataframe.'''
    df = pd.json_normalize(json_ob)
    
    desired_cols = ['BalanceResponse.Computed.cashAvailableForInvestment', 'BalanceResponse.Computed.RealTimeValues.totalAccountValue']
    df = df[desired_cols]
    
    df['investmentValue'] = df['BalanceResponse.Computed.RealTimeValues.totalAccountValue'] - df['BalanceResponse.Computed.cashAvailableForInvestment']
    
    df = add_timestamp(df, sat2)
    
    return df 


def push3(ls_of_tuple):
    '''Input: list of a tuple -> pushes it to MySQL in provided insert query.
    Takes a cooked list of a tuple and passes it to my_sql() with the appropriate insert query (which relies on a table specified in params.py).'''
    insert_query = f'''insert into {tables[2]} (Cash, Total, Investment_Value, ImportDate)
                        values (%s, %s, %s, %s) '''
    to_mysql(insert_query, ls_of_tuple)
    

# 3. other
    # find last Sat
def last_sat():
    from dateutil.relativedelta import relativedelta, SA

    today = datetime.date.today() # works if today is a Sat
    last_sat = today + relativedelta(weekday = SA(-1))
    return last_sat


    # loop through specified period
def loop(tokens, sat1, end):
    '''Loops through each week in the specified period, calling the ppc() functions for each one.'''
    sat2 = sat1 + datetime.timedelta(weeks = 1)
    
    while sat2 <= end:
        print(sat1.strftime('%m/%d/%y'), sat2.strftime('%m/%d/%y'))

        big_ls = pull_transactions(tokens, sat1, sat2) # ppc(tokens, sat2)
        df = pdify(big_ls, sat1, sat2)
        ls_of_tuples = ls_of_tupify(df)
        push_n_call(ls_of_tuples, sat2)        
        
        sat1 += datetime.timedelta(weeks = 1)
        sat2 += datetime.timedelta(weeks = 1) 