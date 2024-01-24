import json
import pandas as pd
import os
import logging
import sys
from sqlalchemy import create_engine, text, MetaData, Table, inspect

logger = logging.getLogger('exercise')

logging.basicConfig(level=logging.DEBUG)
database_connection = "{}+psycopg2://{}:{}@{}:{}/{}".format(
                                            os.getenv('DB_ENGINE', 'postgresql'),
                                            os.getenv('DB_USER', 'dev_user'),
                                            os.getenv('DB_PASSWORD', 'dev_password'),
                                            os.getenv('DB_HOST', 'host.docker.internal'),
                                            os.getenv('DB_PORT', '5432'),
                                            os.getenv('DB_NAME', 'dev_db')
                                        )
db_engine = create_engine(database_connection)


# load the transaction data from json file.
def load_transactional_data(file_path: str):
    f=open(file_path, 'r')
    data = json.load(f)
    transactions = data['transactions']
    trans_keys = ['customerId', 'customerName', 'transactionId', 'transactionDate', 'sourceDate', 'merchantId', 'categoryId', 'currency', 'amount', 'description']
    drop_element = []
    sorted_trans_keys = trans_keys.sort()

    # check whether the data contains unexpected fields
    for i in range(len(transactions)):
        if list(transactions[i].keys()).sort() != sorted_trans_keys:
            logger.warn("error occours: unexpected keys detected. ", i+1, 'th record in the list', transactions[i] )
            drop_element.append(i)
    
    # drop the record if it contains unexpected fields
    if len(drop_element) > 0:
        drop_element = drop_element.sort(reverse=True)
        for position in drop_element:
            transactions.drop(position)
    
    # convert the json data to dataframe
    output = pd.DataFrame(transactions, columns=transactions[0].keys())

    # drop the column contains PII data
    output = output.drop(columns=['customerName'])

    # convert the dataframe headers to lowercase
    output.columns = [x.lower() for x in output.columns]

    return output


# data quality check / data cleaning 
def transaction_data_quality_checks(dataframe: pd.DataFrame):
    # filter out the rows that currency other than EUR, GBP and USD.
    df = dataframe.loc[dataframe['currency'].isin(['EUR', 'GBP', 'USD'])]
    invalid_currency_records = dataframe.loc[~dataframe['currency'].isin(['EUR', 'GBP', 'USD'])]

    # filter out the rows with invalid transactiondate.
    column_headers = df.columns.values
    df["transactiondatecheck"] = pd.to_datetime(df["transactiondate"],errors='coerce')
    invalid_transactionDate_records = df[df['transactiondatecheck'].isnull()][column_headers]
    df = df.dropna(subset=['transactiondatecheck'])[column_headers]

    # sort the rows by sourcedate column in ascending order
    # remove duplicate transaction records based on transactionId and keep the last record
    # will keep the most recent transaction record
    df = df.sort_values(by=['sourcedate'], ascending=True)
    duplicate_rows = df[df.duplicated(subset=['transactionid'], keep=False) == True]
    duplicate_rows = duplicate_rows.sort_values('sourcedate')
    duplicate_rows = duplicate_rows.drop(duplicate_rows.groupby('transactionid').tail(1).index)
    df = df.drop_duplicates(subset=['transactionid'], keep='last')

    # join the rows that failed at each data quality check together and return as output
    rows_failed_quality_check = pd.concat([invalid_currency_records, invalid_transactionDate_records,duplicate_rows])

    return df, rows_failed_quality_check


# upsert function to load the data to the database
def dataframe_load(dataframe: pd.DataFrame, table_name: str, pks: list):
    # turn the dataframe columns name to lower case
    dataframe.columns = map(str.lower, dataframe.columns)

    # load the dataframe data to a temp table
    temp_table_name = 'temp_'+table_name
    dataframe.to_sql(temp_table_name, db_engine, index=False, if_exists='replace')

    # merge temp table into target table
    table_columns = str(dataframe.columns.values.tolist()).replace("[", "").replace("]", "").replace("\n", "").replace("'", "")

    # construct update set part of statement for all the columns other than primary key
    if table_name == 'error_log':
        upsert_statement = 'NOTHING  '
    else:
        upsert_statement = 'UPDATE SET '
        df_header_without_pk = dataframe.columns.values.tolist()
        for pk in pks:
            df_header_without_pk.remove(pk)
        for column_name in df_header_without_pk:
            upsert_statement+=' {} = EXCLUDED.{}, '.format(column_name, column_name)

    # execute and commit the query
    with db_engine.connect() as connection:
        connection.execute(text(
            """
            INSERT INTO {} ({}) 
            SELECT {} FROM {}
            ON CONFLICT ({}) DO {}
            """.format(table_name, table_columns, table_columns, temp_table_name, ','.join(pks), upsert_statement[:-2])
        ))
        connection.commit()


# update the dataframe column type to avoid data type mismatch error when upsert, insert, update the data to the postgres table
def column_type_update(dataframe: pd.DataFrame, table_name: str):

    # use inspect to access the table meta data
    insp = inspect(db_engine)
    columns_table = insp.get_columns(table_name)

    # change the pandas dataframe column datatype coording to the table schema
    for col in columns_table:
        if str(col['type']) == 'DATE':
            dataframe[col['name']] = pd.to_datetime(dataframe[col['name']], format='ISO8601')
        elif str(col['type']) == 'TIMESTAMP':
            dataframe[col['name']] = pd.to_datetime(dataframe[col['name']], format='ISO8601')
        elif str(col['type']) == 'INTEGER':
            dataframe[col['name']] = dataframe[col['name']].astype(int)
        elif str(col['type']) == 'DOUBLE PRECISION':
            dataframe[col['name']] = dataframe[col['name']].astype(float)
    
    return dataframe


# load data to database
def transaction_data_to_database(dataframe: pd.DataFrame):
    # split the dataframe into transacctions and customers tables

    customers = dataframe[['customerid', 'transactiondate']]
    customers = customers.sort_values(by=['transactiondate'], ascending=True)
    customers = customers.drop_duplicates(subset=['customerid'], keep='last')
    customers = column_type_update(customers, 'customers')

    transactions = dataframe[['transactionid', 'transactiondate', 'sourcedate', 'merchantid', 'categoryid', 'currency', 'amount',  'description', 'customerid']]
    transactions = column_type_update(transactions, 'transactions')

    # upsert the data into the database
    dataframe_load(customers, 'customers', ['customerid'])
    dataframe_load(transactions, 'transactions', ['transactionid', 'customerid'])


if __name__ == "__main__":

    # access the arguments
    arg = sys.argv
    file_path = str(arg[1])

    # load transaction data from file to memory
    logger.info('Script begin.')
    raw_data = load_transactional_data(file_path = file_path)
    logger.info('data loaded from file.')

    # data quality checks
    logger.info('data quality check begin.')
    quality_data, bad_data = transaction_data_quality_checks(raw_data)
    logger.info('data quality check completed.')

    # load the data passed the quality check to postgres database corresponding table
    logger.info('data loading to postgres database.')
    transaction_data_to_database(quality_data)
    logger.info('data successfully loaded to postgres database.')

    # load the data failed the quality check to postgres database error_log table
    logger.info('bad records loading to error_log table.')
    dataframe_load(bad_data, 'error_log', bad_data.columns.values.tolist())
    logger.info('data process completed for file')
    