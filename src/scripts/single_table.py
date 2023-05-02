from sdv import Metadata
import pandas as pd
import awswrangler as wr
from sdv.tabular import CTGAN
import datetime
import sys
import boto3
import datetime
from functools import partial
# send logs to cloudwatch


def get_logwatch_client(region):
    return boto3.client('logs', region_name=region)


def send_logs_cloudwatch(client, log_group_name, log_stream_name, message):
    response = client.put_log_events(
        logGroupName=log_group_name,
        logStreamName=log_stream_name,
        logEvents=[
            {
                'timestamp': int(datetime.datetime.now().timestamp() * 1000),
                'message': message
            },
        ]
    )


# Load the data for a single table
def get_table(table_name, database_name):
    data = wr.athena.read_sql_query(
        f"SELECT * FROM {table_name}", database=database_name, ctas_approach=False, s3_output=get_table_location(table_name, database_name).rstrip("/") + "_athena")
    # lambda function to convert to SDV compatible types
    # SDV does not support Int64,float64 and string types.  int64 is converted to int and float64 is converted to float
    # One exception is if int64 has null values then it is converted to float. int64 with null values is not supported by pandas.
    # see - https://pandas.pydata.org/docs/user_guide/integer_na.html

    def convert(x):
        if x.dtype == 'string':
            return x.astype('object')
        elif x.dtype == 'float64':
            return x.astype('float')
        elif x.dtype == 'Int64':
            return x.astype('int') if not x.isna().any() else x.astype('float')
        elif x.dtype == 'boolean':
            return x.fillna(True).astype('bool')
        else:
            return x

    data = data.apply(convert)
    return data

# get s3 location of the table


def get_table_location(table_name, database_name):
    return wr.catalog.get_table_location(database=database_name, table=table_name)

# generate sythetic data of the same size as the original table


def generate_sythetic_data(data, send_status):
    send_status("Training model...")
    model = CTGAN()
    model.fit(data)
    send_status("Generating sythetic data using model...")
    return model.sample(data.shape[0])

# save sythetic data to s3


def save_sythetic_data(synthetic_data, table_name, database_name):
    table_location = get_table_location(table_name, database_name)
    # add sythetic word to location string
    synthetic_location = table_location.rstrip("/") + "_synthetic"
    # add sythetic work to the table name
    synthetic_table_name = table_name + "_synthetic"
    res = wr.s3.to_parquet(
        df=synthetic_data,
        path=synthetic_location,
        dataset=True,
        database=database_name,
        table=synthetic_table_name,
        mode="overwrite",
        description=f"Sythetic data for {table_name} generated on {datetime.datetime.now()}"
    )

# this function sets logging to cloudwatch


def set_cw_logging(aws_region, log_group_name, log_stream_name):
    boto3.setup_default_session(region_name=aws_region)
    log_client = get_logwatch_client(aws_region)
    send_status = partial(send_logs_cloudwatch, log_client,
                          log_group_name, log_stream_name)
    return send_status

# main  running function of script that takes table name and database name as arguments  and generates sythetic data


def main(aws_region, database_name, table_name, log_group_name, log_stream_name):

    boto3.setup_default_session(region_name=aws_region)
    # set up logging to cloudwatch
    log_client = get_logwatch_client(aws_region)
    send_status = set_cw_logging(
        aws_region, log_group_name, log_stream_name)

    send_status("Getting table data from Athena for table: " +
                table_name + " in database: " + database_name + "...")
    data = get_table(table_name, database_name)
    send_status("Generating sythetic data for table: " +
                table_name + " in database: " + database_name + "...")
    synthetic_data = generate_sythetic_data(data, send_status)
    send_status("Saving sythetic data to: " +
                get_table_location(table_name, database_name) + "_sythetic in database: " + database_name + " with table name: " + table_name + "_sythetic")
    save_sythetic_data(synthetic_data, table_name, database_name)
    send_status("done")


if __name__ == "__main__":
    if len(sys.argv) != 6:
        sys.stderr.write("Error: Invalid number of arguments. \n")
        sys.stderr.write(
            "Usage: python3 single_table.py <aws_region> <database_name> <log_group_name> <log_stream_name> \n")
        sys.exit(1)
    else:
        main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
