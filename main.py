from datetime import datetime
import ohlc_utils
import json

# navigate to commons folder to import commonly used postgres queries
import sys
sys.path.append("../../common/")
import postgres_quries

# import configs from the common folder
with open("../../common/postgres_config.json") as f: postgres_config = json.load(f)
with open("../../common/tiingo_config.json") as f: tiingo_config = json.load(f)
with open("../../common/table_schemas.json") as f:
	schema = json.load(f)
	ohlc_table_schema = schema['ohlc']

validated_connection = postgres_quries.validate(
    postgres_config=postgres_config,
    table_schema=ohlc_table_schema
)

last_query_date = postgres_quries.query_postgres_last_record(
    connection=validated_connection,
    table_name=postgres_config['postgres_table']
)

query_return = ohlc_utils.query_tiingo_api(
	tiingo_config=tiingo_config,
	start_date=last_query_date,
	end_date=str(datetime.now()).split(".")[0]
)

data = ohlc_utils.transform_tiingo_query_results(query_return=query_return)

postgres_quries.insert_records(
    data=data,
    table_name=postgres_config['postgres_table'],
    connection=validated_connection
)

validated_connection.close()

print("done!")
