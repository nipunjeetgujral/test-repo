from sqlalchemy import create_engine
import pandas as pd
import psycopg2


# Create connection methods to PostgresSQL
def establish_postgres_connection(database, user, password, host, port):
    """
	Input:
		database | string | name of the database
		user | string | name of the user
		password | string | password to access the database
		host | string | name of the host
		port | string | port number
	Output:
		cursor | psycopg2 cursor | method of connecting to the database
	"""

    try:
        conn = psycopg2.connect(
            dbname=database,
            user=user,
            password=password,
            host=host,
            port=port
        )

        conn.autocommit = True

        cursor = conn.cursor()

        return cursor

    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

def establish_postgres_engine(database, user, password, host, port):
    """
	Input:
		postgres_config | json | all configuration for PostgreSQL
	Output:
		engine | str | method of connecting to the database used with pandas
	"""

    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")

    return engine


# Generate queries
def generate_check_database_query(database):
    """
	Input:
		database | string | name of the database
	Output:
		query | string | SQL query that create a database
	"""

    query = f"SELECT * FROM pg_database where datname = '{database}'"

    return query

def generate_check_table_query(table_name):
    """
	Input:
		table_name | string | name of the database
	Output:
		query | string | SQL query that create a database
	"""

    query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}');"

    return query

def generate_last_record_query(column_of_interest, table_name):
    """
	Input:
		table_name | string | name of the table
		column_of_interest | string | column of interest used to subset the table
	Output:
		query | string | SQL query that create a database
	"""

    query = f"SELECT {column_of_interest} FROM {table_name} ORDER by {column_of_interest} DESC LIMIT 1"

    return query

def generate_create_table_query(table_schema, table_name):
    """
	Input:
		table_schema | json | key value pairing of column name and type
		table_name | string | name of the table
	Output:
		query | string | SQL query that create a table with a particular schema
	"""

    column_definitions = []
    for column_name, column_type in table_schema.items():
        column_definitions.append(f"  {column_name} {column_type}")

    query = f"CREATE TABLE IF NOT EXISTS public.{table_name} (\n"
    query += ",\n".join(column_definitions)
    query += "\n);"

    return query

def generate_create_database_query(database):
    """
	Input:
		database | string | name of the database
	Output:
	 	query | string | SQL query that create a database
	"""

    query = f"""CREATE DATABASE {database}"""

    return query


# Execute boolean return queries
def check_postgres_database_exists(database, postgres_config):

    """
    Input:
        database | string | name of the database
        postgres_config | json | method of connecting to the database
    Output:
        exists | bool | True if the database exists
    """

    exists = None

    engine = establish_postgres_engine(
        database="postgres",
        user=postgres_config['postgres_user'],
        password=postgres_config['postgres_password'],
        host=postgres_config['postgres_host'],
        port=postgres_config['postgres_port']
    )

    try:
        query = generate_check_database_query(database=database)
        df = pd.read_sql_query(sql=query, con=engine)
        if len(df) > 0:
            exists = True
        elif len(df) == 0:
            exists = False
    except:
        exists = False

    print(f"database exists: {exists}")

    return exists

def check_postgres_table_exists(table_name, postgres_config):

    """
    Input:
        table_name | string | name of the table
        postgres_config | json | method of connecting to the table
    Output:
        exits | bool | True if the table exists
    """

    exists = None

    engine = establish_postgres_engine(
        database=postgres_config['postgres_database'],
        user=postgres_config['postgres_user'],
        password=postgres_config['postgres_password'],
        host=postgres_config['postgres_host'],
        port=postgres_config['postgres_port']
    )

    try:
        query = generate_check_table_query(table_name=table_name)
        df = pd.read_sql_query(sql=query, con=engine)
        if len(df) > 0:
            exists = True
        elif len(df) > 0:
            exists = False
    except:
        exists = False

    print(f"table exists: {exists}")

    return exists


# Execute no return queries
def create_table(table_schema, table_name, postgres_config):
    """
	Input:
		table_schema | json | key value pairing of column name and type
		table_name | string | name of the table
		postgres_config | json | method of connecting to the database
	Output:
		None
	"""

    postgres_connection = establish_postgres_connection(
        database=postgres_config["postgres_database"],
        user=postgres_config['postgres_user'],
        password=postgres_config['postgres_password'],
        host=postgres_config['postgres_host'],
        port=postgres_config['postgres_port']
    )

    query = generate_create_table_query(table_schema, table_name)

    postgres_connection.execute(query)

    return None

def create_database(database, postgres_connection):
    """
	Input:
		database | string | name of the database
		postgres_connection | psycopg2 cursor | method of connecting to the database
	Output:
		None
	"""

    query = generate_create_database_query(database)

    postgres_connection.execute(query)

    return None

def validate_postgres_database_and_table(database, table_name, postgres_config, table_schemas):

    """
    Input:
        database | string | name of the database
        table_name | string | name of the table
        postgres_config | json |  method of connecting to the database/table
        table_schemas: | json | method of programmatically creating a table with a particular schema
    Output:
        None
    """


    base_connection = establish_postgres_connection(
        database="postgres",
        user=postgres_config['postgres_user'],
        password=postgres_config['postgres_password'],
        host=postgres_config['postgres_host'],
        port=postgres_config['postgres_port']
    )

    database_check = check_postgres_database_exists(
        database=database,
        postgres_config=postgres_config
    )

    table_check = check_postgres_table_exists(
        table_name=table_name,
        postgres_config=postgres_config,
    )

    if database_check == False:

        print(f"creating database: {postgres_config['postgres_database']}")
        print(f"creating table: {postgres_config['postgres_table']}")

        # create the database
        create_database(
            database=postgres_config['postgres_database'],
            postgres_connection=base_connection)

        # create the stable with a known schema
        create_table(
            table_schema=table_schemas['ohlc'],
            table_name=postgres_config['postgres_table'],
            postgres_config=postgres_config
        )

    elif database_check == True and table_check == False:

        print(f"database: {postgres_config['postgres_database']} exists")
        print(f"creating table: {postgres_config['postgres_table']}")

        # create the table with a known schema
        create_table(
            table_schema=table_schemas['ohlc'],
            table_name=postgres_config['postgres_table'],
            postgres_config=postgres_config
        )

    else:
        print(f"database: {postgres_config['postgres_database']} | status: exist")
        print(f"table: {postgres_config['postgres_table']} | status: exist")
        pass

    return None

def write_to_postgres(data, table_name, engine):
    """
	Input:
		data | pandas dataframe | psycopg2 cursor | method of connecting to the database
		table_name | string | name of the table
		engine | str | method of connecting to the database used with pandas
	Output:
		None
	"""

    if data is not None:
        print(f"Inserting {len(data)} records into {table_name}")
        data.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    else:
        print(f"No records inserted into {table_name}")

    return None


# Execute record return queries
def query_postgres_last_record(engine, table_name):

	query = generate_last_record_query(
		column_of_interest="date",
		table_name=table_name
	)

	try:
		last_record_df = pd.read_sql_query(sql=query, con=engine)
		last_record = str(last_record_df.iloc[0]['date'])
		last_date = last_record.split("+")[0]
	except:
		print(f"table: {table_name} has not records")
		last_date = "2018-01-01 00:00:00"
		print(f"will assume last date of: {last_date}")

	return last_date
