import psycopg2

# establish postgres connection
def establish_postgres_connection(database, user, password, host, port):
    """
	Input:
		database | string | name of the database
		user | string | name of the user
		password | string | password to access the database
		host | string | name of the host
		port | string | port number
	Output:
		cursor | psycopg2 cursor | method of connecting to the database and executing queries
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

# generate queries
def generate_check_database_query(database):
    """
    Input:
        database | string | name of the database
    Output:
        query | string | query to execute
    """

    query = f"SELECT * FROM pg_database where datname = '{database}'"

    return query


def generate_check_table_query(table_name):
    """
    Input:
        table_name | string | name of the table_name
    Output:
        query | string | query to execute
    """

    query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}');"

    return query


def generate_last_record_query(column_of_interest, table_name):
    """
    Input:
        column_of_interest | string | name of the column of interest used to sort the table
        table_name | string | name of the table_name
    Output:
        query | string | query to execute
    """

    query = f"SELECT * FROM {table_name} ORDER by {column_of_interest} DESC LIMIT 1"

    return query


def generate_create_table_query(table_schema, table_name):
    """
    Input:
        table_schema | json | key value pair for all column names and types
        table_name | string | name of the table_name
    Output:
        query | string | query to execute
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
        query | string | query to execute
    """

    query = f"CREATE DATABASE {database}"

    return query


def generate_insert_query(table_name, column_names, values):
    """
    Input:
        table_name | string | name of the table_name
        column_names | list | list of column names
        values | list | list of row valuest to insert
    Output:
        query | string | query to execute
    """

    query = f"INSERT INTO {table_name} ({column_names}) VALUES ({values});"

    return query

# boolean returns
def check_database_exists(postgres_config):
    """
    Input:
        postgres_config | json | key value pairs needed to query the specified postgres
    Output:
        exists | boolean | status of the existence of the database
    """

    connection = establish_postgres_connection(
        database="postgres",
        user=postgres_config['postgres_user'],
        password=postgres_config['postgres_password'],
        host=postgres_config['postgres_host'],
        port=postgres_config['postgres_port']
    )

    query = generate_check_database_query(database=postgres_config['postgres_database'])

    try:
        connection.execute(query=query)
        data = connection.fetchall()
        if data[0][1] == postgres_config['postgres_database']:
            exists = True
        else:
            exists = False
    except:
        exists = False

    connection.close()

    return exists


def check_table_exists(connection, table_name):
    """
    Input:
        connection | psycopg2 cursor | method of connecting to the database and executing queries
        table_name | string | name of the table
    Output:
        exists | boolean | status of the existence of the database
    """

    query = generate_check_table_query(table_name=table_name)

    try:
        connection.execute(query=query)
        data = connection.fetchall()
        if data[0][0] == True:
            exists = True
        else:
            exists = False
    except:
        exists = False

    return exists

# none returns
def create_table(connection, table_name, table_schema):
    """
    Input:
        connection | psycopg2 cursor | method of connecting to the database and executing queries
        table_name | string | name of the table
        table_schema | json | key value pair for all column names and types
    output:
        None
    """

    query = generate_create_table_query(
        table_schema=table_schema,
        table_name=table_name,
    )

    connection.execute(query)

    return None


def create_database(postgres_config):
    """
    Input:
        postgres_config | json | key value pairs needed to query the specified postgres
    Output:
        None
    """

    connection = establish_postgres_connection(
        database="postgres",
        user=postgres_config['postgres_user'],
        password=postgres_config['postgres_password'],
        host=postgres_config['postgres_host'],
        port=postgres_config['postgres_port']
    )

    query = generate_create_database_query(database=postgres_config["postgres_database"])

    connection.execute(query)

    connection.close()

    return None

# misc
def validate(postgres_config, table_schema):
    """
    Input:
        postgres_config | json | key value pairs needed to query the specified postgres
        table_schema | json | key value pair for all column names and types
    Output:
        connection | psycopg2 cursor | method of connecting to the database and executing queries
    """

    db_status = check_database_exists(postgres_config=postgres_config)

    if db_status == True:

        print(f"Database {postgres_config['postgres_database']} exists")

        connection = establish_postgres_connection(
            database=postgres_config['postgres_database'],
            user=postgres_config['postgres_user'],
            password=postgres_config['postgres_password'],
            host=postgres_config['postgres_host'],
            port=postgres_config['postgres_port']
        )

        table_status = check_table_exists(
            connection=connection,
            table_name=postgres_config['postgres_table']
        )

        if table_status == True:
            print(f"Table {postgres_config['postgres_table']} exists")
            pass
        else:
            print("Table does not exist")
            print(f"Creating table: {postgres_config['postgres_table']}")
            create_table(
                connection=connection,
                table_schema=table_schema,
                table_name=postgres_config['postgres_table']
            )
            print(f"Created table: {postgres_config['postgres_database']}")

    else:
        print(f"Database {postgres_config['postgres_database']} does not exists")
        print(f"Table {postgres_config['postgres_table']} does not exists")

        create_database(postgres_config=postgres_config)
        print(f"Created database: {postgres_config['postgres_database']}")

        connection = establish_postgres_connection(
            database=postgres_config['postgres_database'],
            user=postgres_config['postgres_user'],
            password=postgres_config['postgres_password'],
            host=postgres_config['postgres_host'],
            port=postgres_config['postgres_port']
        )

        create_table(
            connection=connection,
            table_schema=table_schema,
            table_name=postgres_config['postgres_table']
        )
        print(f"Created table: {postgres_config['postgres_table']}")

    return connection


def insert_records(data, table_name, connection):
    """
    Input:
        data | pd.DataFrame | dataframe of records to insert
        table_name | string | name of the table
        connection | psycopg2 cursor | method of connecting to the database
    Output:
        None
    """
    columns = data.columns.tolist()
    column_names = ", ".join(columns)
    values = ", ".join(['%s'] * len(columns))

    query = generate_insert_query(table_name, column_names, values)

    data_to_insert = [tuple(row) for row in data.values]

    connection.executemany(query, data_to_insert)

    print(f"Inserted {len(data)} records into {table_name}")

    return None


def query_postgres_last_record(connection, table_name):
    """
    Input:
        connection | psycopg2 cursor | method of connecting to the database
        table_name | string | name of the table
    Output:
        laste_date | string | date of last record in the format "YYYY-MM-DD HH:MM:SS"
    """
    query = generate_last_record_query(
        column_of_interest="date",
        table_name=table_name
    )

    try:
        connection.execute(query)
        last_record = connection.fetchone()
        last_date = str(last_record[1])
    except:
        print(f"No records at table: {table_name}")
        last_date = "2018-01-01 00:00:00"
        print(f"will assume last date of: {last_date}")

    return last_date
