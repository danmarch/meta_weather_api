import sqlite3
import requests


class MetaWeatherApi:
    """
    This class is responsible for updating the weather csv for a new day.

    params
    ------
    weather_date: datetime.date
        The day to update the csv with.
    verify: bool
        Whether or not to use an SSL certificate for encryption.
    """

    API_ENDPOINT   = "https://metaweather.com/api/location/2487956/{}/{}/{}/"
    DB_TABLE_NAME  = "weather_info.db"
    DB_TABLE_INDEX = "applicable_date"

    COLUMNS_AND_TYPES = [
        ("id", "int"),
        ("applicable_date", "string"),
        ("weather_state_name", "string"),
        ("weather_state_abbr", "string"),
        ("wind_speed", "float"),
        ("wind_direction", "float"),
        ("wind_direction_compass", "string"),
        ("min_temp", "int"),
        ("max_temp", "int"),
        ("the_temp", "int"),
        ("air_pressure", "float"),
        ("humidity", "float"),
        ("visibility", "float"),
        ("predictability", "integer"),
        ("created", "string")
    ]

    def __init__(self, weather_date, verify=False):
        self._verify       = verify
        self._weather_date = weather_date

    def _get_conn_and_cursor(self):
        """Connects to sqlite DB and returns its connection and cursor"""
        connection = sqlite3.connect(self.DB_TABLE_NAME)
        db_cursor  = connection.cursor()

        return connection, db_cursor

    def _get_columns_and_types(self):
        """Makes list of columns and types for creating the DB table"""
        columns_and_types = ""

        for column, type in self.COLUMNS_AND_TYPES:
            columns_and_types += "{} {}, ".format(column, type)

        # Get rid of the last comma and space
        columns_and_types = columns_and_types[:-2]

        return columns_and_types

    def _table_does_not_exist(self, db_cursor):
        """Checks if weather_info table exists"""
        try:
            items = db_cursor.execute("SELECT * FROM weather_info limit 1")
            return False
        except sqlite3.OperationalError:
            return True

    def _create_table(self, db_cursor):
        """Makes the weather_info table"""
        columns_and_types = self._get_columns_and_types()
        statement = "CREATE TABLE weather_info ({})".format(columns_and_types)

        db_cursor.execute(statement)

        # Create index on the applicable_date field
        index     = self.DB_TABLE_INDEX
        statement = "CREATE INDEX index ON weather_info ({})".format(index)

        db_cursor.execute(statement)

    def _get_consolidated_weather_hashes(self):
        """Invokes API_ENDPOINT with the correct date and returns results"""
        url = self.API_ENDPOINT.format(self._weather_date.year,
                                       self._weather_date.month,
                                       self._weather_date.day)

        data_hashes = requests.get(url, verify=self._verify).json()

        return data_hashes

    def _get_insert_values(self, data_hash):
        """Makes the inserted values string for the insert statement"""
        insert_string = ""

        for key, key_type in self.COLUMNS_AND_TYPES:
            if key_type == "string" or data_hash[key] == None:
                # Put single quotes around strings
                insert_string += "'{}',".format(data_hash[key])
            else:
                insert_string += "{},".format(data_hash[key])

        # Get rid of the final comma
        insert_string = insert_string[:-1]

        return insert_string

    def _add_hash_to_table(self, db_cursor, data_hash):
        """Adds data_hash to weather_info"""
        insert_str = self._get_insert_values(data_hash)
        statement  = "INSERT INTO weather_info VALUES ({})".format(insert_str)

        db_cursor.execute(statement)

    def _commit_and_close(self, conn):
        """Commits connection changes and closes"""
        conn.commit()
        conn.close()

    def _display_rows(self, conn):
        """Displays the rows in the table"""
        for row in conn.execute("SELECT * FROM weather_info"):
            print(row)

    def drop_table(self, db_cursor):
        """Drops the table pointed to by the cursor"""
        db_cursor.execute("DROP TABLE weather_info")

    def update_and_display_csv(self):
        conn, db_cursor = self._get_conn_and_cursor()
        data_hashes     = self._get_consolidated_weather_hashes()

        if self._table_does_not_exist(db_cursor):
            # Make the weather_info table if it does not exist
            self._create_table(db_cursor)

        for data_hash in data_hashes:
            # Add all of the hashes to weather_info
            self._add_hash_to_table(db_cursor, data_hash)

        # Display the rows
        self._display_rows(conn)

        # Commit all changes and close the connection
        self._commit_and_close(conn)
