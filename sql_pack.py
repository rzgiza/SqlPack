import pandas as pd
import mysql.connector
import json
import sys


class MsqlConnection():
    """Create class to wrap mysql.connector and support with statements."""
    def __init__(self, host, port, user, password, use_pure = True):
        """Initialize parameters to MySQL connection."""
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.use_pure = use_pure
        
    def __enter__(self):
        """Open MySQL connection (conn) and creates cursor (curs) using with."""
        self.conn = mysql.connector.connect(
            host = self.host,
            port = self.port, 
            user = self.user, 
            password = self.password, 
            use_pure = self.use_pure
            )
        self.curs = self.conn.cursor()
        print("Connection Opened\n")
        return self.conn, self.curs
    
    def __exit__(self, exc_type, exc_value, tb):
        """Close MySQL connection using with."""
        self.conn.close()
        print("\nConnection Closed")
        
        
class MsqlPack():
    """Handles MsqlConnection using with statements to retrieve information
    and data queries from the MySQL connection.""" 
    def __init__(self, msql_connection=None):
        """Pass MsqlConnection object. All connections are handled using 
        with statements."""
        if msql_connection is None:
            print("Warning: No MsqlConnection has been passed. Only the save "\
                  "and load methods are functional.")
        else:
            self.msql_connection = msql_connection
        
    def all_dbs(self):
        """Retrieve the names of all the databases in the MySQL connection.
        -> Pandas data frame."""
        with self.msql_connection as conn_curs:
            return pd.read_sql("show databases;", conn_curs[0])
        
    def show_tables(self, data_base, conn_curs=None):
        """Show tables in given database. conn_curs variable allows passing
        an active connection, cursor tuple to avoid opening new one. 
        -> Pandas data frame of the table names."""
        show_query = "show tables in " + data_base + ";"
        if conn_curs is None:
            with self.msql_connection as conn_curs:
                return pd.read_sql(show_query, conn_curs[0])
        else:
            return pd.read_sql(show_query, conn_curs[0])
        
    def show_colnames(self, data_base, table):
        """Show column names of table in given database. -> Pandas data frame
        of the column names."""
        data_base = "'" + data_base + "'"
        table = "'" + table + "'"
        colnames = self.custom(
            "information_schema", 
            colnames=
                """
                SELECT COLUMN_NAME
                FROM COLUMNS
                WHERE TABLE_SCHEMA = """ + data_base + " AND TABLE_NAME = " + table)
        return colnames['colnames']

    def query_tables(self, data_base, *tables):
        """Query tables by name from a database. -> dictionary of pandas data
        frames {table_name: pd}. If no tables are passed default is to query 
        all tables in database."""
        with self.msql_connection as conn_curs:
            conn_curs[1].execute("USE " + data_base + ";") 
            data_dict = {}
            if tables: 
                tables = tables
            else:
                tables = self.show_tables(data_base, conn_curs)
                tables = tables.iloc[:,0].tolist()
            for table in tables: 
                sql_query = "SELECT * FROM " + table + ";"
                data_dict[table] = pd.read_sql(sql_query, conn_curs[0])
            return data_dict
        
    def custom(self, data_base, *no_key_queries, **queries):
        """Retrieve custom select queries from a database. Can pass a name key
           for queries <name = 'select_statement'>. One can also pass queries 
           without a name key. These queries will be named by numerical index.
           -> dictionary of query names and resulting pandas data frames
           {query_name: pd}. All query statements are stored in lists and these 
           are stored in the returned dictionary by the keys 'queries' and 
           'no_key_queries' depending on if query statements were passed with a
           name key or not. Do not use 'queries' or 'no_key_queries' as names 
           for any queries to avoid conflict with dictionary name keys."""
        with self.msql_connection as conn_curs:
            conn_curs[1].execute("USE " + data_base + ";") 
            data_dict = {}
            query_error = False
            if no_key_queries:
                no_key_queries = list(no_key_queries)
                name = 0
                for sql_query in no_key_queries:
                    try:
                        data_dict[name] = pd.read_sql(sql_query, conn_curs[0])
                    except:
                        query_error = True
                        data_dict[name] = str(sys.exc_info()[1])
                        print("Error in executing Query " + str(name) + ".")
                    else:    
                        print("Query " + str(name) + " has been executed.")
                    finally:
                        name += 1
                data_dict["no_key_queries"] = no_key_queries    
            if queries:
                for name, sql_query in queries.items():
                    try:
                        data_dict[name] = pd.read_sql(sql_query, conn_curs[0])
                    except:
                        query_error = True
                        data_dict[name] = str(sys.exc_info()[1])
                        print("Error in executing Query " + str(name) + ".")
                    else:
                        print("Query " + name + " has been executed.")
                data_dict["queries"] = queries
            if query_error is True:
                print("\nAn error has occured in retrieving at least one"\
                      " select query.")
            return data_dict
    
    @staticmethod
    def sql_save(pandas_dict, file_name):
        """Save query dictionary returned from the query_tables or custom
        methods as JSON file. All panda data frames in the query dictionary 
        are converted to JSON strings before the dictionary is saved."""
        pandas_dict = pandas_dict.copy()
        for key in pandas_dict.keys():
            try:
                pandas_dict[key] = pandas_dict[key].to_json()
            except:
                pass
        with open(file_name, 'w') as f_obj:
            json.dump(pandas_dict, f_obj)
            
    @staticmethod
    def sql_load(file_name):
        """Load query dictionary from JSON file. Format will be same as the
        dictionary returned from the query_tables or custom methods."""
        with open(file_name) as f_obj:
            json_data = json.load(f_obj)
        pandasDict = {}    
        for key in json_data.keys():
            try:
                key = int(key)
            except:
                try:
                    pandasDict[key] = pd.read_json(json_data[key])
                except:
                    pandasDict[key] = json_data[key]
            else:
                try:
                    pandasDict[key] = pd.read_json(json_data[str(key)])
                except:
                    pandasDict[key] = json_data[str(key)]
        return pandasDict
    