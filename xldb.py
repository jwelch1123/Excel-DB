import sqlite3
from pathlib import Path
import pandas as pd


class XLDB:
    """
    A class for creating a SQLite database from XLSX or CSV files and performing operations on it.

    Attributes:
        db_path_dbname (Path): The path to the database file.
        db_dir (str): The directory where the database file is located.
        db_name (str): The name of the database file.
        source_locations (list): A list of paths to data files.
        con (sqlite3.Connection): The connection object for the SQLite database.
        cursor (sqlite3.Cursor): The cursor object for executing SQL queries.

    Methods:
        __init__: Initializes the XLDB object.
        _create_database: Creates a SQLite database with the given name.
        _clear_db: Clears the database by closing the connection and deleting the database file.
        read_tabular_data: Reads tabular data from a file.
        _fetch_tables: Fetches the names of all tables in the SQLite database.
        _fetch_columns: Fetches the column names of a given table.
        _fetch_data: Fetches all data from the specified table and returns it as a pandas DataFrame.
        to_csv: Writes the data from the database tables to CSV files.
        to_excel: Writes the data from the database tables to an Excel file.
        _append_data: Appends data to a specified table in the database.
        add_data: Adds data to the database tables.
    """

    db_path_dbname = Path
    db_dir = str
    db_name = str
    source_locations = list
    con = sqlite3.connect
    cursor = sqlite3.Cursor

    def __init__(self, path_dbname:str|Path, data_location:str|Path|list = None):
            """
            Initialize the XLDB object.

            Args:
                path_dbname (str or Path): The path to the database file.
                data_location (str, Path, or list, optional): The location(s) of the data file(s). Defaults to None.

            Raises:
                TypeError: If the database path is not a string or Path object.
                TypeError: If any of the data locations are not strings or Path objects.
            """

            if not isinstance(path_dbname, (str, Path)):
                raise TypeError("Database path should be a string or Path object")
            if data_location and (not all(isinstance(data, (str, Path)) for data in data_location)):
                raise TypeError("All data elements should be strings or Path objects")


            self.db_path_dbname = Path(path_dbname)
            if self.db_path_dbname.suffix != '.db': 
                self.db_path_dbname = self.db_path_dbname.with_suffix('.db')
            self.db_dir = self.db_path_dbname.parent
            self.db_name = self.db_path_dbname.stem

            if data_location is None: data_location = []
            if isinstance(data_location, (str,Path)): data_location = [data_location]
            self.source_locations = [Path(dir) for dir in data_location]

            self.con, self.cursor = self._create_database(self.db_name)

            if self.source_locations:
                data = {}
                for file in self.source_locations:
                    file_path = Path(file)
                    data.update(self.read_tabular_data(file_path.absolute()))
                self.add_data(data)

    def _create_database(self, db_name:str):
        """
        Creates a SQLite database with the given name.

        Args:
            db_name (str): The name of the database.

        Returns:
            tuple: A tuple containing the connection and cursor objects.
        """
        try:
            con = sqlite3.connect(db_name)
            cur = con.cursor()
            return con, cur
        except Exception as e:
            raise Exception("Database not created due to exception: ", e)

    def _clear_db(self):
        """
        Clears the database by closing the connection and deleting the database file.
        """
        try:
            self.con.close()
            Path(self.db_path_dbname).unlink()
        except Exception as e:
            raise Exception("Database not deleted due to exception: ", e)

    def read_tabular_data(self, file_path, **kwargs) -> dict:
        """
        Read tabular data from a file.

        Args:
            file_path (str): The path to the file.
            **kwargs: Additional keyword arguments to be passed to the pandas read_csv or read_excel function.

        Returns:
            dict: A dictionary containing the tabular data. The keys are the sheet names (for Excel files) or the file name (for CSV files),
                  and the values are pandas DataFrames representing the data.

        Raises:
            Exception: If the file format is not supported.

        """
        
        if not isinstance(file_path, Path):
            raise TypeError("File path should be a Path object")
        
        try:
            if file_path.suffix == '.csv':
                data = {file_path.stem: pd.read_csv(file_path, **kwargs)}
            elif (file_path.suffix == '.xls') or (file_path.suffix == '.xlsx'):
                with pd.ExcelFile(file_path) as xls:
                    sheet_names = xls.sheet_names
                    data = {sheet_name: pd.read_excel(file_path, sheet_name, **kwargs) for sheet_name in sheet_names}
            else:
                raise Exception('File format not supported')
            
            for key, value in data.items():
                datetime_cols = value.select_dtypes(include=['datetime']).columns
                for col in datetime_cols:
                    value[col] = value[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                data[key] = value

            return data
        except Exception as e:
            raise Exception("Data not read due to exception: ", e)        

    def _fetch_tables(self):
        """
        Fetches the names of all tables in the SQLite database.

        Returns:
            A list of table names.
        """
        try:
            table_list = "SELECT name FROM sqlite_master WHERE type='table';"
            self.cursor.execute(table_list)
            tables = [table[0] for table in self.cursor.fetchall()]
            self.con.commit()
            return tables
        except Exception as e:
            self.con.rollback()
            raise Exception("Tables not fetched due to exception: ", e)

    def _fetch_columns(self, table_name:str):
        """
        Fetches the column names of a given table.

        Args:
            table_name (str): The name of the table.

        Returns:
            list: A list of column names.

        """
        try:
            get_cols = f"PRAGMA table_info({table_name})"
            self.cursor.execute(get_cols)
            cols = [col[1] for col in self.cursor.fetchall()]
            self.con.commit()
            return cols
        except Exception as e:
            self.con.rollback()
            raise Exception("Columns not fetched due to exception: ", e)

    def _fetch_data(self, table_name:str):
        """
        Fetches all data from the specified table and returns it as a pandas DataFrame.

        Args:
            table_name (str): The name of the table to fetch data from.

        Returns:
            pandas.DataFrame: A DataFrame containing all the data from the specified table.
        """
        try:
            cols = self._fetch_columns(table_name)

            query_all = f"SELECT * FROM {table_name}"
            self.cursor.execute(query_all)
            data = self.cursor.fetchall()

            df = pd.DataFrame(data, columns=cols)
            self.con.commit()
            return df
        
        except Exception as e:
            self.con.rollback()
            raise Exception("Data not fetched due to exception: ", e)

    def to_csv(self, dir:str=None, exclude:list=[], include_db_name:bool = True, close_delete:bool = True, **kwargs):
        """
        Export the data from the database tables to CSV files.

        Args:
            dir (str, optional): The directory path where the CSV files will be saved. Defaults to None.
            exclude (list, optional): A list of table names to exclude from exporting. Defaults to [].
            include_db_name (bool, optional): Whether to include the database name in the CSV file names. Defaults to True.
            close_delete (bool, optional): Whether to close and delete the database after exporting. Defaults to True.
            **kwargs: Additional keyword arguments that will be passed to the `to_csv` method of the pandas DataFrame.

        Raises:
            TypeError: If the `dir` argument is provided but not a string, or if the `exclude` argument is provided but not a list.
            Exception: If an error occurs during the export process.

        Returns:
            None
        """
    
        if dir and (not isinstance(dir, str)):
            raise TypeError("dir argument should be a string")
        if exclude and (not isinstance(exclude, list)):
            raise TypeError("exclude argument should be a list")        
        if not isinstance(include_db_name, bool):
            raise TypeError("include_db_name argument should be a boolean")
        if not isinstance(close_delete, bool):
            raise TypeError("close_delete argument should be a boolean")

        try:
            tables = [table for table in self._fetch_tables() if table not in exclude]

            if kwargs.get('index') is None:
                kwargs['index'] = False

            for table_name in tables:
                df = self._fetch_data(table_name)
                dir_db_file = f"{dir+'_' if dir else ''}{self.db_name+'_' if include_db_name else ''}{table_name}.csv"

                df.to_csv(dir_db_file, **kwargs)

            if close_delete:
                self._clear_db()

        except Exception as e:
            raise Exception("Data not written to csv files due to exception: ", e)

    def to_excel(self, dir:str=None, exclude:list=[], file_name:str = None, close_delete:bool = True, **kwargs):
        """
        Export the data from the database to an Excel file.

        Args:
            dir (str, optional): The directory where the Excel file will be saved. Defaults to None.
            exclude (list, optional): A list of table names to exclude from the export. Defaults to an empty list.
            file_name (str, optional): The name of the Excel file. If not provided, the database name will be used. Defaults to None.
            close_delete (bool, optional): Whether to close and delete the database after exporting. Defaults to True.
            **kwargs: Additional keyword arguments to be passed to the `to_excel` method of pandas DataFrame.

        Raises:
            TypeError: If the `dir` argument is not a string, `exclude` argument is not a list, `file_name` argument is not a string, or `close_delete` argument is not a boolean.

        Returns:
            None
        """
        if not isinstance(dir, str):
            raise TypeError("dir argument should be a string")
        if not isinstance(exclude, list):
            raise TypeError("exclude argument should be a list")
        if file_name and (not isinstance(file_name, str)):
            raise TypeError("file_name argument should be a string")
        if not isinstance(close_delete, bool):
            raise TypeError("close_delete argument should be a boolean")
        
        try:    
            if not file_name:
                file_name = self.db_name

            tables = [table for table in self._fetch_tables() if table not in exclude]
            dir_db_file = f"{dir + '_' if dir else ''}{file_name}.xlsx"
            
            writer = pd.ExcelWriter(dir_db_file)
            
            for table_name in tables:
                df = self._fetch_data(table_name)
                df.to_excel(writer, sheet_name=table_name, index=False, **kwargs)
            writer.close()

            if close_delete: self._clear_db()
        
        except Exception as e:
            raise Exception("Data not written to excel file due to exception: ", e)

    def _append_data(self, table_name:str, data:pd.DataFrame):
        """
        Append data to a specified table in the database.

        Args:
            table_name (str): The name of the target table.
            data (pd.DataFrame): The data to be appended, represented as a pandas DataFrame.

        Raises:
            Exception: If the columns in the data do not match the columns in the target table.

        Returns:
            None
        """
        try:
            target_cols = self._fetch_columns(table_name)

            if not all(col in target_cols for col in data.columns):
                raise Exception(f"Columns do not match target table '{table_name}'. Non-matching columns: ", [col in data.columns if col not in target_cols else ''])

            data = data[target_cols]

            add_data = f"INSERT INTO {table_name} VALUES({', '.join(['?']*len(target_cols))})"

            self.cursor.executemany(add_data, [tuple(row) for row in data.values])
            self.con.commit()

        except Exception as e:
            self.con.rollback()
            raise Exception("Data not written to database due to exception: ", e)
        
    def add_data(self, data: dict, overwrite: bool = False, map: dict = None):
        """
        Add data to the database.

        Args:
            data (dict): A dictionary containing the data to be added. The keys should be table names and the values should be Pandas DataFrames.
            overwrite (bool, optional): If True, existing tables will be dropped and recreated. Defaults to False.
            map (dict, optional): A dictionary with table names as keys and a data column: sql column dictionary as values. Defaults to None.

        Raises:
            TypeError: If the data argument is not a dictionary, if the keys are not strings, if the values are not Pandas DataFrames, if the overwrite argument is not a boolean, or if the map argument is not a dictionary.
            Exception: If the columns in the data do not match the columns in the target table.

        Returns:
            None
        """

        if not isinstance(data, dict):
            raise TypeError("Data argument should be a dictionary")
        if not all(isinstance(key, str) for key in data.keys()):
            raise TypeError("All keys should be strings")
        if not all(isinstance(value, pd.DataFrame) for value in data.values()):
            raise TypeError("All dictionary values should be Pandas DataFrames")
        if not isinstance(overwrite, bool):
            raise TypeError("Overwrite argument should be a boolean")
        if map and (not isinstance(map, dict)):
            raise TypeError("Map argument should be a dictionary with table names as keys and a data column: sql column dictionary as values")

        tables = self._fetch_tables()

        try:
            for table_name, df in data.items():

                if map and (table_name in map):
                    columns = [map[table_name].get(col, col) for col in df.columns]
                    cols = ', '.join(columns)
                else:
                    columns = df.columns
                    cols = ', '.join(columns)

                if table_name in tables:
                    if overwrite:
                        self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")     
                        create_table = f'CREATE TABLE {table_name}({cols})'               
                else:
                    create_table = f'CREATE TABLE {table_name}({cols})'
                    self.cursor.execute(create_table)
                
                target_cols = self._fetch_columns(table_name)
                
                if not all(col in target_cols for col in columns):
                    raise Exception(f"Columns do not match target table '{table_name}'. Try using the map argument or renaming dataframes. Non-matching columns: ", [col in columns if col not in target_cols else ''])

                self._append_data(table_name, df)

            self.con.commit()  

        except Exception as e:
            self.con.rollback()
            raise Exception("Data not written to database due to exception: ", e)
        
    def query(self, query:str):
        """
        Executes the given SQL query and returns the result.

        Args:
            query (str): The SQL query to execute.

        Returns:
            list: The result of the query.

        Raises:
            Exception: If the query execution fails.
        """

        if not isinstance(query, str):
            raise TypeError("Query should be a string")

        try:
            self.cursor.execute(query)
            self.con.commit()
            return self.cursor.fetchall()
        except Exception as e:
            self.con.rollback()            
            raise Exception("Query failed: ", e)

if __name__ == '__main__':

    db = XLDB(path_dbname='csv_test_db')
    data = db.read_tabular_data(Path('test_csv.csv'))
    db.add_data(data)
    #print(db.query("SELECT * FROM test_csv limit 10"))
    #db.to_csv()
    
    #db = XLDB('xlsx_test_db')
    #db = XLDB('simple_xlsx_test_db')
    #data = db.read_tabular_data(Path('test_xlsx.xlsx'))
    #data = db.read_tabular_data(Path('simple_test_xlsx.xlsx'))
    #db.add_data(data)
    #db.to_excel()




    # [X] rewrite __init__
    # [x] read csv/xls/xlsx file
    # [x] get the sheet/table names and data types
    # [x] create a database
    # [x] write data to the database
    # [x] return the database object.
    # [x] append additional data. NEEDs overwrite control. Data types control. column matching
    # [x] execute arbitrary queries
    # [x] write data back to a file
    # [X] Fix bad types in xlsx

# https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html 

    # Polishing:
    # [] Redo tabluar data reading
    # [X] Make sure I am commiting after every write
    # [X] Try and except catches
    # [X] Type checks
    # [X] replace prints with raises
    # [?] abstract some of the query logic
    # [] table and column name validation
