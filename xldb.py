import sqlite3
from pathlib import Path
import pandas as pd
from typing import Union


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
        add_data: Adds data to the database tables.
    """

    db_path_dbname = Path
    db_dir = str
    db_name = str
    source_locations = list
    con = sqlite3.connect
    cursor = sqlite3.Cursor

    def __init__(self, db_name_path: Union[str, Path], data_location:Union[str,Path,list] = None):
            """
            Initialize the XLDB object.

            Args:
                path_dbname (str or Path): The path to the database file.
                data_location (str, Path, or list, optional): The location(s) of the data file(s). Defaults to None.

            Raises:
                TypeError: If the database path is not a string or Path object.
                TypeError: If any of the data locations are not strings or Path objects.
            """

            if not isinstance(db_name_path, (str, Path)):
                raise TypeError("Database path should be a string or Path object")
            if data_location and (not all(isinstance(data, (str, Path)) for data in data_location)):
                raise TypeError("All data elements should be strings or Path objects")

            try:
                self.db_path_dbname = Path(db_name_path)
                if self.db_path_dbname.suffix != '.db': 
                    self.db_path_dbname = self.db_path_dbname.with_suffix('.db')
                self.db_dir = self.db_path_dbname.parent
                self.db_name = self.db_path_dbname.stem

                if data_location is None: data_location = []
                if isinstance(data_location, (str,Path)): data_location = [data_location]
                self.source_locations = [Path(dir) for dir in data_location]
            except Exception as e:
                raise Exception("Issue with class attribute creation: ", e)

            try:
                self.con, self.cursor = self._create_database(self.db_path_dbname)
            except Exception as e:
                raise Exception("Database could not be created due to exception: ", e)
            
            if self.source_locations:
                try:
                    self.add_data(data_path=self.source_locations, if_exists='fail')
                    self.con.commit()
                except Exception as e:
                    self.con.rollback()
                    self._clear_db()
                    raise Exception("Database Removed. Data not added to database due to exception: ", e)

    def _create_database(self, db_name:str) -> tuple:
        """
        Creates a SQLite database with the given name.

        Args:
            db_name (str): The name of the database.

        Returns:
            tuple: A tuple containing the connection and cursor objects.
        """
        if Path.exists(db_name):
            raise Exception("Database already exists")

        try:
            con = sqlite3.connect(db_name)
            cur = con.cursor()
            return con, cur
        except Exception as e:
            raise Exception("Database not created due to exception: ", e)

    def _clear_db(self) -> None:
        """
        Clears the database by closing the connection and deleting the database file.
        """
        try:
            self.con.close()
            Path(self.db_path_dbname).unlink()
        except Exception as e:
            raise Exception("Database not deleted due to exception: ", e)

    def _parse_csv(self, file_path:Path, **kwargs) -> dict:
        """
        Parse a CSV file to a pandas DataFrame.

        Args:
            file_path (Path): The path to the CSV file.

        Returns:
            dictionary: A dictionary of the file name and pandas DataFrame containing the data from the file.

        Raises:
            Exception: If an error occurs during the parsing process.
        """
        try:
            data = pd.read_csv(file_path, **kwargs)
            return {file_path.stem: data}
        except Exception as e:
            raise Exception("CSV was unable to be read: ", e)

    def _parse_excel(self, file_path:Path, **kwargs) -> dict:
        """
        Parse an Excel file to a pandas DataFrame.

        Args:
            file_path (Path): The path to the Excel file.

        Returns:
            dictionary: A dictionary of the sheet name and pandas DataFrame containing the data from the file.

        Raises:
            Exception: If an error occurs during the parsing process.
        """
        try:
            with pd.ExcelFile(file_path) as xls:
                sheet_names = xls.sheet_names
                data = {sheet_name: pd.read_excel(file_path, sheet_name, **kwargs) for sheet_name in sheet_names}
                return data
        except Exception as e:
            raise Exception("Excel file was unable to be read: ", e)
                                                        
    def _parse_to_pd(self, file_path:Path, **kwargs) -> dict:
        """
        Parse the file to a pandas DataFrame.

        Args:
            file_path (Path): The path to the file.

        Returns:
            dictionary: A dictionary of the file or sheet name and pandas DataFrame containing the data from the file.

        Raises:
            Exception: If the file format is not supported.
        """

        supported_formats = {'.csv':  self._parse_csv, 
                             '.xls':  self._parse_excel,
                             '.xlsx': self._parse_excel}

        try:
            file_path = Path(file_path)
        except:
            raise TypeError("File path should be a Path object or able to convert to a Path Object")

        if not file_path.suffix in supported_formats:
            raise Exception(f'File format not supported, please provide a file of the supported types: {supported_formats.keys()}')
        if not file_path.exists():
            raise Exception('File does not exist, please provide a valid file path.')
        
        try:
            func = supported_formats[file_path.suffix]
            return func(file_path, **kwargs)

        except Exception as e:
            raise Exception("Data not read due to exception: ", e)

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

    def _fetch_tables(self) -> list:
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

    def _fetch_columns(self, table_name:str) -> list:
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

    def _fetch_data(self, table_name:str) -> pd.DataFrame: 
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

    def to_csv(self, dir:str=None, exclude:list=[], include_db_name:bool = True, close_delete:bool = True, **kwargs) -> None:
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

    def to_excel(self, dir:str=None, exclude:list=[], file_name:str = None, close_delete:bool = True, **kwargs) -> None:
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
        
    def add_data(self, data_path: Union[str, Path, list], if_exists='fail', map: dict = None, **kwargs) -> None:

        # need to differentiate between add and append.


        if data_path and (not all(isinstance(data, (str, Path)) for data in data_path)):
            raise TypeError("All data elements should be strings or Path objects")
        check_if_exists = ['fail', 'replace', 'append']
        if not if_exists in check_if_exists:
            raise TypeError(f"if_exists argument should be one of {check_if_exists}")


        try:
            # package data_path(s) and add to source_locations
            

            # This isn't quite working the right way.
            print(data_path)
            if isinstance(data_path, (str,Path)): 
                data_path = [data_path]
                print("invoked list embedding")
            print(self.source_locations)
            # Only need to do this is the file is not already in the source_locations
            #self.source_locations.extend([Path(dir) for dir in data_path])
            print(self.source_locations)

            print("Add Data Checkpoint 1")
            print('*'*50)   
            
            print(data_path)
            for file in data_path:
                data_dict = self._parse_to_pd(file, **kwargs)
                table_name = list(data_dict.keys())[0]
                df = list(data_dict.values())[0]

                # rename columns if map exists
                if map and (table_name in map):
                    columns = [map[table_name].get(col, col) for col in df.columns]
                    df = df[columns]

                try:
                    df.to_sql(table_name, self.con, if_exists=if_exists, index=False)
                    self.con.commit()
                except Exception as e:
                    self.con.rollback()
                    raise Exception("Table not written to database due to exception: ", e)

            self.con.commit()

        except Exception as e:
            self.con.rollback()
            raise Exception("Data not written to database due to exception: ", e)
            

        pass

    def old_add_data(self, data: dict, overwrite: bool = False, map: dict = None) -> None:
        """

        PRESERVING THIS FOR MY EMBARASEMENT AT NOT HAVING READ THE df.to_sql DOCS

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
                

                try:
                    target_cols = self._fetch_columns(table_name)

                    #if not all(col in target_cols for col in data.columns):
                    if not set(data.columns) == set(target_cols):
                        raise Exception(f"Columns do not match target table '{table_name}'. Non-matching columns: ", [col in data.columns if col not in target_cols else ''])

                    data = data[target_cols]

                    add_data = f"INSERT INTO {table_name} VALUES({', '.join(['?']*len(target_cols))})"

                    self.cursor.executemany(add_data, [tuple(row) for row in data.values])
                    self.con.commit()

                except Exception as e:
                    self.con.rollback()
                    raise Exception("Data not written to database due to exception: ", e)




            self.con.commit()  

        except Exception as e:
            self.con.rollback()
            raise Exception("Data not written to database due to exception: ", e)
        
    def query(self, query:str) -> list:
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

    #db = XLDB(db_name_path='csv_test_db', data_location=['test_csv.csv'])
    db = XLDB(db_name_path='xlsx_test_db', data_location=['test_xlsx.xlsx'])

    #print(db.query("SELECT * FROM test_csv limit 10"))
    print(db.query("SELECT * FROM users limit 10"))
    db.to_csv()
    
    #db = XLDB('xlsx_test_db')
    #db = XLDB('simple_xlsx_test_db')
    #db.add_data(data)
    #db.to_excel()


# https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html 

    # Polishing:
    # [X] Redo tabluar data reading
    # [X] Make sure I am commiting after every write
    # [X] Try and except catches
    # [X] Type checks
    # [X] replace prints with raises
    # [?] abstract some of the query logic
    # [X] table and column name validation
    # [] Flow to add vs append data
    # [X] saving db to location doesn't work.
