import sqlite3
from pathlib import Path
import pandas as pd
from typing import Union # only needed on python below 3.10


class XLDB:
    '''
    The XLDB class is a class that allows the import of data from CSV and Excel files into a SQLite database.
    Manipulation of that data using SQL and exporting the data back to CSV or Excel files.

    Attributes:
    - db_path_dbname: The path to the database file.
    - db_dir: The directory where the database file is located.
    - db_name: The name of the database file.
    - source_locations: The location(s) of the data file(s).
    - con: The connection object to the database.
    - cursor: The cursor object to the database.

    Methods:
    - __init__: Initializes the XLDB object.
    - _create_database: Creates a SQLite database with the given name.
    - _clear_db: Clears the database by closing the connection and deleting the database file.
    - _parse_csv: Parse a CSV file to a pandas DataFrame.
    - _parse_excel: Parse an Excel file to a pandas DataFrame.
    - _parse_to_pd: Parse the file to a pandas DataFrame.
    - _fetch_tables: Fetches the names of all tables in the SQLite database.
    - _fetch_columns: Fetches the column names of a given table.
    - _fetch_data: Fetches all data from the specified table and returns it as a pandas DataFrame.
    - to_csv: Export the data from the database tables to CSV files.
    - to_excel: Export the data from the database to an Excel file.
    - add_data: Add data to the database.
    - append_data: Appends data to the XLDB.
    - query: Executes the given SQL query and returns the results. 
    
    '''

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
        if dir and (not isinstance(dir, str)):
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
        """
        Add data to the database.

        Args:
            data_path (Union[str, Path, list]): The path(s) to the data file(s) to be added.
            if_exists (str, optional): Specifies how to behave if the table already exists. 
                Possible values are 'fail', 'replace', and 'append'. Defaults to 'fail'.
            map (dict, optional): A dictionary that maps table names to column names. 
                Only columns specified in the map will be included in the database table.
            **kwargs: Additional keyword arguments to be passed to the _parse_to_pd method.

        Raises:
            TypeError: If data_path is not a string, Path object, or a list of strings/Path objects.
            TypeError: If if_exists is not one of 'fail', 'replace', or 'append'.
            Exception: If an error occurs while writing the data to the database.

        Returns:
            None
        """
        if data_path and (not all(isinstance(data, (str, Path)) for data in data_path)):
            raise TypeError("All data elements should be strings or Path objects")
        check_if_exists = ['fail', 'replace', 'append']
        if not if_exists in check_if_exists:
            raise TypeError(f"if_exists argument should be one of {check_if_exists}")

        try:
            if isinstance(data_path, (str,Path)): 
                data_path = [data_path]
            
            for file in data_path:
                if file not in self.source_locations:
                    self.source_locations.append(file)
                data_dict = self._parse_to_pd(file, **kwargs)

                print("Data Dict: ", data_dict.keys())

                for table_name, df in data_dict.items():
                    print(table_name, df.columns)
                    if map and (table_name in map):
                        columns = [map[table_name].get(col, col) for col in df.columns]
                        df = df[columns]
                        data_dict[table_name] = df

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
            
    def append_data(self, data_path: Union[str, Path, list]) -> None:
        """
        Appends data to the XLDB.

        Parameters:
        - data_path: The path to the data file(s) to be appended. It can be a string, a Path object, or a list of paths.

        Returns:
        - None
        """
        self.add_data(data_path, if_exists='append')
        
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
        