# Excel-DB
What if Excel **was** a database

## Features
- **Seamless Integration with Excel and CSV Files:** Easily create SQLite databases from your existing Excel and CSV files.
- **Comprehensive Data Manipulation:** Read, append, and export data with ease.
- **Pandas Compatibility:** Leverage the power of pandas for efficient data handling and manipulation, making it easier to 

## Getting Started
- Copy the repository to your project directory.
- Use `from xldb import XLDB` to bring the module into your project.

## Usage
- Create a Database: `db = XLDB('database_name', data_location=[file_path(s)])`
- Read Data: `{'table_name': dataframe} =db.read_tabular_data(file_path)`
- Add Data: `db.add_data({'table_name': dataframe})`
- Arbitrary SQL operations: `db.query('SELECT * FROM table_name')`
- Export Data: `db.to_csv()` or `db.to_excel()`


## FAQ
- Why would you want to use Excel as a database?
  - 
- Is this useful?
  - No
- Why did you make this?
  - My previous manager used the phrase "Excel isn't a Database" so many times I started to wonder if you could.
- Doesn't Pandas provide a DataFrame.to_sql method?
  - Yes, but I found that out after I was almost done. It doesn't do exactly the same thing...
- What would you do differently?
  - Fully read the Pandas and SQLite documentation before starting.

## Acknowledgements
- Thank you Kelly Raymond for your mentorship and guidance. 