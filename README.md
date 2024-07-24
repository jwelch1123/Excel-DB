# Excel-DB
What if Excel ***was*** a database

## Features
- **Seamless Integration with Excel and CSV Files:** Easily create SQLite databases from your existing Excel and CSV files.
- **Comprehensive Data Manipulation:** Read, append, and export data with ease.


## Getting Started
- Copy the repository to your project directory.
- Use `from xldb import XLDB` to bring the module into your project.

## Usage
- Create a Database:

  `db = XLDB('database_name', data_location=[file_path(s)])`
- Add or append Data: 

  `db.add_data(data_path=file_name)` or
  `db.append_data(data_path=file_name)`
- Arbitrary SQL operations: 
  
  `db.query('SELECT * FROM table_name')`
- Export Data: 

  `db.to_csv()` or `db.to_excel()`


## FAQ

- Why would you want to use Excel as a database?
  - Sometimes you want to do sql on a csv file and this is a quick way to spin one up. 
- Is this useful?
  - Nope
- Why did you make this?
  - My previous manager used the phrase "Excel isn't a Database" so many times I started to wonder if you could.
- Doesn't Pandas provide a DataFrame.to_sql method?
  - Yes, but I found that out after I was almost done. It doesn't do exactly the same thing...
- What would you do differently?
  - Fully read the Pandas and SQLite documentation before starting.

## Acknowledgements
- Thank you Kelly Raymond for your mentorship and guidance. 
- Thanks to [code camp for providing a guide](https://www.freecodecamp.org/news/build-your-first-python-package/) to setting up a package version of this project. As well as [Arjan codes](https://www.youtube.com/watch?v=5KEObONUkik) for the video tutorial on the same topic.