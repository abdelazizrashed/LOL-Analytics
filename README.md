# League of Legeneds (LOL) Game Analytics using Apache Spark

This project is made using Apache Spark and the processed data is already saved in local Spark SQL database

## The code has two unconnected modes

### 1- Run project

This mode assumes you have already have the data from LOL developers API and this mode in tern is divided into two mode

### A- Show Analytics

This mode mode uses the processed data saved in the SQL DB to show the analytics to run this mode run the following command in the terminal in the same level as the root of the project

> python main.py

### B- Clean and Save data

This mode process the raw data after getting it from LOL API. To run this mode follow these steps

- Step 1: Create the database and its columns using sample data for speed

> python prepare_db.py

- Step 2: Add the data by giving the path to the JSON file contianing the data

> python add_more_data.py

## 2- Get data from LOL API
All the scripts should run in the same level as /get_data directory

- Step 1: Get summoners names
> python get_names.py
- Step 2: Get summoners puuid
> python get_puuid.py
- Step 3: Get matches id
> python get_matchid.py
- Step 4: Get the matches from the API
> python get_matches.py
- Step 5: Combine all the matches data in one file in the /data_combiner dir
>python combine.py
