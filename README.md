# Dryft Data Engineering Challenge

## Introduction

The goal of this project is to design and implement a robust, scalable data pipeline that fetches daily differential
reservation data, performs necessary aggregations, and stores the results in a PostgreSQL relational database. The
pipeline is expected to:

- **Process and store** the initial base dataset.
- **Design and implement:** a pipeline that updates the current data while retaining historical information properly.

## Implementation Steps

Follow these steps to set up and run the Dryft Data Engineering Pipeline locally:

### Step 1: Clone the Repository

- Open your terminal or command prompt
- Run the following commands:
  ```
  git clone https://github.com/Afaqrehman98/Dryft-Data-Engineering.git
  ```

### Step 2: Update the config.py according to your credentials :

  ```
  DB_NAME = ''
  USER_NAME = ''
  PASSWORD = ''
  HOST = ''
  PORT = ''
  ```

### Step 3: Install Dependencies:

```
pip install -r requirements.txt
```

### Step 4: The PostgreSQL schema will be designed to store reservation data efficiently. We'll use a single table with the following structure:

  ```
  CREATE TABLE reservations (
    id SERIAL PRIMARY KEY,
    reserv_nr VARCHAR(50) NOT NULL,
    pos INT NOT NULL,
    bedarfsmenge INT,
    bme VARCHAR(10),
    material VARCHAR(100),
    bed_termin DATE,
    gel BOOLEAN,
    kda_pos INT,
    auftrag VARCHAR(50),
    angel_am DATE,
    uhrzeit TIME,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When the record was added
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP   -- When the record was last updated
    );
  ```

### Step 5: Creating the initial tables in the database:

- **Script:** create_table.py.
- **Purpose:** This script will create the initial tables in the desired database.

### Step 6: Processing the Base Dataset

- **Script:** load_initial_data.py.
- **Purpose:** This script will load the base data from the CSV file and store it in the reservations table.
- **Steps:**
    - Load the base CSV data.
    - Insert the data into the **reservations** table.
    - Set **created_at** to **Current time when the record was added** for all records, indicating that these are the
      latest versions.
    - Set **updated_at** to **9999-12-31 23:59:59** for all records, indicating that these are the latest versions.

### Step 7: Processing Daily Updates

- **Script:** process_daily_updates.py.
- **Purpose:** This script will process daily update files and update the existing records or insert new ones in the **
  reservations** table.
- **Steps:**
    - Load the update CSV file.
    - If a matching record exists (based on **reserv_nr** and **pos**), update the existing record:
        - Update the property
        - Set updated_at to Current Time for the record.
    - If no matching record exists, insert the new record with **created_at** set to current time and **updated_at** to
      the default future time.

### Step 8 Automation and Scheduling

You can schedule the pipeline to run daily using tools like **cron** (for Linux) or **Task Scheduler** (for Windows).

Example cron job (runs daily at 6 AM):

```
0 6 * * * /path/to/python /path/to/dryft_data_pipeline/scripts/pipeline.py

```

## Libraries & DB Used

- **PostgreSQL:** The relational database to store the data.
- **Pandas:** For data manipulation.
- **SQLAlchemy:** For database interaction.
- **psycopg2:** PostgreSQL adapter for Python.



## Contributing

Please fork this repository and contribute back using
[pull requests](https://github.com/Afaqrehman98/Dryft-Data-Engineering/pulls).

Any contributions, large or small, major features, bug fixes, are welcomed and appreciated
but will be thoroughly reviewed .

### Contact - Let's become friend

- [Twitter](https://x.com/afaqkhan_98)
- [Github](https://github.com/Afaqrehman98)
- [Linkedin](https://www.linkedin.com/in/afaqrehman98/)

## License

```
Copyright 2024 Afaq Ur Rehman Khan

Licensed under the Apache License, Version 2.0 (the "License");

you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

```
