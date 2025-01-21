import snowflake.connector
from datetime import datetime

connection_params = {
    'user': 'xxx',
    'password': 'xxx',
    'account': 'xxx',
    'warehouse': 'xxx',
    'database': 'xxx',
    'schema': 'xxx'
}

def connect_to_snowflake():
    return snowflake.connector.connect(**connection_params)

def create_tables(cursor):
    cursor.execute("""
        CREATE OR REPLACE TABLE Staging_Table (
            ID INT,
            Name VARCHAR(255),
            DOB DATE,
            Vaccination_Type VARCHAR(10),
            Vaccination_Date DATE,
            Country VARCHAR(10),
            Free_Or_Paid CHAR(1)
        );
    """)

    cursor.execute("""
        CREATE OR REPLACE TABLE USA_Table (
            ID INT,
            Name VARCHAR(255),
            Vaccination_Type VARCHAR(10),
            Vaccination_Date DATE
        );
    """)
    cursor.execute("""
        CREATE OR REPLACE TABLE India_Table (
            ID INT,
            Name VARCHAR(255),
            DOB DATE,
            Vaccination_Type VARCHAR(10),
            Vaccination_Date DATE,
            Free_Or_Paid CHAR(1)
        );
    """)
    cursor.execute("""
        CREATE OR REPLACE TABLE AUS_Table (
            ID INT,
            Name VARCHAR(255),
            DOB DATE,
            Vaccination_Type VARCHAR(10),
            Vaccination_Date DATE
        );
    """)
    print("Tables created successfully!")

def normalize_data(data, country):
    normalized_data = []
    for row in data:
        if country == "USA":
            # Normalize USA data
            normalized_data.append((row[0], row[1], None, row[2], datetime.strptime(row[3], '%m%d%Y').date(), "USA", None))
        elif country == "India":
            # Normalize India data
            normalized_data.append((row[0], row[1], datetime.strptime(row[2], '%m/%d/%Y').date(), row[3], datetime.strptime(row[4], '%m/%d/%Y').date(), "India", row[5]))
        elif country == "AUS":
            # Normalize AUS data
            dob = datetime.strptime(row[3], '%m/%d/%Y').date() if row[3] else None
            normalized_data.append((row[0], row[1], dob, row[2], datetime.strptime(row[4], '%m/%d/%Y').date(), "AUS", None))
    return normalized_data

# Load data into Staging Table
def load_to_staging(cursor, data):
    print("Loading data into Staging Table...")
    for row in data:
        cursor.execute("""
            INSERT INTO Staging_Table (ID, Name, DOB, Vaccination_Type, Vaccination_Date, Country, Free_Or_Paid)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, row)
    print("Data loaded into Staging Table successfully!")

# Transform and load into country-specific tables
def transform_and_load(cursor):
    print("Transforming and loading data into country-specific tables...")
    
    # USA Table
    cursor.execute("""
        INSERT INTO USA_Table (ID, Name, Vaccination_Type, Vaccination_Date)
        SELECT ID, Name, Vaccination_Type, Vaccination_Date
        FROM Staging_Table
        WHERE Country = 'USA';
    """)

    # India Table
    cursor.execute("""
        INSERT INTO India_Table (ID, Name, DOB, Vaccination_Type, Vaccination_Date, Free_Or_Paid)
        SELECT ID, Name, DOB, Vaccination_Type, Vaccination_Date, Free_Or_Paid
        FROM Staging_Table
        WHERE Country = 'India';
    """)

    # AUS Table
    cursor.execute("""
        INSERT INTO AUS_Table (ID, Name, DOB, Vaccination_Type, Vaccination_Date)
        SELECT ID, Name, DOB, Vaccination_Type, Vaccination_Date
        FROM Staging_Table
        WHERE Country = 'AUS';
    """)

    print("Data loaded into country-specific tables successfully!")

# Main workflow
def main():
    usa_data = [
        (1, 'Sam', 'EFG', '6152022'),
        (2, 'John', 'XYZ', '1052022'),
        (3, 'Mike', 'ABC', '12282021')
    ]
    india_data = [
        (1, 'Vikas', '12/1/1998', 'XYZ', '1/1/2022', 'F'),
        (2, 'Rahul', '8/13/1982', 'ABC', '3/5/2022', 'P'),
        (3, 'Sameer', '8/13/1952', 'ABC', '2/20/2022', 'F')
    ]
    aus_data = [
        (1, 'Mike', 'LMN', None, '5/11/2022'),
        (2, 'Jonnathan', 'XYZ', '12/13/1997', '2021-13-13'),
        (3, 'Cristina', 'ABC', '3/12/1998', '3/12/2022')
    ]

    # Connect to Snowflake
    conn = connect_to_snowflake()
    cursor = conn.cursor()

    try:
        # Step 1: Create tables
        create_tables(cursor)

        # Step 2: Normalize and load data into Staging Table
        load_to_staging(cursor, normalize_data(usa_data, "USA"))
        load_to_staging(cursor, normalize_data(india_data, "India"))
        load_to_staging(cursor, normalize_data(aus_data, "AUS"))

        # Step 3: Transform and load into country-specific tables
        transform_and_load(cursor)

    finally:
        # Close the connection
        cursor.close()
        conn.close()
        print("Connection closed.")

# Run the script
if __name__ == "__main__":
    main()
