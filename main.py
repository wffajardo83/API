from fastapi import FastAPI, File, UploadFile, HTTPException
import mysql.connector
import pandas as pd
import io


# Create an instance of the FastAPI class
app = FastAPI()

# Define a route for the root URL ("/")
@app.get("/hello")
def read_root():
    return {"message": "Hello, World!"}

# MySQL database configuration
db_config = {
    'host': '34.121.211.184',
    'user': 'admin',
    'password': 'admin',
    'database': 'globant'
}

# Function to connect to the MySQL database
def get_db_connection():
    return mysql.connector.connect(**db_config)

# Endpoint to upload CSV and insert data into MySQL
@app.post("/upload-csv/")
async def upload_csv(file: UploadFile = File(...)):
    # Ensure the file is a CSV
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a CSV")

    # Read the CSV file into a pandas DataFrame
    contents = await file.read()
    df = pd.read_csv(io.StringIO(contents.decode('utf-8')))

    # Connect to the MySQL database
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        # Insert data into the database
        for _, row in df.iterrows():
            query = """
            INSERT INTO jobs (job_id, job_name)
            VALUES (%s, %s)
            """
            cursor.execute(query, tuple(row))

        # Commit the transaction
        connection.commit()
    except Exception as e:
        # Rollback in case of error
        connection.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # Close the database connection
        cursor.close()
        connection.close()

    return {"message": "CSV data inserted successfully"}





