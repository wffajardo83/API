from fastapi import FastAPI, HTTPException
import mysql.connector
import avro.schema
import avro.io
import io
import os

app = FastAPI()

# MySQL database configuration
db_config = {
    "host": "localhost",
    "user": "your_username",
    "password": "your_password",
    "database": "your_database"
}

# Avro schema for the table
avro_schema = {
    "type": "record",
    "name": "TableRecord",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"}
    ]
}

def fetch_table_data(table_name: str):
    """Fetch data from the specified MySQL table."""
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor(dictionary=True)

        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        rows = cursor.fetchall()

        cursor.close()
        connection.close()

        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def convert_to_avro(data, schema, output_file):
    """Convert data to Avro format and save to a file."""
    try:
        schema = avro.schema.parse(str(schema))
        with open(output_file, "wb") as f:
            writer = avro.io.DatumWriter(schema)
            encoder = avro.io.BinaryEncoder(f)
            for record in data:
                writer.write(record, encoder)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/create-avro-backup/")
async def create_avro_backup(table_name: str):
    # Fetch data from the MySQL table
    data = fetch_table_data(table_name)

    # Define the output Avro file path
    output_file = f"{table_name}_backup.avro"

    # Convert data to Avro format and save to file
    convert_to_avro(data, avro_schema, output_file)

    return {"message": f"Avro backup created successfully at {output_file}"}