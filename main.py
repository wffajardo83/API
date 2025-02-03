from fastapi import FastAPI, File, UploadFile, HTTPException, Form
from fastapi.responses import JSONResponse
from upload import Upload
from pydantic import BaseModel
import mysql.connector
import pandas as pd
import io
import avro.schema
import avro.io
import os
import fastavro
import datetime

BACKUP_DIR = 'data/backup'

class Item(BaseModel):
    report_id: int

class Table(BaseModel):
    table_name: str

class TableRestore(BaseModel):
    table_name: str
    file_name: str

# Create an instance of the FastAPI class
app = FastAPI()



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


# Define a route for the root URL ("/")
@app.get("/hello")
def read_root():
    return {"message": "Hello, World!"}


# Endpoint to upload CSV and insert data into MySQL
@app.post("/upload-csv/")
async def upload_csv(file: UploadFile = File(...)):
    # Ensure the file is a CSV
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a CSV")
    
    l_file = file.filename
    l_file = l_file[:-4]
    print(l_file)
    valid_files = ['departments', 'jobs', 'hired_employees']
    if l_file not in valid_files:
        raise HTTPException(status_code=400, detail="File name not accepted")


    # file jobs

    

    # Read the CSV file into a pandas DataFrame
    contents = await file.read()
    
    if l_file =="jobs":
        df = pd.read_csv(io.StringIO(contents.decode('utf-8')),header=None,names=["job_id","job_name"])

    if l_file =="departments":
        df = pd.read_csv(io.StringIO(contents.decode('utf-8')),header=None,names=["department_id","department_name"])

    if l_file =="hired_employees":
        df = pd.read_csv(io.StringIO(contents.decode('utf-8')),header=None,names=["id","employee_name","date_hired","department_id","job_id"])
        df["date_hired"] = df["date_hired"].str[:-1]
        df["department_id"] = df["department_id"].fillna(0).astype(int).replace(0, None)
        df["job_id"] = df["job_id"].fillna(0).astype(int).replace(0, None)        

        for col in ['department_id','job_id']:
            for index, value in df[col].isna().items():
                if value:
                    print(f"Missing value found in column '{col}' at index {index}")

    

    # Connect to the MySQL database
    connection = get_db_connection()
    cursor = connection.cursor()
    

    try:
        batch_size = 1000
        rows_to_insert = [tuple(row[col] for col in df.columns) for _, row in df.iterrows()]

        for i in range(0, len(rows_to_insert), batch_size):
            batch = rows_to_insert[i:i + batch_size]
            filtered_batch = []

            for row in batch:
                # Verificar si el 'id' ya existe en la tabla antes de insertar
                if l_file == 'hired_employees':
                    if row[4]!= None: 
                        if row[3]!= None:
                            cursor.execute("SELECT COUNT(*) FROM departments WHERE department_id = %s", (row[3],))
                            result = cursor.fetchone()
                            cursor.execute("SELECT COUNT(*) FROM jobs WHERE job_id = %s", (row[4],))
                            result2 = cursor.fetchone()
                            if result[0] != 0 and result2[0] != 0 :  # Si el id de departamento y el id job existen, agregar a filtered_batch
                                filtered_batch.append(row) 
                            else:
                                print(f"Registro con error en identidad referencial '{row[0]}'")
                        else:
                            print(f"Vacio en department id registro'{row[0]}'")
                    else:
                        print(f"Vacio en job_id registro '{row[0]}'")
                else :
                    filtered_batch.append(row)

            if filtered_batch:
                # Preparar la consulta de inserción para solo los registros no duplicados
                csv_columns = df.columns.tolist()
                placeholders = ", ".join(["%s"] * len(csv_columns))
                columns_str = ", ".join(csv_columns)
                insert_query = f"INSERT INTO {l_file} ({columns_str}) VALUES ({placeholders})"
                print(insert_query)
                
                cursor.executemany(insert_query, filtered_batch)
        
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

@app.post("/backup")       
async def backupAVRO(table: Table):

    table_name = table.table_name
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)        

        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()

        if not rows:
            return f"No data found in table {table_name}."


        cursor.execute(f"DESCRIBE {table_name}")
        table_description = cursor.fetchall()        
        schema_fields = []
        for column in table_description:
            column_name = column['Field']            
            column_type = column['Type']            
            if 'int' in column_type:
                schema_fields.append({'name': column_name, 'type': ['null', 'int']})
            elif 'float' in column_type or 'double' in column_type:
                schema_fields.append({'name': column_name, 'type': ['null', 'float']})
            elif 'varchar' in column_type or 'text' in column_type or 'char' in column_type:
                schema_fields.append({'name': column_name, 'type': ['null', 'string']})
            elif 'date' in column_type or 'datetime' in column_type:
                schema_fields.append({'name': column_name, 'type': ['null', 'string']})
            else:
                schema_fields.append({'name': column_name, 'type': ['null', 'string']})

        schema = {
            'name': table_name,
            'type': 'record',
            'fields': schema_fields
        }


        if not os.path.exists(BACKUP_DIR):
            os.makedirs(BACKUP_DIR)


        backup_file = os.path.join(BACKUP_DIR, f"{table_name}.avro")


        for row in rows:
            for key, value in row.items():
                if isinstance(value, datetime.datetime):
                    row[key] = value.strftime('%Y-%m-%d %H:%M:%S')  


        with open(backup_file, 'wb') as out:
            fastavro.writer(out, schema, rows)  

        return f"Backup for table {table_name} saved as {backup_file}"

    except mysql.connector.Error as err:
        return f"Database error: {str(err)}"
    except Exception as e:
        return f"Error during backup: {type(e).__name__} - {str(e)}"
    finally:
        if cursor:
            cursor.close()

@app.post("/restore")
async def restoreAVRO(file: UploadFile = File(...), table_name: str = Form(...)):

    file_content = await file.read()
    file_stream = io.BytesIO(file_content)

    cursor = None
    connection = get_db_connection()
    if table_name not in ['departments', 'jobs', 'hired_employees']:
        return {"error": "Invalid table name"}, 400

    if not file.filename.endswith('.avro'):
        return {"error": "File is not an AVRO"}

    try:
        reader = fastavro.reader(file_stream)
        cursor = connection.cursor()

        for record in reader:
            columns = ', '.join(record.keys())
            placeholders = ', '.join(['%s'] * len(record))
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            cursor.execute(insert_query, list(record.values()))

        connection.commit()
        return {"message": f"Data restored successfully to {table_name}"}

    except mysql.connector.Error as err:
        connection.rollback()
        return {"error": f"Database error: {str(err)}"}
    except Exception as e:
        return {"error": f"Error restoring data: {str(e)}"}
    finally:
        if cursor:
            cursor.close()
            connection.close()

@app.post("/reports")
async def reports(item: Item):
    if item.report_id == 1:
        cursor = None
        connection = get_db_connection()
        try:            
            cursor = connection.cursor()            

            query = """
            SELECT department,
                job,
                Q1,
                Q2,
                Q3,
                Q4
            FROM uvw_quarter_hires;            
            """

            cursor.execute(query)
            rows = cursor.fetchall()

            if not rows:
                return {"message": f"No data found for report 1."}

            return rows

        except mysql.connector.Error as err:
            return ({"error": f"Database error: {str(err)}"}), 500
        except Exception as e:
            return ({"error": f"Error during processing: {type(e).__name__} - {str(e)}"}), 500
        finally:
            if cursor:
                cursor.close()
        
    else:
        cursor = None
        connection = get_db_connection()
        try:            
            cursor = connection.cursor()            

            query = """
            SELECT id,
                department,
                hired 
            FROM uvw_hires_beyond_mean;            
            """

            cursor.execute(query)
            rows = cursor.fetchall()

            if not rows:
                return {"message": f"No data found for report 2."}

            return rows

        except mysql.connector.Error as err:
            return ({"error": f"Database error: {str(err)}"}), 500
        except Exception as e:
            return ({"error": f"Error during processing: {type(e).__name__} - {str(e)}"}), 500
        finally:
            if cursor:
                cursor.close()
        
    



