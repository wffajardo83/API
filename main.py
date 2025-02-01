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


     #   csv_columns = df.columns.tolist()
     #   placeholders = ", ".join(["%s"] * len(csv_columns))
     #   columns_str = ", ".join(csv_columns)
     #   insert_query = f"INSERT INTO {l_file} ({columns_str}) VALUES ({placeholders})"
     #   cursor.executemany(insert_query,df.values.tolist())
        
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
       
    





