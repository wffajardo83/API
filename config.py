import mysql.connector

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