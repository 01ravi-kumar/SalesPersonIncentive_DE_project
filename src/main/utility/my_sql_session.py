import mysql.connector

def get_mysql_connection():
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="password",
        database="de_db"
        # ssl_disabled=True
    )
    return connection



# create a database in docker container
# docker run --name mysql1 -e MYSQL_ROOT_PASSWORD=password  -e MYSQL_DATABASE=de_db -p 3306:3306 -v ./db_mount:/var/lib/mysql -d mysql:latest

# go inside the container
# docker exec -it mysql2 bash

# run the mysql shell
# mysql -p
# enter the password 

# select the our created database which is de_db
# use de_db











# connection = mysql.connector.connect(
#     host="localhost",
#     user="root",
#     password="password",
#     database="manish"
# )
#
# # Check if the connection is successful
# if connection.is_connected():
#     print("Connected to MySQL database")
#
# cursor = connection.cursor()
#
# # Execute a SQL query
# query = "SELECT * FROM manish.testing"
# cursor.execute(query)
#
# # Fetch and print the results
# for row in cursor.fetchall():
#     print(row)
#
# # Close the cursor
# cursor.close()
#
# connection.close()
