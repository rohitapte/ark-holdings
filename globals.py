from mysql.connector import connect

cxn = connect(host="localhost",
                user="arkuser",
                password="arkuser123",
                database='arkdb'
              )