import pyodbc

def getConnection():
    return pyodbc.connect("""Driver={ODBC Driver 17 for SQL Server};
                        Server=tcp:prenaissance.database.windows.net,1433;
                        Database=customerpp;Uid=alex;Pwd=Test1234;Encrypt=yes;
                        TrustServerCertificate=no;Connection Timeout=30;""")