from sqlalchemy import create_engine

def get_connection():
    connection_string = (
        "mssql+pyodbc://localhost/Americans?"
        "driver=ODBC+Driver+17+for+SQL+Server&Trusted_connection=yes"
    )

    try:
        conn = create_engine(connection_string)
        print("Conexion establecida")
        return conn
    except Exception as e:
        print("Algo salio mal", e)
        return None