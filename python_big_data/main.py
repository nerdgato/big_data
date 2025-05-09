from conexion import get_connection
import pandas as pd

def get_data():
    cnx = get_connection()
    if cnx is None:
        return

    try:
        # CLIENTES
        clientes_query = 'SELECT NRO, ID_CLIENTE, EDAD, RESIDENCIA, ORIGEN FROM CLIENTES'
        df_clientes = pd.read_sql(clientes_query, cnx)
        df_clientes['RESIDENCIA'] = df_clientes['RESIDENCIA'].str.strip()
        df_clientes['ORIGEN'] = df_clientes['ORIGEN'].str.strip()

        print('\nDatos cargados al DataFrame CLIENTES')
        print(df_clientes.head(10))

        # UBICACIONES
        comuna_query = 'SELECT * FROM COMUNA'
        provincia_query = 'SELECT * FROM PROVINCIA'
        region_query = 'SELECT * FROM REGION'
        sucursal_query = 'SELECT * FROM SUCURSAL'

        df_comuna = pd.read_sql(comuna_query, cnx)
        df_provincia = pd.read_sql(provincia_query, cnx)
        df_region = pd.read_sql(region_query, cnx)
        df_sucursal = pd.read_sql(sucursal_query, cnx)

        # hacer JOIN para unir todo en una tabla ubicaciones
        df = df_sucursal.merge(df_comuna, on='COMUNA_ID', how='left') \
                        .merge(df_provincia, on='PROVINCIA_ID', how='left') \
                        .merge(df_region, on='REGION_ID', how='left')

        print('\nDatos unidos en el DataFrame UBICACIONES (con IDs)')
        print(df.head(10))

        # guardar ubicaciones y renombrar id sucursal
        df_final = df[['SUCURSAL_ID', 'NOMBRE_SUCURSAL', 'CIUDAD_COMUNA', 'PROVINCIA', 'REGION']] \
            .rename(columns={'SUCURSAL_ID': 'UBICACION_ID'})

        print('\nDatos finales de UBICACIONES (columnas limpias y renombradas)')
        print(df_final.head(10))

        # guardar excel
        archivo_excel = 'Americans.xlsx'
        with pd.ExcelWriter(archivo_excel) as writer:
            df_clientes.to_excel(writer, sheet_name='Clientes', index=False)
            df_final.to_excel(writer, sheet_name='Ubicaciones', index=False)

        print(f'\nSe almacenaron los datos en {archivo_excel}')

    except Exception as e:
        print("Algo salió mal\n\n", e)
        return None

if __name__ == '__main__':
    get_data()
