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
        articulos_query = 'SELECT * FROM ARTICULOS'
        canal_query = 'SELECT * FROM CANAL'
        detalle_venta_query = 'SELECT * FROM DETALLE_VENTA'
        vendedores_query = 'SELECT * FROM VENDEDORES'
        ventas_query = 'SELECT * FROM VENTAS'
        

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
        
        # ARTICULOS
        df_articulos = pd.read_sql(articulos_query, cnx)
        
        # CANAL
        df_canal = pd.read_sql(canal_query, cnx)
        
        # DETALLE_VENTA
        df_detalle_venta = pd.read_sql(detalle_venta_query, cnx)
        
        # VENDEDORES
        df_vendedores = pd.read_sql(vendedores_query, cnx)
        # VENTAS
        df_ventas = pd.read_sql(ventas_query, cnx)
        
        

        # guardar ubicaciones en CSV
        df_final.to_csv('ubicaciones.csv', index=False, sep=';', encoding='utf-8-sig' )
        print('\nSe guardaron los datos de ubicaciones en ubicaciones.csv')
        # guardar clientes en CSV
        df_clientes.to_csv('clientes.csv', index=False, sep=';', encoding='utf-8-sig' )
        print('\nSe guardaron los datos de clientes en clientes.csv')
        # guardar articulos en CSV
        df_articulos.to_csv('articulos.csv', index=False, sep=';', encoding='utf-8-sig' )
        print('\nSe guardaron los datos de articulos en articulos.csv')
        # guardar canal en CSV
        df_canal.to_csv('canal.csv', index=False, sep=';', encoding='utf-8-sig' )
        print('\nSe guardaron los datos de canal en canal.csv')
        # guardar detalle_venta en CSV
        df_detalle_venta.to_csv('detalle_venta.csv', index=False, sep=';', encoding='utf-8-sig' )
        print('\nSe guardaron los datos de detalle_venta en detalle_venta.csv')
        # guardar vendedores en CSV
        df_vendedores.to_csv('vendedores.csv', index=False, sep=';', encoding='utf-8-sig' )
        print('\nSe guardaron los datos de vendedores en vendedores.csv')
        # guardar ventas en CSV
        df_ventas.to_csv('ventas.csv', index=False, sep=';', encoding='utf-8-sig' )
        print('\nSe guardaron los datos de ventas en ventas.csv')
        
        


    except Exception as e:
        print("Algo salió mal\n\n", e)
        return None

if __name__ == '__main__':
    get_data()
