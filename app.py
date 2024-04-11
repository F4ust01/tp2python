import csv
import MySQLdb
import sys
import os

# Crear el directorio 'localidades_por_provincia' si no existe
directorio_salida = 'localidades_por_provincia'
if not os.path.exists(directorio_salida):
    os.makedirs(directorio_salida)

# Abre el archivo CSV en modo lectura
with open('localidades.csv', 'r', newline='') as csvfile:
    spamreader = csv.reader(csvfile)
    next(spamreader)  # Omitir la primera fila que contiene encabezados

    # La lectura del archivo CSV debe estar dentro de este bloque

    try:
        # Conectarse a la base de datos MySQL
        db = MySQLdb.connect("localhost", "root", "", "localidadesdb")
        print("Conexión correcta.")
    except MySQLdb.Error as e:
        print("No se pudo conectar a la base de datos:", e)
        sys.exit(1)

    cursor = db.cursor()

    # Eliminar la tabla si ya existe
    sql_drop_table = "DROP TABLE IF EXISTS localidades"
    try:
        cursor.execute(sql_drop_table)
        print("Tabla eliminada exitosamente si existía.")
    except Exception as e:
        print("Error al eliminar la tabla:", e)

    # Crear una tabla en la base de datos
    sql_create_table = """
        CREATE TABLE localidades (
            provincia TEXT,
            id INTEGER,
            localidad TEXT,
            cp INTEGER,
            id_prov_mstr INTEGER
        )
    """

    # Ejecutar la sentencia SQL para crear la tabla
    try:
        cursor.execute(sql_create_table)
        # Confirmar los cambios en la base de datos
        db.commit()
        print("Tabla creada exitosamente.")
    except Exception as e:
        # En caso de error, revertir los cambios
        db.rollback()
        print("Error al crear la tabla:", e)

    # Iterar sobre cada fila en el archivo CSV y realizar inserciones
    for fila in spamreader:
        # Insertar los datos en la tabla de la base de datos
        try:
            cursor.execute("INSERT INTO localidades (provincia, id, localidad, cp, id_prov_mstr) VALUES (%s, %s, %s, %s, %s)", fila)
            db.commit()
        except Exception as e:
            # En caso de error, revertir los cambios
            db.rollback()
            print("Error al insertar fila:", e)

    # Consultar las provincias disponibles en la base de datos
    cursor.execute("SELECT DISTINCT provincia FROM localidades")
    provincias = cursor.fetchall()

    # Exportar cada grupo de localidades por provincia a archivos CSV separados
    for provincia in provincias:
        provincia_nombre = provincia[0]
        cursor.execute("SELECT * FROM localidades WHERE provincia = %s", (provincia_nombre,))
        localidades_provincia = cursor.fetchall()

        # Nombre del archivo CSV para esta provincia
        archivo_csv = provincia_nombre.replace(" ", "_") + ".csv"

        # Ruta completa del archivo CSV
        ruta_archivo = os.path.join(directorio_salida, archivo_csv)

        # Escribir los datos en el archivo CSV
        with open(ruta_archivo, 'w', newline='') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(['provincia', 'id', 'localidad', 'cp', 'id_prov_mstr'])
            csv_writer.writerows(localidades_provincia)

        print(f"Archivo CSV exportado para {provincia_nombre}: {ruta_archivo}")

    # Cerrar la conexión con la base de datos
    db.close()
