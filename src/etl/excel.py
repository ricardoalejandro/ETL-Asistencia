import os
import pandas as pd
from funciones import procesar_excel
from datetime import datetime

# Parametros iniciales
carpeta_input = "files"

carpeta_output = "summary"
hojaExcel = "Probacionistas"

# Listar archivos
archivos_excel = [archivo for archivo in os.listdir(carpeta_input) if archivo.endswith(".xlsx")]

# Lista para acumular los DataFrames
dataframes = []

# Iterar sobre cada archivo Excel
for archivo in archivos_excel:
    # Ruta completa al archivo
    ruta_archivo = os.path.join(carpeta_input, archivo)
    
    # Procesar el archivo y obtener el DataFrame
    df = procesar_excel(ruta_archivo, hojaExcel)
    
    # Agregar el DataFrame a la lista
    dataframes.append(df)

# Combinar todos los DataFrames en uno solo
dataframe_final = pd.concat(dataframes, ignore_index=True)

# Transformamos columnas en filas 
df_unpivot = pd.melt(
    dataframe_final,
    id_vars=['Filial', 'MesInscrito', 'DiaClase', 'Grupo', 'Inscritos'],  
    value_vars=['C01', 'C02', 'C03', 'C04', 'C05', 'C06', 'C07', 'C08', 'C09', 'C10', 'C11', 'C12'],  
    var_name='Clase',  
    value_name='Asistentes'  
)

df_final = df_unpivot.sort_values(by=['Filial', 'MesInscrito', 'Grupo', 'Clase'])

# Define el nombre del archivo
fecha_hora_actual = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
nombre_archivo = f'exportado_{fecha_hora_actual}.xlsx'
ruta_completa = f'{carpeta_output}/{nombre_archivo}'

# Exportar a excel
df_final.to_excel(ruta_completa, index=False)

# print(df_unpivot)