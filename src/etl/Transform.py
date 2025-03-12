import os
import pandas as pd
from datetime import datetime
import json
import logging
from typing import List, Dict
from extract import load_config, check_required_directories

logger = logging.getLogger('ETL-Process.Transform')

def procesar_excel(archivo_excel: str, nombre_sheet: str) -> pd.DataFrame:
    """
    Procesa un archivo Excel y retorna un DataFrame con los datos transformados
    """
    try:
        logger.info(f"Procesando archivo: {os.path.basename(archivo_excel)}, hoja: {nombre_sheet}")
        
        # Leemos excel
        df = pd.read_excel(
            archivo_excel,
            sheet_name=nombre_sheet, 
            header=None,  
            skiprows=2,  
            nrows=100,  
            usecols="A:AS"
        )
        logger.debug(f"Datos leídos del Excel, shape inicial: {df.shape}")

        # Establecer la primera fila del rango como encabezado
        df.columns = df.iloc[0]
        df = df[1:]

        # Filtramos los Preinscritos
        pre_filter_count = len(df)
        df = df[df['Tipo Incrito'] != 'Pre-Inscrito']
        post_filter_count = len(df)
        logger.info(f"Filtrados {pre_filter_count - post_filter_count} registros pre-inscritos")

        # Seleccionamos las columnas elegidas
        columnas = ["Mes Inscrito", "Mes de Alta como miembro", "Dia de clases Inscrito", "Grupo", "DNI / CE", 
                   "Nombres", "Apellidos", "Edad", "sem 01", "sem 02", "sem 03", "sem 04", "sem 05", "sem 06", 
                   "sem 07", "sem 08", "sem 09", "sem 10", "sem 11", "sem 12"]
        df_final = df[columnas]

        encabezados = ["MesInscrito", "MesAlta", "DiaClase", "Grupo", "DNI", "Nombres", "Apellidos", "Edad",
                    "C01", "C02", "C03", "C04", "C05", "C06", "C07", "C08", "C09", "C10", "C11", "C12"]
        df_final.columns = encabezados
 
        # Limpieza de datos
        rows_before = len(df_final)
        df_final = df_final.dropna(how="all")
        rows_after = len(df_final)
        if rows_before != rows_after:
            logger.info(f"Se eliminaron {rows_before - rows_after} filas vacías")
            
        df_final = df_final.fillna("")
        logger.debug(f"Shape final del DataFrame: {df_final.shape}")

        return df_final

    except Exception as e:
        logger.error(f"Error procesando Excel {archivo_excel}: {str(e)}", exc_info=True)
        raise

def contar_presente(df: pd.DataFrame) -> pd.Series:
    """
    Cuenta los presentes de la C01 a la C12
    """
    columnas_clases = [f'C{i:02d}' for i in range(1, 13)]
    return df[columnas_clases].apply(lambda x: (x == 'P').sum())

def procesar_archivos(archivos: List[str], hoja_excel: str = "Probacionistas") -> pd.DataFrame:
    """
    Procesa una lista de archivos Excel y retorna un DataFrame consolidado
    """
    logger.info(f"Iniciando procesamiento de {len(archivos)} archivos")
    dataframes = []

    for archivo in archivos:
        try:
            logger.info(f"Procesando archivo: {os.path.basename(archivo)}")
            df = procesar_excel(archivo, hoja_excel)
            
            nombre_archivo = os.path.basename(archivo)
            filial = nombre_archivo.split('-')[0].strip()
            logger.debug(f"Filial extraída del nombre: {filial}")
            
            df_grupos = df.groupby(['MesInscrito', 'DiaClase', 'Grupo'])
            resultado = df_grupos.apply(contar_presente).reset_index()
            resultado['Inscritos'] = df_grupos.size().reset_index(drop=True)
            resultado['Filial'] = filial

            dataframes.append(resultado)
            logger.info(f"Archivo {os.path.basename(archivo)} procesado exitosamente")
            
        except Exception as e:
            logger.error(f"Error procesando archivo {archivo}: {str(e)}", exc_info=True)
            continue

    if not dataframes:
        msg = "No se pudo procesar ningún archivo correctamente"
        logger.error(msg)
        raise ValueError(msg)

    logger.info("Combinando resultados de todos los archivos")
    dataframe_final = pd.concat(dataframes, ignore_index=True)
    
    # Transformar columnas en filas
    logger.info("Transformando estructura de datos (unpivot)")
    df_unpivot = pd.melt(
        dataframe_final,
        id_vars=['Filial', 'MesInscrito', 'DiaClase', 'Grupo', 'Inscritos'],
        value_vars=[f'C{i:02d}' for i in range(1, 13)],
        var_name='Clase',
        value_name='Asistentes'
    )

    resultado_final = df_unpivot.sort_values(by=['Filial', 'MesInscrito', 'Grupo', 'Clase'])
    logger.info(f"Transformación completada. Shape final: {resultado_final.shape}")
    
    return resultado_final

def transform_data(input_data: Dict[str, any]) -> str:
    """
    Función principal que transforma los datos
    """
    try:
        logger.info("Iniciando proceso de transformación de datos")
        
        if not input_data or 'download_folder' not in input_data or 'files' not in input_data:
            msg = "Datos de entrada incorrectos o incompletos"
            logger.error(msg)
            raise ValueError(msg)

        logger.info(f"Procesando {len(input_data['files'])} archivos de {input_data['download_folder']}")
        
        # Cargar configuración y verificar directorios
        config = load_config()
        dirs = check_required_directories()

        # Procesar los archivos
        df_final = procesar_archivos(input_data['files'])

        # Definir el nombre del archivo de salida
        fecha_hora = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        nombre_archivo = f'transformado_{fecha_hora}.xlsx'
        ruta_salida = os.path.join(dirs['processed_dir'], nombre_archivo)

        # Exportar a excel
        logger.info(f"Guardando resultados en {nombre_archivo}")
        df_final.to_excel(ruta_salida, index=False)
        logger.info(f"Archivo transformado guardado exitosamente")

        return ruta_salida

    except Exception as e:
        logger.error(f"Error en la transformación de datos: {str(e)}", exc_info=True)
        return None

def main():
    logger.info("Este módulo debe ser importado y utilizado desde el flujo principal de ETL")

if __name__ == "__main__":
    main()