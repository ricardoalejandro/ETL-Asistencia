import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import logging
from typing import Optional
from extract import load_config, check_required_directories

logger = logging.getLogger('ETL-Process.Load')

def get_google_client() -> gspread.Client:
    """
    Configura y retorna el cliente de Google Sheets
    """
    try:
        logger.info("Configurando cliente de Google Sheets")
        config = load_config()
        
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        logger.debug("Configurando credenciales con scope de Google Sheets y Drive")
        
        credentials_dict = config['google_services']['client_id']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
        client = gspread.authorize(creds)
        logger.info("Cliente de Google Sheets configurado exitosamente")
        
        return client
    except Exception as e:
        logger.error("Error configurando cliente de Google Sheets", exc_info=True)
        raise

def load_to_sheets(archivo_entrada: str, spreadsheet_id: str = "1KyRGrnkql19dQYnnPxmecLd3hQ7Cn2fLJ8BOBLHKtMA") -> bool:
    """
    Carga los datos del archivo Excel procesado a Google Sheets
    """
    try:
        logger.info(f"Iniciando carga de datos desde: {os.path.basename(archivo_entrada)}")
        
        # Leer el archivo Excel
        logger.debug("Leyendo archivo Excel")
        df = pd.read_excel(archivo_entrada)
        logger.info(f"Datos leídos del Excel, shape: {df.shape}")

        # Obtener cliente de Google Sheets
        client = get_google_client()

        # Abrir la hoja de Google Sheets usando el ID
        logger.info(f"Conectando con Google Sheet ID: {spreadsheet_id}")
        spreadsheet = client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.sheet1
        logger.debug(f"Conectado a hoja: {worksheet.title}")

        # Convertir los datos a un formato serializable
        def convert_to_serializable(val):
            if pd.isna(val):
                return ''
            elif isinstance(val, pd.Timestamp):
                return val.strftime('%Y-%m-%d %H:%M:%S')
            else:
                return str(val)

        # Aplicar la conversión a todos los datos
        logger.info("Preparando datos para la carga")
        values = [[convert_to_serializable(val) for val in row] for row in df.values]
        headers = df.columns.values.tolist()

        # Limpiar la hoja de Google Sheets
        logger.info("Limpiando contenido existente en Google Sheets")
        worksheet.clear()

        # Escribir los datos en Google Sheets
        logger.info(f"Cargando {len(values)} filas de datos")
        worksheet.update([headers] + values)
        
        logger.info(f"Datos cargados exitosamente en: {spreadsheet.url}")
        return True

    except Exception as e:
        logger.error(f"Error cargando datos a Google Sheets: {str(e)}", exc_info=True)
        return False

def load_data(archivo_procesado: str) -> bool:
    """
    Función principal para cargar los datos
    """
    try:
        logger.info("=== INICIANDO PROCESO DE CARGA ===")
        
        # Verificar que el archivo existe
        if not os.path.exists(archivo_procesado):
            msg = f"El archivo procesado no existe: {archivo_procesado}"
            logger.error(msg)
            raise ValueError(msg)

        # Cargar los datos a Google Sheets
        result = load_to_sheets(archivo_procesado)
        
        if result:
            logger.info("=== PROCESO DE CARGA COMPLETADO EXITOSAMENTE ===")
        else:
            logger.error("=== PROCESO DE CARGA FALLIDO ===")
            
        return result

    except Exception as e:
        logger.error(f"Error en la carga de datos: {str(e)}", exc_info=True)
        return False

def main():
    """
    Punto de entrada principal cuando se ejecuta como script
    """
    logger.info("Este módulo debe ser importado y utilizado desde el flujo principal de ETL")

if __name__ == "__main__":
    main()