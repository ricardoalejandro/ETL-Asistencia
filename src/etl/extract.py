import os
import datetime
import requests
import pandas as pd
import openpyxl
import json
import logging
from urllib.parse import urlparse, parse_qs
from io import BytesIO

logger = logging.getLogger('ETL-Process.Extract')

def load_config():
    """
    Carga el archivo de configuración desde la raíz del proyecto
    """
    # Get the absolute path to the root of the project (2 levels up from the current script)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    config_path = os.path.join(project_root, 'config.json')
    
    with open(config_path, 'r') as f:
        return json.load(f)

def check_required_directories():
    """
    Verifica si los directorios requeridos existen, si no, los crea
    """
    config = load_config()
    # Obtenemos la ruta raíz del proyecto
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    
    # Rutas absolutas para los directorios
    required_dirs = [
        os.path.join(project_root, config['paths']['downloads_dir']),
        os.path.join(project_root, config['paths']['logs_dir']),
        os.path.join(project_root, config['paths']['processed_dir'])
    ]
    
    for directory in required_dirs:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"Directorio creado: {directory}")
        else:
            print(f"Directorio ya existe: {directory}")
    
    return {
        'downloads_dir': os.path.join(project_root, config['paths']['downloads_dir']),
        'logs_dir': os.path.join(project_root, config['paths']['logs_dir']),
        'processed_dir': os.path.join(project_root, config['paths']['processed_dir'])
    }

def create_folder():
    """
    Crea una carpeta para almacenar los archivos descargados con un timestamp
    """
    # Cargar configuración
    config = load_config()
    
    # Obtener ruta absoluta al directorio de descargas
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    downloads_dir = os.path.join(project_root, config['paths']['downloads_dir'])
    
    # Crear carpeta con formato data_probacionismo_yyyymmdd_hhmmss
    current_time = datetime.datetime.now()
    folder_name = f"data_probacionismo_{current_time.strftime('%Y%m%d_%H%M%S')}"
    full_path = os.path.join(downloads_dir, folder_name)
    
    if not os.path.exists(full_path):
        os.makedirs(full_path)
    
    return full_path, current_time

def get_direct_download_url(url):
    """Convertir enlace de OneDrive compartido a enlace de descarga directa"""
    if 'onedrive.live.com' in url:
        # Extraer el enlace compartido de la URL si existe
        if '1drv.ms' in url:
            return url  # Devolver URLs cortas tal cual
            
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query)
        
        if 'id' in query_params:
            file_id = query_params['id'][0]
            # Usar el formato de URL corto, que es más confiable
            return f"https://1drv.ms/{file_id}"
            
    return url

def format_sede_name(sede):
    """Da formato al nombre de la sede reemplazando espacios y guiones consecutivos con un solo guión bajo"""
    # Primero reemplazar todos los guiones con espacios
    sede = sede.replace('-', ' ')
    # Luego dividir por cualquier número de espacios y unir con un solo guión bajo
    return '_'.join(word for word in sede.split() if word)

def save_downloaded_file(excel_content, folder_path, sede, timestamp):
    """
    Guarda el archivo descargado sin procesamiento
    """
    try:
        config = load_config()
        nivel = config['excel_urls'][sede]['nivel']
        
        formatted_sede = format_sede_name(sede)
        file_name = f"{formatted_sede}-{nivel}-{timestamp.strftime('%Y%m%d_%H%M%S')}.xlsx"
        file_path = os.path.join(folder_path, file_name)
        
        # Guardar el contenido directamente
        with open(file_path, 'wb') as f:
            f.write(excel_content)
            
        logger.info(f"Archivo guardado: {file_name}")
        return file_path
        
    except Exception as e:
        logger.error(f"Error guardando archivo para {sede}: {str(e)}", exc_info=True)
        return False

def download_and_process_file(url, folder_path, sede, timestamp):
    try:
        logger.info(f"Iniciando descarga para sede: {sede}")
        download_url = get_direct_download_url(url)
        logger.debug(f"URL de descarga: {download_url}")
        
        session = requests.Session()
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }
        
        response = session.get(download_url, headers=headers, allow_redirects=True)
        response.raise_for_status()
        
        final_url = response.url
        logger.debug(f"URL final después de redirecciones: {final_url}")
        
        if '1drv.ms' in download_url:
            if 'sharepoint.com' in final_url or 'onedrive.live.com' in final_url:
                parsed = urlparse(final_url)
                query_params = parse_qs(parsed.query)
                
                if 'share' in query_params:
                    share_token = query_params['share'][0]
                    share_api_url = f"https://api.onedrive.com/v1.0/shares/{share_token}/driveItem/content"
                    logger.debug(f"Usando API de compartir: {share_api_url}")
                    response = session.get(share_api_url, headers=headers)
                    response.raise_for_status()
                
                elif 'resid' in query_params and 'authkey' in query_params:
                    resid = query_params['resid'][0]
                    authkey = query_params['authkey'][0].replace('!', '')
                    
                    urls_to_try = [
                        f"https://onedrive.live.com/download?resid={resid}&authkey={authkey}",
                        f"https://onedrive.live.com/download.aspx?resid={resid}&authkey={authkey}",
                        f"https://api.onedrive.com/v1.0/drives/items/{resid}/content"
                    ]
                    
                    for try_url in urls_to_try:
                        try:
                            logger.debug(f"Intentando URL alternativa: {try_url}")
                            response = session.get(try_url, headers=headers)
                            response.raise_for_status()
                            
                            if 'text/html' in response.headers.get('Content-Type', ''):
                                continue
                                
                            if response.content.startswith(b'PK'):
                                logger.info("Archivo Excel encontrado correctamente")
                                break
                        except Exception as e:
                            logger.debug(f"Error con URL {try_url}: {str(e)}")
                            continue
        
        content_type = response.headers.get('Content-Type', '')
        if not response.content.startswith(b'PK'):
            logger.warning(f"Tipo de contenido inesperado: {content_type}")
            
            if '?' in final_url:
                final_url += '&download=1'
            else:
                final_url += '?download=1'
            logger.debug(f"Intentando descarga directa: {final_url}")
            response = session.get(final_url, headers=headers)
            response.raise_for_status()
            
            if not response.content.startswith(b'PK'):
                raise ValueError(f"El contenido descargado para {sede} no es un archivo Excel válido")
        
        logger.info(f"Archivo descargado correctamente para {sede}")
        # Cambiamos process_excel_file por save_downloaded_file
        return save_downloaded_file(response.content, folder_path, sede, timestamp)
    
    except Exception as e:
        logger.error(f"Error descargando archivo para {sede}: {str(e)}", exc_info=True)
        if 'response' in locals():
            logger.error(f"Headers de respuesta: {response.headers}")
            logger.error(f"URL de respuesta: {response.url}")
        return False

def download_excel_files():
    """
    Ejecuta el proceso de descarga de todos los archivos Excel configurados.
    """
    try:
        logger.info("Iniciando proceso de descarga de archivos Excel")
        config = load_config()
        excel_urls = config['excel_urls']
        
        folder_path, timestamp = create_folder()
        logger.info(f"Carpeta creada para descargas: {folder_path}")
        
        successful_downloads = 0
        total_files = len(excel_urls)
        downloaded_files = []
        
        for sede, info in excel_urls.items():
            logger.info(f"Procesando sede: {sede} (Nivel {info['nivel']})")
            download_url = info['url']
            file_path = download_and_process_file(download_url, folder_path, sede, timestamp)
            
            if file_path:
                successful_downloads += 1
                downloaded_files.append(file_path)
                logger.info(f"Procesamiento exitoso para {sede}")
        
        logger.info(f"Proceso de descarga completado. {successful_downloads} de {total_files} archivos procesados")
        
        return {
            'download_folder': folder_path,
            'files': downloaded_files
        }
        
    except Exception as e:
        logger.error(f"Error en el proceso de descarga: {str(e)}", exc_info=True)
        return None

def main():
    try:
        logger.info("=== INICIANDO PROCESO DE EXTRACCIÓN ===")
        start_time = datetime.datetime.now()
        
        dirs = check_required_directories()
        logger.info("Directorios verificados y creados si es necesario")
        
        download_result = download_excel_files()
        
        if download_result:
            end_time = datetime.datetime.now()
            duration = end_time - start_time
            logger.info(f"=== PROCESO DE EXTRACCIÓN COMPLETADO ({duration.total_seconds():.3f} segundos) ===")
            return download_result
        else:
            logger.error("=== PROCESO DE EXTRACCIÓN FALLIDO ===")
            return None
            
    except Exception as e:
        logger.error(f"Error en el proceso principal de extracción: {str(e)}", exc_info=True)
        return None

if __name__ == "__main__":
    main()