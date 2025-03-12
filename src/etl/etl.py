from extract import main as extract_main, load_config, check_required_directories
from Transform import transform_data
from Load import load_data
import logging
import os
from datetime import datetime
import traceback

def setup_logging():
    """
    Configura el logging para el proceso ETL con formato detallado
    """
    # Obtener la ruta de logs del config
    config = load_config()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    logs_dir = os.path.join(project_root, config['paths']['logs_dir'])
    
    # Asegurar que el directorio de logs existe
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    
    # Crear el nombre del archivo de log con timestamp
    log_file = os.path.join(logs_dir, f'etl_log_{datetime.now().strftime("%Y%m%d_%H%M%S_%f")}.log')
    
    # Formato detallado para los logs
    log_format = '[%(asctime)s.%(msecs)03d] %(levelname)s [%(process)d] [%(name)s] - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    # Configurar el logging básico
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt=date_format,
        handlers=[
            logging.StreamHandler(),  # Para mostrar en consola
            logging.FileHandler(log_file, encoding='utf-8')  # Para guardar en archivo
        ]
    )
    
    logger = logging.getLogger('ETL-Process')
    # Configurar nivel de logging para bibliotecas externas
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('googleapiclient').setLevel(logging.WARNING)
    
    return logger

def log_error_details(logger, error, context=""):
    """
    Registra detalles extendidos del error
    """
    logger.error(f"Error en {context}: {str(error)}")
    logger.error("Detalles del error:")
    logger.error(traceback.format_exc())
    if hasattr(error, 'response'):
        logger.error(f"Respuesta del servidor: {error.response.text if hasattr(error.response, 'text') else 'No disponible'}")

def run_etl():
    """
    Ejecuta el proceso ETL completo con logging detallado
    """
    logger = setup_logging()
    start_time = datetime.now()
    
    try:
        logger.info("=== INICIANDO PROCESO ETL ===")
        logger.info(f"Hora de inicio: {start_time.strftime('%Y-%m-%d %H:%M:%S.%f')}")
        
        # 1. Extraer datos
        logger.info("Iniciando proceso de extracción...")
        extract_start = datetime.now()
        extract_result = extract_main()
        
        if not extract_result:
            raise Exception("Falló el proceso de extracción")
        
        logger.info(f"Extracción completada en {(datetime.now() - extract_start).total_seconds():.3f} segundos")
        logger.info(f"Archivos guardados en: {extract_result['download_folder']}")
        logger.info(f"Archivos descargados: {[os.path.basename(f) for f in extract_result['files']]}")
        
        # 2. Transformar datos
        logger.info("Iniciando proceso de transformación...")
        transform_start = datetime.now()
        transform_result = transform_data(extract_result)
        
        if not transform_result:
            raise Exception("Falló el proceso de transformación")
            
        logger.info(f"Transformación completada en {(datetime.now() - transform_start).total_seconds():.3f} segundos")
        logger.info(f"Archivo generado: {transform_result}")
        
        # 3. Cargar datos
        logger.info("Iniciando proceso de carga...")
        load_start = datetime.now()
        load_result = load_data(transform_result)
        
        if not load_result:
            raise Exception("Falló el proceso de carga")
            
        logger.info(f"Proceso de carga completado en {(datetime.now() - load_start).total_seconds():.3f} segundos")
        
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info("=== PROCESO ETL COMPLETADO ===")
        logger.info(f"Hora de finalización: {end_time.strftime('%Y-%m-%d %H:%M:%S.%f')}")
        logger.info(f"Duración total: {duration.total_seconds():.3f} segundos")
        
        return True
        
    except Exception as e:
        log_error_details(logger, e, "proceso ETL")
        end_time = datetime.now()
        duration = end_time - start_time
        logger.error("=== PROCESO ETL FALLIDO ===")
        logger.error(f"Hora de finalización: {end_time.strftime('%Y-%m-%d %H:%M:%S.%f')}")
        logger.error(f"Duración total: {duration.total_seconds():.3f} segundos")
        return False

if __name__ == "__main__":
    if run_etl():
        print("\nProceso ETL completado exitosamente!")
    else:
        print("\nEl proceso ETL falló. Revise los logs para más detalles.")