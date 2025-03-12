import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import numpy as np

# Obtiene la ruta absoluta al directorio del script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Configura las credenciales de Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials_path = os.path.join(script_dir, "acoustic-alpha-452721-d0-1372f00b966b.json")
creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
client = gspread.authorize(creds)

# Abre la hoja de Google Sheets usando el ID
spreadsheet = client.open_by_key("1KyRGrnkql19dQYnnPxmecLd3hQ7Cn2fLJ8BOBLHKtMA")
worksheet = spreadsheet.sheet1

# Lee el archivo de Excel usando la ruta absoluta
excel_path = os.path.join(script_dir, "Copia de Consolidado.xlsx")
df = pd.read_excel(excel_path)

# Convertir los datos a un formato serializable
def convert_to_serializable(val):
    if pd.isna(val):
        return ''
    elif isinstance(val, pd.Timestamp):
        return val.strftime('%Y-%m-%d %H:%M:%S')
    else:
        return str(val)

# Aplicar la conversión a todos los datos
values = [[convert_to_serializable(val) for val in row] for row in df.values]
headers = df.columns.values.tolist()

# Limpia la hoja de Google Sheets
worksheet.clear()

# Escribe los datos en Google Sheets
worksheet.update([headers] + values)