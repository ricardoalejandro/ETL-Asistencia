import pandas as pd

# Función para contar los presentes de la C01 a la C12
def contar_presente(df):
    # Columnas de las clases C01 a C12
    columnas_clases = [f'C{i:02d}' for i in range(1, 13)]
    return df[columnas_clases].apply(lambda x: (x == 'P').sum())


def procesar_excel(_archivoExcel, _nombreSheet):
    archivoExcel = _archivoExcel

    # Leemos excel
    df = pd.read_excel(
        archivoExcel,
        sheet_name=_nombreSheet, 
        header=None,  
        skiprows=2,  
        nrows=100,  
        usecols="A:AS"
    )

    # Establecer la primera fila del rango como encabezado
    df.columns = df.iloc[0]
    df = df[1:]

    # Filtramos los Preinscritos
    df = df[df['Tipo Incrito'] != 'Pre-Inscrito']

    # Seleccionamos las columnas elegidas
    columnas = ["Mes Inscrito", "Mes de Alta como miembro", "Dia de clases Inscrito", "Grupo", "DNI / CE", "Nombres", "Apellidos", "Edad", "sem 01",
                "sem 02", "sem 03", "sem 04", "sem 05", "sem 06", "sem 07", "sem 08", "sem 09", "sem 10", 
                "sem 11", "sem 12"]
    df_final = df[columnas]

    encabezados = ["MesInscrito", "MesAlta", "DiaClase", "Grupo", "DNI", "Nombres", "Apellidos", "Edad", "C01",
                "C02", "C03", "C04", "C05", "C06", "C07", "C08", "C09", "C10", "C11", "C12"]
    df_final.columns = encabezados

    df_final = df_final.dropna(how="all")
    df_final = df_final.fillna("")

    # Ya con datos limpios, contamos las asistencias
    df_grupos = df_final.groupby(['MesInscrito', 'DiaClase', 'Grupo'])
    resultado = df_grupos.apply(contar_presente).reset_index()
    resultado['Inscritos'] = df_grupos.size().reset_index(drop=True)
    
    filial = _archivoExcel.replace("files\Registro Probacionismo 2025 - ","").strip()
    filial = filial.replace(".xlsx", "").strip()

    resultado['Filial'] = filial

    return resultado
