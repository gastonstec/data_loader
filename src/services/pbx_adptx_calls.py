import csv
import pandas as pd

# "fecha" 0,"caller_id" 1,"grupo_timbrado" 2,"destino" 3,"canal_origen" 4,
# "codigo_cuenta" 5,"canal_destino" 6,"estado" 7,"duracion" 8,
# "duracion_seg" 9, extension 10
# "fecha", "caller_id", "grupo_timbrado", "destino", "canal_origen", "codigo_cuenta", "canal_destino", "estado", "duracion"

def get_duracion_seg(duracion: str):
    return duracion[:duracion.find("s")]

def get_extension(destino: str):
    return destino[destino.find("/"):destino.find("-")]

def pbx_load_data(filepath: str):
    print(r"Loading PBX data from file: {}".format(filepath))
    df = pd.read_csv(filepath)
    df = df.rename(columns={
        "fecha": "fecha",
        "caller_id": "caller_id",
        "grupo_timbrado": "grupo_timbrado",
        "destino": "destino",
        "canal_origen": "canal_origen",
        "codigo_cuenta": "codigo_cuenta",
        "canal_destino": "canal_destino",
        "estado": "estado",
        "duracion": "duracion"
    })
    for row in df.itertuples():
        print(row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9])
              
pbx_load_data("d:\datospbx\CDRReport-202411-202505.csv")
