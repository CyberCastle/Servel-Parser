#!/usr/bin/env python3
# encoding: utf-8

# Internal libraries
from io import BytesIO
from os import path, listdir

# External libraries
import pikepdf
import psutil
import ray
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, String, Integer, MetaData
from sqlalchemy.dialects.postgresql import insert

# Global variables
DB_STRING = "postgres://servel:servel@localhost:5432/servel"

# Project dependencies
from tablefilter import TableFilter

"""
Este código es para extraer la información de los padrones electorales publicados
por el SERVEL. Este no se basa en ninguna de las soluciones que hay en internet, las
cuales se apoyan fuertemente en librerías que extraen textos de los PDFs.

Este programa se basa en la librería pikepdf (un wrapper de libqpdf), que permite
analizar la estructura del PDF, disminuyendo asi la tasa de errores durante la
extracción de textos.
"""

# Initializing Ray
num_cpus = psutil.cpu_count(logical=False)
ray.init(num_cpus=num_cpus)


@ray.remote
class DataTableHandler:
    def __init__(self, columnNames=None):
        self._columnNames = columnNames
        self._df = pd.DataFrame(columns=columnNames)

    def add_data(self, data):
        df = pd.DataFrame(data, columns=self._columnNames)
        self._df = pd.concat([self._df, df], ignore_index=True)

    def get_dataframe(self):
        return self._df

    def generate_csv(self, filePath):
        self._df.to_csv(filePath, sep=";", index=False)
        pass

    def save_to_db(self, db_string):

        # Definiciones para crear una conexión hacia la DB
        db = create_engine(db_string)
        meta = MetaData()
        meta.reflect(bind=db)

        # Se seleccionan sólo las columnas que serán guardadas
        selected_columns = [
            "Nombre",
            "RUN",
            "Sexo",
            "Domicilio",
            "Id Comuna",
        ]
        df = self._df[selected_columns]

        # Renombramos las columnas del dataframe, para que se igualen a
        # los nombres usados en la tabla "Votantes"
        df = df.rename(
            columns={
                "Nombre": "nombre",
                "RUN": "rut",
                "Sexo": "sexo",
                "Domicilio": "domicilio",
                "Id Comuna": "comuna_id",
            }
        )

        # Se prepara la sentencia SQL para insertar los registros en la tabla
        stmt = insert(meta.tables["votantes"]).values(df.to_dict(orient="records"))
        on_conflict_stmt = stmt.on_conflict_do_nothing(
            index_elements=["rut"]
        )  # Exclusive for PostgreSQL (UPSERT)

        # Se abre la conexiós, se ejecuta la sentencia y luego se cierra la conexión
        with db.connect() as conn:
            conn.execute(on_conflict_stmt)
            conn.close()


@ray.remote
def process_page(page, columnSize, commune_id, data_table):

    filter = TableFilter(columnSize, commune_id)
    pdf = pikepdf.open(page)
    page = pikepdf.Page(pdf.pages[0])
    page.get_filtered_contents(filter)
    data_table.add_data.remote(filter.table)
    return True


def process_file(inPath, outPath, fileName, columnSize, columnNames):
    fileIn = path.join(inPath, fileName)
    fileOut = path.join(outPath, fileName.replace(".pdf", ".csv"))

    # Se obtiene un id para la comuna a partir del nombre del archivo, el
    # cual está basado en el Código Único Territorial, definido por el INE.
    # Más info aquí: http://www.subdere.cl/sites/default/files/documentos/articles-73111_recurso_2.pdf
    commune_id = fileName.translate(
        str.maketrans({"A": "", ".": "", "p": "", "d": "", "f": ""})
    )
    pdf = pikepdf.open(fileIn)
    pages = pdf.pages

    print(f"Número de Páginas del archivo {fileName}: {len(pages)}")
    data_table_handler_actor = DataTableHandler.remote(columnNames)

    remote_proccess_ids = []
    for idx, pageObj in enumerate(pages, start=1):

        page = pikepdf.Page(pageObj)
        tmpFile = BytesIO()
        newPdf = pikepdf.Pdf.new()
        newPdf.pages.append(pageObj)
        newPdf.save(tmpFile)
        id = process_page.remote(
            tmpFile, columnSize, commune_id, data_table_handler_actor
        )
        remote_proccess_ids.append(id)
        tmpFile.flush()
        tmpFile.close()

    # Wait until all the processes have finished
    ray.get(remote_proccess_ids)

    # Count rows
    df = ray.get(data_table_handler_actor.get_dataframe.remote())
    print(f"Número de registros leídos del archivo {fileName}: {df.shape[0]}")

    # Generate CSV
    # ray.get(data_table_handler_actor.generate_csv.remote(fileOut))

    # Save registers into database
    ray.get(data_table_handler_actor.save_to_db.remote(DB_STRING))


def database_settings(db_string):
    db = create_engine(db_string)
    meta = MetaData(db)
    voter_table = Table(
        "votantes",
        meta,
        Column("rut", String, primary_key=True, nullable=False),
        Column("nombre", String, nullable=False),
        Column("sexo", String),
        Column("domicilio", String),
        Column("comuna_id", String),
    )

    with db.connect() as conn:
        voter_table.create(checkfirst=True)
        conn.close()


# Main method
def main():

    database_settings(DB_STRING)

    servel_files_in = "/Users/cybercastle/tmp/filesIn"
    servel_files_out = "/Users/cybercastle/tmp/filesOut"
    columnNames = [
        "Nombre",
        "RUN",
        "Sexo",
        "Domicilio",
        "Circunscripción",
        "Mesa Nº",
        "",
        "Id Comuna",
    ]
    columnSize = len(columnNames) - 1

    """
    for filename in listdir(servel_files_in):
        process_file(
            servel_files_in, servel_files_out, filename, columnSize, columnNames
        )
        pass

    """

    fileNameDemo = "A13201.pdf"
    process_file(
        servel_files_in, servel_files_out, fileNameDemo, columnSize, columnNames
    )


# Run the main method
if __name__ == "__main__":
    main()
