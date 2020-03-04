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


@ray.remote
def process_page(page, columnSize, data_table):

    filter = TableFilter(columnSize)
    pdf = pikepdf.open(page)
    page = pikepdf.Page(pdf.pages[0])
    page.get_filtered_contents(filter)
    data_table.add_data.remote(filter.table)
    return True


def process_file(inPath, columnNames):
    outPath = inPath.replace(".pdf", ".csv")
    pdf = pikepdf.open(inPath)
    pages = pdf.pages
    fileName = path.split(inPath)[1]
    print(f"Número de Páginas del archivo {fileName}: {len(pages)}")
    columnSize = len(columnNames)
    data_table_handler_actor = DataTableHandler.remote(columnNames)

    remote_proccess_ids = []
    for idx, pageObj in enumerate(pages, start=1):

        page = pikepdf.Page(pageObj)
        tmpFile = BytesIO()
        newPdf = pikepdf.Pdf.new()
        newPdf.pages.append(pageObj)
        newPdf.save(tmpFile)
        id = process_page.remote(tmpFile, columnSize, data_table_handler_actor)
        remote_proccess_ids.append(id)
        tmpFile.flush()
        tmpFile.close()

    # Wait until all the processes have finished
    ray.get(remote_proccess_ids)

    # Count rows
    df = ray.get(data_table_handler_actor.get_dataframe.remote())
    print(f"Número de registros leídos del archivo {fileName}: {df.shape[0]}")

    # Generate CSV
    ray.get(data_table_handler_actor.generate_csv.remote(outPath))


# Main method
def main():
    servel_files_dir = "/Users/cybercastle/tmp/files"
    columnNames = [
        "Nombre",
        "RUN",
        "Sexo",
        "Domicilio",
        "Circunscripción",
        "Mesa Nº",
        "",
    ]

    for filename in listdir(servel_files_dir):
        filepath = path.join(servel_files_dir, filename)
        process_file(filepath, columnNames)
        pass


# Run the main method
if __name__ == "__main__":
    main()
