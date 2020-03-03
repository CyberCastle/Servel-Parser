#!/usr/bin/env python3
# encoding: utf-8

# Internal libraries
from io import BytesIO

# External libraries
import pikepdf
import psutil
import ray

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
    def __init__(self):
        self.data_table = []

    def add_data(self, data):
        self.data_table.extend(data)

    def get_data_table(self):
        return self.data_table


@ray.remote
def process_page(page, data_table):

    filter = TableFilter()
    pdf = pikepdf.open(page)
    page = pikepdf.Page(pdf.pages[0])
    page.get_filtered_contents(filter)
    data_table.add_data.remote(filter.table)
    return True


# Main method
def main():
    pdf = pikepdf.open("/Users/cybercastle/tmp/A12103.pdf")
    # pdf = pikepdf.open("/Users/cybercastle/tmp/A04104.pdf")
    pages = pdf.pages
    print(f"Número de Páginas: {len(pages)}")

    data_table_handler_actor = DataTableHandler.remote()
    remote_proccess_ids = []
    for idx, pageObj in enumerate(pages, start=1):
        print(f"Procesando página {idx}...")
        page = pikepdf.Page(pageObj)

        tmpFile = BytesIO()
        newPdf = pikepdf.Pdf.new()
        newPdf.pages.append(pageObj)
        newPdf.save(tmpFile)
        id = process_page.remote(tmpFile, data_table_handler_actor)
        remote_proccess_ids.append(id)
        tmpFile.flush()
        tmpFile.close()

    print("before")
    print(ray.get(remote_proccess_ids))
    print("after")

    result = ray.get(data_table_handler_actor.get_data_table.remote())

    print(result)


# Run the main method
if __name__ == "__main__":
    main()
