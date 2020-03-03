# Servel-Parser

Este código es para extraer la información de los padrones electorales publicados por el SERVEL (Febrero del 2020). Este 
no se basa en ninguna de las soluciones que hay en internet, las cuales se apoyan fuertemente en librerías que extraen
textos (pdfminer u otras) de los PDFs.

Este programa se basa en la librería [pikepdf](https://pikepdf.readthedocs.io) (un wrapper de [libqpdf](http://qpdf.sourceforge.net/)), que permite analizar la estructura del PDF, disminuyendo asi la tasa de errores durante la
extracción de textos.

Además, para acelerar el proceso, se utiliza la librería [ray](https://ray.readthedocs.io), la cual permite paralelizar la extracción de textos.

TODO:
- Añadir más documentación
