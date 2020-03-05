from pikepdf import TokenFilter, TokenType


class TableFilter(TokenFilter):

    """
    Esta clase permite extraer la información contenida en las tablas
    que hay en los PDFs del SERVEL (Nombre, RUT, Sexo, Dirección, etc.).
    """

    def __init__(self, columnSize=7, commune_id=-1):
        super().__init__()
        self.is_f2_name_token_type = False
        self.table = []
        self.rows = []
        self.cntColumns = 0
        self.columnSize = columnSize
        self.commune_id = commune_id

    def handle_token(self, token):
        """
        Cada fila de las tablas que hay en los PDFs del SERVEL tienen la siguiente estructura:
        
                    BT
                    1 0 0 1 22 385 Tm
                    /F2 6 Tf
                    0 0 0 rg
                    (<<-- Nombre del Sujeto -->>)Tj
                    0 g
                    ET
                    BT
                    1 0 0 1 279.37 385 Tm
                    /F2 6 Tf
                    0 0 0 rg
                    (<<-- Rut del Sujeto -->>)Tj
                    0 g
                    ET
                    BT
                    1 0 0 1 320.94 385 Tm
                    /F2 6 Tf
                    0 0 0 rg
                    (VARON)Tj
                    0 g
                    ET
                    BT
                    1 0 0 1 346.27 385 Tm
                    /F2 6 Tf
                    0 0 0 rg
                    (BASE E FREI M)Tj
                    0 g
                    ET
                    BT
                    1 0 0 1 577.89 385 Tm
                    /F2 6 Tf
                    0 0 0 rg
                    (ANTARTICA)Tj
                    0 g
                    ET
                    BT
                    1 0 0 1 755.85 385 Tm
                    /F2 6 Tf
                    0 0 0 rg
                    (2)Tj
                    0 g
                    ET
                    BT
                    1 0 0 1 763.19 385 Tm
                    /F2 6 Tf
                    0 0 0 rg
                    (V)Tj
                    0 g
                    ET
                    
            Por tanto, buscamos todos los elementos que están bajo del Token Name "/F2", ya que este Token define
            el tipo de tipografía que se usa para renderizar el contenido.
        """
        if token.type_ == TokenType.name and token.value == "/F2":
            self.is_f2_name_token_type = True

        if token.type_ == TokenType.string and self.is_f2_name_token_type:
            try:

                """
                pikepdf maneja la codificación de los caracteres usado en el PDF, a través del codec "pdfdoc".
                Más info aquí: https://pikepdf.readthedocs.io/en/latest/topics/encoding.html#pdfdocencoding
                """
                text = token.raw_value.decode("pdfdoc").translate(
                    str.maketrans({"(": "", ")": ""})
                )  # Se eliminan los parentesis que encierran al texto extraído

                self.rows.append(text)
                self.cntColumns += 1
                if self.cntColumns >= self.columnSize:
                    # Se añade la comuna como una columna más
                    self.rows.append(self.commune_id)
                    self.table.append(self.rows)
                    self.rows = []
                    self.cntColumns = 0

                self.is_f2_name_token_type = False

            except Exception as e:
                print(e)

        return None
