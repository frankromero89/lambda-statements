import os 

import xml.etree.ElementTree as ET
from lxml import etree
import pandas as pd

class XMLDataProcessor:
    def __init__(self, pathfact, xml_ns):
        self.pathfact = pathfact
        self.xml_ns = xml_ns
        self.df = pd.DataFrame()

    def process_files(self, file_names):
        for filename in file_names:
            xml_file = os.path.join(self.pathfact, filename)
            data = self.extract_xmldata(xml_file)
            # Utiliza el nombre del archivo como nombre de columna en el DataFrame
            column_name = os.path.splitext(filename)[0]
            self.df[column_name] = pd.Series(data)

        # Transpone el DataFrame para que los archivos sean columnas y no filas
        self.df = self.df.T

        return self.df

    def extract_xmldata(self, xml_file):
        tree = ET.parse(xml_file)
        root = tree.getroot()

        arbol = etree.parse(xml_file)
        ns = arbol.xpath('namespace::*')
        namespaces = {key: value for key, value in ns}

        data = {}
        for element in root.findall(self.xml_ns, namespaces):
            for child in element:
                data[child.tag] = child.text

        return data
