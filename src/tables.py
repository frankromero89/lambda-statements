import os
import pandas as pd
import numpy as np

from xml_processor import XMLDataProcessor

class Tables:
    def __init__(self, pathfact):
        self.pathfact = pathfact

    def convert_to_fecha(self, row):
        mes = row['Mes']
        año = row['Año']
        fecha = f"{row['Mes']} {str(row['Año']).zfill(2)}"
        return fecha

    def merge_columns_to_gdmth(self, df):
        column_count = max(df['MesesHistorialMen'].astype(int))
        merged_df = pd.DataFrame()

        meses = []
        año = []
        consumo = []
        demanda = []
        demandab = []
        demandai = []
        demandap = []
        demandas = []
        facpot = []
        faccar = []
        pmv = []

        for j in range(1, column_count):
            if j < 10:
                meses.append(f'MESR0{j}')
                año.append(f'AAR0{j}')
                consumo.append(f'CONSUMOR0{j}')
                demanda.append(f'DEMANDAR0{j}')
                demandab.append(f'KWB0{j}')
                demandai.append(f'KWI0{j}')
                demandap.append(f'KWP0{j}')
                demandas.append(f'KWS0{j}')
                facpot.append(f'FACPOTR0{j}')
                faccar.append(f'FACARR0{j}')
                pmv.append(f'PMVR0{j}')
            else:
                meses.append(f'MESR{j}')
                año.append(f'AAR{j}')
                consumo.append(f'CONSUMOR{j}')
                demanda.append(f'DEMANDAR{j}')
                demandab.append(f'KWB{j}')
                demandai.append(f'KWI{j}')
                demandap.append(f'KWP{j}')
                demandas.append(f'KWS{j}')
                facpot.append(f'FACPOTR{j}')
                faccar.append(f'FACARR{j}')
                pmv.append(f'PMVR{j}')

        merged_df['Mes'] = df[meses].stack().reset_index(drop=True)
        merged_df['Año'] = df[año].stack().reset_index(drop=True)
        merged_df['KWh'] = df[consumo].stack().reset_index(drop=True)
        merged_df['KW'] = df[demanda].stack().reset_index(drop=True)
        merged_df['KWb'] = df[demandab].stack().reset_index(drop=True)
        merged_df['KWi'] = df[demandai].stack().reset_index(drop=True)
        merged_df['KWp'] = df[demandap].stack().reset_index(drop=True)
        merged_df['KWs'] = df[demandas].stack().reset_index(drop=True)
        merged_df['FP%'] = df[facpot].stack().reset_index(drop=True)
        merged_df['FC%'] = df[faccar].stack().reset_index(drop=True)
        merged_df['PMV'] = df[pmv].stack().reset_index(drop=True)

        return merged_df

    def merge_columns_to_gdmto(self, df):
        column_count = max(df['MesesHistorialMen'].astype(int))
        merged_df = pd.DataFrame()

        meses = []
        año = []
        consumo = []
        demanda = []
        facpot = []
        faccar = []
        pmv = []

        for j in range(1, column_count):
            if j < 10:
                meses.append(f'MESR0{j}')
                año.append(f'AAR0{j}')
                consumo.append(f'CONSUMOR0{j}')
                demanda.append(f'DEMANDAR0{j}')
                facpot.append(f'FACPOTR0{j}')
                faccar.append(f'FACARR0{j}')
                pmv.append(f'PMVR0{j}')
            else:
                meses.append(f'MESR{j}')
                año.append(f'AAR{j}')
                consumo.append(f'CONSUMOR{j}')
                demanda.append(f'DEMANDAR{j}')
                facpot.append(f'FACPOTR{j}')
                faccar.append(f'FACARR{j}')
                pmv.append(f'PMVR{j}')

        merged_df['Mes'] = df[meses].stack().reset_index(drop=True)
        merged_df['Año'] = df[año].stack().reset_index(drop=True)
        merged_df['KWh'] = df[consumo].stack().reset_index(drop=True)
        merged_df['KW'] = df[demanda].stack().reset_index(drop=True)
        merged_df['FP%'] = df[facpot].stack().reset_index(drop=True)
        merged_df['FC%'] = df[faccar].stack().reset_index(drop=True)
        merged_df['PMV'] = df[pmv].stack().reset_index(drop=True)

        return merged_df
    
    def merge_columns_to_pdbt(self, df):
        column_count = max(df['MesesHistorialBim'].astype(int))
        merged_df = pd.DataFrame()

        meses = []
        año = []
        consumo = []

        for j in range(1, column_count):
            meses.append(f'MESH{j}')
            año.append(f'YEARH{j}')
            consumo.append(f'CONSUMOH{j}')

        merged_df['Mes'] = df[meses].stack().reset_index(drop=True)
        merged_df['Año'] = df[año].stack().reset_index(drop=True)
        merged_df['KWh'] = df[consumo].stack().reset_index(drop=True)

        return merged_df
    
    def df_imp(self, Cols, Tcon, Timp):
        # Crea un diccionario para almacenar los datos
        data = {col: [] for col in Cols}

        # Recorre las filas de Tcon
        for i in range(len(Tcon)):
            fila_con = Tcon.iloc[i, :]  # Obtiene la fila completa de Tcon

            for col in Cols:
                if col in fila_con.values:
                    # Si el concepto está en la fila, obtén el valor correspondiente de Timp
                    j = np.where(fila_con.values == col)[0][0]  # Encuentra la columna donde se encuentra el concepto
                    valor = Timp.iloc[i, j]  # Obtiene el valor correspondiente de Timp
                else:
                    valor = 0  # Si no se encuentra, asigna cero
                data[col].append(valor)

        # Crea el DataFrame final
        df_final = pd.DataFrame(data)
        
        return df_final
    
    def tablas(self):
        for pathfact_filter in os.listdir(self.pathfact):
            if pathfact_filter.endswith('.DS_Store') or pathfact_filter.startswith('.com'):
                os.remove(os.path.join(self.pathfact, pathfact_filter))

        xml_path = './/cfdi:Addenda/clsRegArchFact'
        xmls = os.listdir(self.pathfact)
        processor = XMLDataProcessor(self.pathfact, xml_path)
        TA = processor.process_files(xmls)
        df3 = TA.copy()

        month_mapping = {'ENE': 'Jan', 'FEB': 'Feb', 'MAR': 'Mar', 'ABR': 'Apr', 'MAY': 'May', 'JUN': 'Jun', 'JUL': 'Jul', 'AGO': 'Aug', 'SEP': 'Sep', 'OCT': 'Oct', 'NOV': 'Nov', 'DIC': 'Dec'}

        meses = ['FECDESDE', 'FECHASTA', 'FECLIMITE', 'FECORTE']

        for f in meses:
            TA[f] = pd.to_datetime(TA[f].replace(month_mapping, regex=True), format="%d %b %y")

        TA.loc[:, 'FechaHoraCFD'] = pd.to_datetime(TA['FechaHoraCFD'], format='%d/%m/%Y %H:%M:%S:%f').dt.strftime("%Y-%m-%d")
        TA['CantConceptos'] = TA['CantConceptos'].astype(int)
        TA = TA.sort_values(by='CantConceptos', ascending=False)

        # Acomodar la lista de xml en orden descendente en función de la cantidad 
        # de Conceptos

        dfm_idx = TA.index.tolist()
        dfm_idx = [nombre + ".xml" for nombre in dfm_idx]
        TA['idx'] = dfm_idx

        # Complemento para la tabla TA, contiene conceptos
        xml_con = './/cfdi:Addenda/clsRegArchFact/Conceptos'
        processor_c = XMLDataProcessor(self.pathfact, xml_con)
        Tcon = processor_c.process_files(dfm_idx)
        Tcon = Tcon.iloc[:, 7:]
        Tcon = Tcon.copy()
        Tcon.fillna(0.0, inplace=True)

        # Conceptos de importes
        xml_imp = './/cfdi:Addenda/clsRegArchFact/Importes'
        processor_p = XMLDataProcessor(self.pathfact, xml_imp)
        Timp = processor_p.process_files(dfm_idx)
        Timp = Timp.iloc[:, 7:]
        Timp = Timp.copy()
        Timp = Timp.astype(float)
        Timp.fillna(0.0, inplace=True)

        Cols = [
            'Cargo Fijo???', 'Energ?a', '2% Baja Tensi?n???', 'Bonificaci?n Factor de Potencia???', 'Cargo Factor de Potencia???', '(1) Reconexion???', 'Reconexion???', 
            'Subtotal', 'IVA 16%', 'Fac. del Periodo' ,'Facturaci?n del Periodo', 'DAP???', 'Derecho de Alumbrado P?blico???', 'Credito Aplic. Fac.???', 'Adeudo Anterior', 
            'Diferencia por redondeo', 'Su Pago' , 'Total'
            ]

        TAimp = self.df_imp(Cols, Tcon, Timp)
        TA = TA.reset_index(drop=True)
        TA = pd.concat([TA, TAimp], axis=1)
        TA = TA.sort_values(by='FechaHoraCFD', ascending=True)
        TA = TA.reset_index(drop=True)

        tarifa = TA.loc[0, 'TARIFA_REG']
        ### Hasta aquí todas las tablas siguen estos pasos##

        if tarifa == 'GDMTH':
            # Elige columnas
            cols = [
            'FechaHoraCFD', 'LineaDeReferencia', 'HayReconexion', 'HayCargoFIDE', 'CantConceptos', 'MesesHistorialMen',
            'NOMBRE', 'NOMPOB', 'NOMEST', 'DIRECC', 'RPU', 'NUMMED1', 'TARIFA_REG', 'HILOS', 'CARGA_CONTRATADA', 'CARGA_CONECTADA',
            'FECDESDE', 'FECHASTA', 'FECLIMITE', 'FECORTE',
            'CargosCreditos', 'MontoCalculadoEnLetras', 'CONSUMO_R', 'DEMANDA',
            'CONSUMO1F', 'CONSUMO2F', 'CONSUMO3F', 'CONSUMO4F', 
            'DEMANDA1P', 'DEMANDA2P', 'DEMANDA3P','DEMANDA4P', 'KVARH', 'FacPot',
            'IMPTE_TOT_REG_1', 'IMPTE_TOT_REG_2', 'IMPTE_TOT_REG_3', 'IMPTE_TOT_REG_4', 'IMPTE_TOT_REG_5', 'IMPTE_TOT_REG_6', 'IMPTE_TOT_REG_7', 'IMPTE_TOT_REG_8', 'IMPTE_TOT_REG_9',
            'Cargo Fijo???', 'Energ?a', '2% Baja Tensi?n???', 'Bonificaci?n Factor de Potencia???','Cargo Factor de Potencia???', '(1) Reconexion???', 'Reconexion???', 
            'Subtotal', 'IVA 16%', 'Fac. del Periodo' ,'Facturaci?n del Periodo', 'DAP???', 'Derecho de Alumbrado P?blico???', 'Credito Aplic. Fac.???', 'Adeudo Anterior', 
            'Diferencia por redondeo', 'Su Pago' , 'Total'
            ]
            # Renombra columnas
            rcols = {
            'FechaHoraCFD': 'Fecha', 'LineaDeReferencia': 'Referencia', 'HayReconexion': 'HayReconexion', 'HayCargoFIDE': 'HayCargoFIDE', 'CantConceptos': 'CantConceptos', 'MesesHistorialMen': 'Meses historal', 
            'NOMBRE': 'Nombre', 'NOMPOB': 'Población', 'NOMEST': 'Estado', 'DIRECC': 'Dirección', 'RPU': 'RPU', 'NUMMED1': 'Num. Medidor', 
            'TARIFA_REG': 'Tarifa', 'HILOS': 'Hilos', 'CARGA_CONTRATADA': 'KW contra', 'CARGA_CONECTADA': 'KW conec',
            'FECDESDE': 'Fecha ini', 'FECHASTA': 'Fecha fin', 'FECLIMITE': 'Fecha límite', 'FECORTE': 'Fecha corte', 'CargosCreditos': 'Cargos', 'MontoCalculadoEnLetras': 'Monto en Letras', 'CONSUMO_R': 'KWh', 'DEMANDA': 'KW',
            'CONSUMO1F': 'KWhb', 'CONSUMO2F': 'KWhi', 'CONSUMO3F': 'KWhp', 'CONSUMO4F': 'KWhee', 'DEMANDA1P': 'KWb','DEMANDA2P': 'KWi', 'DEMANDA3P': 'KWp', 'DEMANDA4P': 'KWe', 
            'KVARH': 'KVArh', 'FacPot': 'FP %',
            'IMPTE_TOT_REG_1': 'Suministro', 'IMPTE_TOT_REG_2': 'Distribución', 'IMPTE_TOT_REG_3': 'Transmisión', 'IMPTE_TOT_REG_4': 'CENACE', 'IMPTE_TOT_REG_5': 'Energía b', 'IMPTE_TOT_REG_6': 'Energía i', 'IMPTE_TOT_REG_7': 'Energía p', 'IMPTE_TOT_REG_8': 'Capacidad', 'IMPTE_TOT_REG_9': 'SCnMEM',
            'Cargo Fijo???': 'Cargo fijo', 'Energ?a': 'Energía', '2% Baja Tensi?n???': '2% BT', 'Bonificaci?n Factor de Potencia???': 'Bonificación FP', 'Cargo Factor de Potencia???': 'Cargo FP', 
            '(1) Reconexion???': 'Reconexción', 'Reconexion???': 'Reconexción2', 'Subtotal': 'Subtotal', 'IVA 16%': 'IVA', 'Fac. del Periodo': 'Factura', 'Facturaci?n del Periodo': 'Factura2', 
            'DAP???': 'DAP', 'Derecho de Alumbrado P?blico???': 'DAP2', 'Credito Aplic. Fac.???': 'Cred ap.', 'Adeudo Anterior': 'Adeudo ant.', 'Diferencia por redondeo': 'Redondeo', 
            'Su Pago': 'Su pago', 'Total': 'Total',
            }

            columns_to_convert_float1 = [
            'Cargos', 'KWh', 'KW', 'KWhb', 'KWhi', 'KWhp', 'KWhee', 'KWb', 'KWi', 'KWp', 'KWe', 'KVArh', 'FP %',
            'Suministro', 'Distribución', 'Transmisión', 'CENACE', 'Energía b', 'Energía i', 'Energía p', 'Capacidad', 'SCnMEM',
            'Cargo fijo', 'Energía', '2% BT', 'Bonificación FP', 'Cargo FP', 'Reconexción', 'Reconexción2', 'Subtotal', 'IVA', 'Factura', 'Factura2', 
            'DAP', 'DAP2', 'Cred ap.', 'Adeudo ant.', 'Redondeo', 'Su pago', 'Total'
            ]

            df = TA[cols].copy()
            df = df.rename(columns=rcols)
            df.loc[:, columns_to_convert_float1] = df[columns_to_convert_float1].astype(float)

            # TA0 Datos de la cuenta
            TA0 = df.iloc[:, :6]

            # TA1 Datos de usuario
            TA1 = df.iloc[:, 6:16]

            # TA2  Fechas importantes, Monto total y consumo desagregado
            TA2 = df.iloc[:, 16:34]
            colfloat = ['Cargos', 'KWh', 'KW', 'KWhb', 'KWhi', 'KWhp', 'KWhee', 'KWb', 'KWi', 'KWp', 'KWe', 'KVArh', 'FP %']
            TA2[colfloat] = TA2[colfloat].astype(float)

            # Calcular la columna $/KWh
            TA2['$/KWh'] = TA2['Cargos'] / TA2['KWh']
            TA2['$/KWh'] = TA2['$/KWh'].round(2)
            TA2 = TA2.sort_values(by='Fecha fin', ascending=True)
            
            # T3 Distribución de costos de energía y de la factura
            T3 = df.iloc[:, [0] + list(range(34, 61))]
            T3.loc[:, 'Reconexción'] = T3['Reconexción'] + T3['Reconexción2']
            T3.loc[:, 'Factura'] = T3['Factura'] + T3['Factura2']
            T3.loc[:, 'DAP'] = T3['DAP'] + T3['DAP2']
            TA3 = T3.copy()
            TA3.drop(['2% BT', 'Reconexción2', 'Factura2', 'DAP2'], axis=1, inplace=True)
            
            # TAH DataFrame para históricos
            TAH = self.merge_columns_to_gdmth(df=df3)
            
            TAH[['KWh', 'KW', 'KWb', 'KWi', 'KWp', 'KWs', 'FP%', 'FC%', 'PMV']] = TAH[['KWh', 'KW', 'KWb', 'KWi', 'KWp', 'KWs', 'FP%', 'FC%', 'PMV']].astype(float)

            TAH['Fecha'] = TAH.apply(self.convert_to_fecha, axis=1)

            TAH['Fecha'] = TAH['Mes'] + ' ' + TAH['Año']
            TAH = TAH.drop(['Mes', 'Año'], axis=1)
            columns = TAH.columns.tolist()
            columns = ['Fecha'] + [col for col in columns if col != 'Fecha']
            TAH = TAH[columns]

            TAH['Fecha'] = pd.to_datetime(TAH['Fecha'].replace(month_mapping, regex=True), format="%b %y")

            TAH = pd.pivot_table(TAH, values=['KWh', 'KW', 'KWb', 'KWi', 'KWp', 'KWs', 'FP%', 'FC%', 'PMV'], index=['Fecha'],
                                    aggfunc={
                                        'KWh': np.max,
                                        'KW': np.max,
                                        'KWb': np.max,
                                        'KWi': np.max,
                                        'KWp': np.max,
                                        'KWs': np.max,
                                        'FP%': np.max,
                                        'FC%': np.max,
                                        'PMV': np.max
                                    })
            
            TAH['KVARh'] = (((TAH['KWh'] / (TAH['FP%'] / 100))**2 - (TAH['KWh']**2))**(1/2)).round(2)

        elif tarifa == 'GDMTO':
            # Elige columnas
            cols = [
            'FechaHoraCFD', 'LineaDeReferencia', 'HayReconexion', 'HayCargoFIDE', 'CantConceptos', 'MesesHistorialMen',
            'NOMBRE', 'NOMPOB', 'NOMEST', 'DIRECC', 'RPU', 'NUMMED1', 'TARIFA_REG', 'HILOS', 'CARGA_CONTRATADA', 'CARGA_CONECTADA',
            'FECDESDE', 'FECHASTA', 'FECLIMITE', 'FECORTE', 'CargosCreditos', 'MontoCalculadoEnLetras', 'Dias', 'TotalMed1', 'TotalMed4', 'TotalMed5', 'FacPot',
            'IMPTE_TOT_REG_1', 'IMPTE_TOT_REG_2', 'IMPTE_TOT_REG_3', 'IMPTE_TOT_REG_4', 'IMPTE_TOT_REG_5', 'IMPTE_TOT_REG_6', 'IMPTE_TOT_REG_7',
            'Cargo Fijo???', 'Energ?a', '2% Baja Tensi?n???', 'Bonificaci?n Factor de Potencia???','Cargo Factor de Potencia???', '(1) Reconexion???', 'Reconexion???', 
            'Subtotal', 'IVA 16%', 'Fac. del Periodo' ,'Facturaci?n del Periodo', 'DAP???', 'Derecho de Alumbrado P?blico???', 'Credito Aplic. Fac.???', 'Adeudo Anterior', 
            'Diferencia por redondeo', 'Su Pago' , 'Total'
            ]
            # Renombra columnas
            rcols = {
            'FechaHoraCFD': 'Fecha', 'LineaDeReferencia': 'Referencia', 'HayReconexion': 'HayReconexion', 'HayCargoFIDE': 'HayCargoFIDE', 'CantConceptos': 'CantConceptos', 'MesesHistorialMen': 'Meses historal', 
            'NOMBRE': 'Nombre', 'NOMPOB': 'Población', 'NOMEST': 'Estado', 'DIRECC': 'Dirección', 'RPU': 'RPU', 'NUMMED1': 'Num. Medidor', 
            'TARIFA_REG': 'Tarifa', 'HILOS': 'Hilos', 'CARGA_CONTRATADA': 'KW contra', 'CARGA_CONECTADA': 'KW conec',
            'FECDESDE': 'Fecha ini', 'FECHASTA': 'Fecha fin', 'FECLIMITE': 'Fecha límite', 'FECORTE': 'Fecha corte', 'CargosCreditos': 'Cargos', 'MontoCalculadoEnLetras': 'Monto en Letras',
            'Dias': 'Días', 'TotalMed1': 'KWh', 'TotalMed4': 'KW', 'TotalMed5': 'KVArh', 'FacPot': 'FP %',
            'IMPTE_TOT_REG_1': 'Suministro', 'IMPTE_TOT_REG_2': 'Distribución', 'IMPTE_TOT_REG_3': 'Transmisión', 'IMPTE_TOT_REG_4': 'CENACE', 'IMPTE_TOT_REG_5': 'Energía c', 'IMPTE_TOT_REG_6': 'Capacidad', 'IMPTE_TOT_REG_7': 'SCnMEM',
            'Cargo Fijo???': 'Cargo fijo', 'Energ?a': 'Energía', '2% Baja Tensi?n???': '2% BT', 'Bonificaci?n Factor de Potencia???': 'Bonificación FP', 'Cargo Factor de Potencia???': 'Cargo FP', 
            '(1) Reconexion???': 'Reconexción', 'Reconexion???': 'Reconexción2', 'Subtotal': 'Subtotal', 'IVA 16%': 'IVA', 'Fac. del Periodo': 'Factura', 'Facturaci?n del Periodo': 'Factura2', 
            'DAP???': 'DAP', 'Derecho de Alumbrado P?blico???': 'DAP2', 'Credito Aplic. Fac.???': 'Cred ap.', 'Adeudo Anterior': 'Adeudo ant.', 'Diferencia por redondeo': 'Redondeo', 
            'Su Pago': 'Su pago', 'Total': 'Total',
            }

            columns_to_convert_float1 = [
            'Cargos', 'Días', 'KWh', 'KW', 'KVArh', 'FP %',
            'Suministro', 'Distribución', 'Transmisión', 'CENACE', 'Energía c', 'Capacidad', 'SCnMEM',
            'Cargo fijo', 'Energía', '2% BT', 'Bonificación FP', 'Cargo FP', 'Reconexción', 'Reconexción2', 'Subtotal', 'IVA', 'Factura', 'Factura2', 
            'DAP', 'DAP2', 'Cred ap.', 'Adeudo ant.', 'Redondeo', 'Su pago', 'Total'
            ]

            df = TA[cols].copy()
            df = df.rename(columns=rcols)
            df.loc[:, columns_to_convert_float1] = df[columns_to_convert_float1].astype(float)

            # TA0 Datos de la cuenta
            TA0 = df.iloc[:, :6]

            # TA1 Datos de usuario
            TA1 = df.iloc[:, 6:16]

            # TA2  Fechas importantes, Monto total y consumo desagregado
            TA2 = df.iloc[:, 16:27]
            colfloat = ['Cargos', 'KWh', 'KW', 'KVArh', 'FP %']
            TA2[colfloat] = TA2[colfloat].astype(float)

            # Calcular la columna $/KWh
            TA2['$/KWh'] = TA2['Cargos'] / TA2['KWh']
            TA2['$/KWh'] = TA2['$/KWh'].round(2)
            TA2 = TA2.sort_values(by='Fecha fin', ascending=True)

            # TA3 Distribución de costos de energía y de la factura
            TA3 = df.iloc[:, [0] + list(range(27, 52))]
            TA3.loc[:, 'Reconexción'] = TA3['Reconexción'] + TA3['Reconexción2']
            TA3.loc[:, 'Factura'] = TA3['Factura'] + TA3['Factura2']
            TA3.loc[:, 'DAP'] = TA3['DAP'] + TA3['DAP2']
            TA3.drop(['Reconexción2', 'Factura2', 'DAP2'], axis=1, inplace=True)

            # TAH DataFrame para históricos
            TAH = self.merge_columns_to_gdmto(df=df3)
            
            TAH[['KWh', 'KW', 'FP%', 'FC%', 'PMV']] = TAH[['KWh', 'KW', 'FP%', 'FC%', 'PMV']].astype(float)

            TAH['Fecha'] = TAH.apply(self.convert_to_fecha, axis=1)

            TAH['Fecha'] = TAH['Mes'] + ' ' + TAH['Año']
            TAH = TAH.drop(['Mes', 'Año'], axis=1)
            columns = TAH.columns.tolist()
            columns = ['Fecha'] + [col for col in columns if col != 'Fecha']
            TAH = TAH[columns]

            TAH['Fecha'] = pd.to_datetime(TAH['Fecha'].replace(month_mapping, regex=True), format="%b %y")

            TAH = pd.pivot_table(TAH, values=['KWh', 'KW', 'FP%', 'FC%', 'PMV'], index=['Fecha'],
                                    aggfunc={
                                        'KWh': np.max,
                                        'KW': np.max,
                                        'FP%': np.max,
                                        'FC%': np.max,
                                        'PMV': np.max
                                    })
            
            TAH['KVARh'] = (((TAH['KWh'] / (TAH['FP%'] / 100))**2 - (TAH['KWh']**2))**(1/2)).round(2)

        elif tarifa == 'PDBT':
            # Elige columnas
            cols = [
            'FechaHoraCFD', 'LineaDeReferencia', 'HayReconexion', 'HayCargoFIDE', 'CantConceptos', 'MesesHistorialBim',
            'NOMBRE', 'NOMPOB', 'NOMEST', 'DIRECC', 'RPU', 'NUMMED1', 'TARIFA_REG', 'HILOS', 'CARGA_CONTRATADA', 'CARGA_CONECTADA',
            'FECDESDE', 'FECHASTA', 'FECLIMITE', 'FECORTE', 'CargosCreditos', 'MontoCalculadoEnLetras', 'Dias', 'TotalMed1', 'ConsumoDiario', 'PrecioDiario',
            'IMPTE_TOT_REG_1', 'IMPTE_TOT_REG_2', 'IMPTE_TOT_REG_3', 'IMPTE_TOT_REG_4', 'IMPTE_TOT_REG_5', 'IMPTE_TOT_REG_6', 'IMPTE_TOT_REG_7',
            'Cargo Fijo???', 'Energ?a', '2% Baja Tensi?n???', 'Bonificaci?n Factor de Potencia???','Cargo Factor de Potencia???', '(1) Reconexion???', 'Reconexion???', 
            'Subtotal', 'IVA 16%', 'Fac. del Periodo' ,'Facturaci?n del Periodo', 'DAP???', 'Derecho de Alumbrado P?blico???', 'Credito Aplic. Fac.???', 'Adeudo Anterior', 
            'Diferencia por redondeo', 'Su Pago' , 'Total'
            ]

            # Renombra columnas
            rcols = {
            'FechaHoraCFD': 'Fecha', 'LineaDeReferencia': 'Referencia', 'HayReconexion': 'HayReconexion', 'HayCargoFIDE': 'HayCargoFIDE', 'CantConceptos': 'CantConceptos', 'MesesHistorialBim': 'Bimestres historal', 
            'NOMBRE': 'Nombre', 'NOMPOB': 'Población', 'NOMEST': 'Estado', 'DIRECC': 'Dirección', 'RPU': 'RPU', 'NUMMED1': 'Num. Medidor', 
            'TARIFA_REG': 'Tarifa', 'HILOS': 'Hilos', 'CARGA_CONTRATADA': 'KW contra', 'CARGA_CONECTADA': 'KW conec',
            'FECDESDE': 'Fecha ini', 'FECHASTA': 'Fecha fin', 'FECLIMITE': 'Fecha límite', 'FECORTE': 'Fecha corte', 'CargosCreditos': 'Cargos', 'MontoCalculadoEnLetras': 'Monto en Letras',
            'Dias': 'Días', 'TotalMed1': 'KWh', 'ConsumoDiario': 'KWh/d', 'PrecioDiario': '$/d',
            'IMPTE_TOT_REG_1': 'Suministro', 'IMPTE_TOT_REG_2': 'Distribución', 'IMPTE_TOT_REG_3': 'Transmisión', 'IMPTE_TOT_REG_4': 'CENACE', 'IMPTE_TOT_REG_5': 'Energía c', 'IMPTE_TOT_REG_6': 'Capacidad', 'IMPTE_TOT_REG_7': 'SCnMEM',
            'Cargo Fijo???': 'Cargo fijo', 'Energ?a': 'Energía', '2% Baja Tensi?n???': '2% BT', 'Bonificaci?n Factor de Potencia???': 'Bonificación FP', 'Cargo Factor de Potencia???': 'Cargo FP', 
            '(1) Reconexion???': 'Reconexción', 'Reconexion???': 'Reconexción2', 'Subtotal': 'Subtotal', 'IVA 16%': 'IVA', 'Fac. del Periodo': 'Factura', 'Facturaci?n del Periodo': 'Factura2', 
            'DAP???': 'DAP', 'Derecho de Alumbrado P?blico???': 'DAP2', 'Credito Aplic. Fac.???': 'Cred ap.', 'Adeudo Anterior': 'Adeudo ant.', 'Diferencia por redondeo': 'Redondeo', 
            'Su Pago': 'Su pago', 'Total': 'Total',
            }

            columns_to_convert_float1 = [
            'Cargos', 'Días', 'KWh', 'KWh/d', '$/d',
            'Suministro', 'Distribución', 'Transmisión', 'CENACE', 'Energía c', 'Capacidad', 'SCnMEM',
            'Cargo fijo', 'Energía', '2% BT', 'Bonificación FP', 'Cargo FP', 'Reconexción', 'Reconexción2', 'Subtotal', 'IVA', 'Factura', 'Factura2', 
            'DAP', 'DAP2', 'Cred ap.', 'Adeudo ant.', 'Redondeo', 'Su pago', 'Total'
            ]

            df = TA[cols].copy()
            df = df.rename(columns=rcols)
            df.loc[:, columns_to_convert_float1] = df[columns_to_convert_float1].astype(float)

            # TA0 Datos de la cuenta
            TA0 = df.iloc[:, :6]

            # TA1 Datos de usuario
            TA1 = df.iloc[:, 6:16]

            # TA2  Fechas importantes, Monto total y consumo desagregado
            TA2 = df.iloc[:, 16:26]
            colfloat = ['Cargos', 'Días', 'KWh', 'KWh/d', '$/d']
            TA2[colfloat] = TA2[colfloat].astype(float)

            # Calcular la columna $/KWh
            TA2['$/KWh'] = TA2['Cargos'] / TA2['KWh']
            TA2['$/KWh'] = TA2['$/KWh'].round(2)
            TA2 = TA2.sort_values(by='Fecha fin', ascending=True)

            # TA3 Distribución de costos de energía y de la factura
            TA3 = df.iloc[:, [0] + list(range(26, 51))]
            TA3.loc[:, 'Reconexción'] = TA3['Reconexción'] + TA3['Reconexción2']
            TA3.loc[:, 'Factura'] = TA3['Factura'] + TA3['Factura2']
            TA3.loc[:, 'DAP'] = TA3['DAP'] + TA3['DAP2']
            TA3.drop(['Reconexción2', 'Factura2', 'DAP2'], axis=1, inplace=True)

            # TAH DataFrame para históricos
            TAH = self.merge_columns_to_pdbt(df=df3)
            
            TAH[['KWh']] = TAH[['KWh']].astype(float)

            TAH['Fecha'] = TAH.apply(self.convert_to_fecha, axis=1)

            TAH['Fecha'] = TAH['Mes'] + ' ' + TAH['Año']
            TAH = TAH.drop(['Mes', 'Año'], axis=1)
            columns = TAH.columns.tolist()
            columns = ['Fecha'] + [col for col in columns if col != 'Fecha']
            TAH = TAH[columns]

            TAH = TAH.drop(TAH[TAH['Fecha'] == '00 0000'].index)

            TAH['Fecha'] = pd.to_datetime(TAH['Fecha'], format="%m %Y")

            TAH = pd.pivot_table(TAH, values=['KWh'], index=['Fecha'],
                                    aggfunc={
                                        'KWh': np.max
                                    })

        elif tarifa == 'DB2' or 'DB1':
            # Elige columnas
            cols = [
            'idx', 'FechaHoraCFD', 'LineaDeReferencia', 'HayReconexion', 'HayCargoFIDE', 'CantConceptos', 'ENTSALVER', 'MaxEscalonesEnSubperiodos1', 'CUOTAE', 'CUOTAE_CONSUMIDA', 'Porc_CuotaEnerAnual_Cons', 'TipoDeConsumo', 'MaxEscalonesEnSubperiodos2','MesesHistorialBim',
            'NOMBRE', 'NOMPOB', 'NOMEST', 'DIRECC', 'RPU', 'NUMMED1', 'TARIFA_REG', 'HILOS', 'CARGA_CONTRATADA', 'CARGA_CONECTADA',
            'FECDESDE', 'FECHASTA', 'FECLIMITE', 'FECORTE', 'DIASXPERIODO1', 'CargosCreditos', 'MontoCalculadoEnLetras', 'Dias', 'TotalMed1', 'ConsumoDiario', 'PrecioDiario',
            'IMPTE_TOT_REG_1', 'IMPTE_TOT_REG_2', 'IMPTE_TOT_REG_3', 'IMPTE_TOT_REG_4', 'IMPTE_TOT_REG_5', 'IMPTE_TOT_REG_6', 'IMPTE_TOT_REG_7',
            'Cargo Fijo???', 'Energ?a', '2% Baja Tensi?n???', 'Bonificaci?n Factor de Potencia???','Cargo Factor de Potencia???', '(1) Reconexion???', 'Reconexion???', 
            'Subtotal', 'IVA 16%', 'Fac. del Periodo' ,'Facturaci?n del Periodo', 'DAP???', 'Derecho de Alumbrado P?blico???', 'Credito Aplic. Fac.???', 'Adeudo Anterior', 
            'Diferencia por redondeo', 'Su Pago' , 'Total'
            ]

            # Renombra columnas
            rcols = {
            'idx': 'idx', 'FechaHoraCFD': 'Fecha', 'LineaDeReferencia': 'Referencia', 'HayReconexion': 'HayReconexion', 'HayCargoFIDE': 'HayCargoFIDE', 'CantConceptos': 'CantConceptos', 'ENTSALVER': 'ENTSALVER', 'MaxEscalonesEnSubperiodos1': 'Escalonesp1', 'CUOTAE': 'CE', 'CUOTAE_CONSUMIDA': 'CE con', 'Porc_CuotaEnerAnual_Cons': '% CAE', 'TipoDeConsumo': 'Tconsumo', 'MaxEscalonesEnSubperiodos2': 'Escalonesp2', 'MesesHistorialBim': 'Bimestres historal', 
            'NOMBRE': 'Nombre', 'NOMPOB': 'Población', 'NOMEST': 'Estado', 'DIRECC': 'Dirección', 'RPU': 'RPU', 'NUMMED1': 'Num. Medidor', 
            'TARIFA_REG': 'Tarifa', 'HILOS': 'Hilos', 'CARGA_CONTRATADA': 'KW contra', 'CARGA_CONECTADA': 'KW conec',
            'FECDESDE': 'Fecha ini', 'FECHASTA': 'Fecha fin', 'FECLIMITE': 'Fecha límite', 'FECORTE': 'Fecha corte', 'DIASXPERIODO1': 'Días p1', 'CargosCreditos': 'Cargos', 'MontoCalculadoEnLetras': 'Monto en Letras',
            'Dias': 'Días', 'TotalMed1': 'KWh', 'ConsumoDiario': 'KWh/d', 'PrecioDiario': '$/d',
            'IMPTE_TOT_REG_1': 'Suministro', 'IMPTE_TOT_REG_2': 'Distribución', 'IMPTE_TOT_REG_3': 'Transmisión', 'IMPTE_TOT_REG_4': 'CENACE', 'IMPTE_TOT_REG_5': 'Energía c', 'IMPTE_TOT_REG_6': 'Capacidad', 'IMPTE_TOT_REG_7': 'SCnMEM',
            'Cargo Fijo???': 'Cargo fijo', 'Energ?a': 'Energía', '2% Baja Tensi?n???': '2% BT', 'Bonificaci?n Factor de Potencia???': 'Bonificación FP', 'Cargo Factor de Potencia???': 'Cargo FP', 
            '(1) Reconexion???': 'Reconexción', 'Reconexion???': 'Reconexción2', 'Subtotal': 'Subtotal', 'IVA 16%': 'IVA', 'Fac. del Periodo': 'Factura', 'Facturaci?n del Periodo': 'Factura2', 
            'DAP???': 'DAP', 'Derecho de Alumbrado P?blico???': 'DAP2', 'Credito Aplic. Fac.???': 'Cred ap.', 'Adeudo Anterior': 'Adeudo ant.', 'Diferencia por redondeo': 'Redondeo', 
            'Su Pago': 'Su pago', 'Total': 'Total',
            }

            columns_to_convert_float1 = [
            'Cargos', 'Días', 'KWh', 'KWh/d', '$/d',
            'Suministro', 'Distribución', 'Transmisión', 'CENACE', 'Energía c', 'Capacidad', 'SCnMEM',
            'Cargo fijo', 'Energía', '2% BT', 'Bonificación FP', 'Cargo FP', 'Reconexción', 'Reconexción2', 'Subtotal', 'IVA', 'Factura', 'Factura2', 
            'DAP', 'DAP2', 'Cred ap.', 'Adeudo ant.', 'Redondeo', 'Su pago', 'Total'
            ]

            df = TA[cols].copy()
            df = df.rename(columns=rcols)
            df.loc[:, columns_to_convert_float1] = df[columns_to_convert_float1].astype(float)

            xml_dom1 = './/cfdi:Addenda/clsRegArchFact/DetallesSubperiodo1'
            xml_dom2 = './/cfdi:Addenda/clsRegArchFact/DetallesSubperiodo2'

            processor1 = XMLDataProcessor(self.pathfact, xml_dom1)
            df1 = processor1.process_files(xmls)
            df1 = df1.iloc[:, 7:].astype(float)
            df1.index = df1.index.astype(str) + '.xml'

            df_ = df[['ENTSALVER', 'Escalonesp1', 'Escalonesp2']].astype(int)
            df_['idx'] = df['idx']
            dfm = df_[df_['ENTSALVER'] == 3]
            dfm_idx = dfm['idx']
            df = df.set_index('idx')

            processor2 = XMLDataProcessor(self.pathfact, xml_dom2)
            df2 = processor2.process_files(dfm_idx)
            df2 = df2.iloc[:, 7:].astype(float)
            df2.index = df2.index.astype(str) + '.xml'

            df_.set_index('idx', inplace=True)
            df_raw = pd.concat([df_, df1, df2], axis=1)
            df_raw.fillna(0.0, inplace=True)

            # Subperiodo 1 Fuera de verano 
            df1_fv = df_raw[df_raw['ENTSALVER'] == 2].iloc[:, 3:15]
            df2_fv = df_raw[(df_raw['ENTSALVER'] == 3) & (df_raw['Escalonesp1'] <= 3)].iloc[:, 3:15]
            df3_fv = df_raw[(df_raw['ENTSALVER'] == 3) & (df_raw['Escalonesp2'] <= 3)].iloc[:, 15:27]
            df3_fv.columns = df1_fv.columns

            df_fv = pd.concat([df1_fv, df2_fv, df3_fv], axis=0)

            # Subperiodo 2 Verano 
            df1_v = df_raw[df_raw['ENTSALVER'] == 1].iloc[:, 3:15]
            df2_v = df_raw[(df_raw['ENTSALVER'] == 3) & (df_raw['Escalonesp1'] > 3)].iloc[:, 3:15]
            df3_v = df_raw[(df_raw['ENTSALVER'] == 3) & (df_raw['Escalonesp2'] > 3)].iloc[:, 15:27]
            df1_v.columns = df3_v.columns
            df2_v.columns = df3_v.columns

            df_v = pd.concat([df1_v, df2_v, df3_v], axis=0)

            df_t = pd.concat([df_fv, df_v], axis=1)
            df_t.fillna(0.0, inplace=True)
            df_t = df_t.round(2)

            dftrcols = {
                'DetallekWh11': 'Efvb', 'DetallePrecio11': 'Pfvb', 'DetalleSubTotal11': 'Tfvb',
                'DetallekWh12': 'Efvi', 'DetallePrecio12': 'Pfvi', 'DetalleSubTotal12': 'Tfvi', 
                'DetallekWh13': 'Efve', 'DetallePrecio13': 'Pfve', 'DetalleSubTotal13': 'Tfve', 
                'DetallekWh14': 'Efv', 'DetallePrecio14': 'Pfv', 'DetalleSubTotal14': 'Tfv',
                'DetallekWh21': 'Evb', 'DetallePrecio21': 'Pvb', 'DetalleSubTotal21': 'Tvb', 
                'DetallekWh22': 'Evi1', 'DetallePrecio22': 'Pvi1', 'DetalleSubTotal22': 'Tvi1', 
                'DetallekWh23': 'Evi2', 'DetallePrecio23': 'Pvi2', 'DetalleSubTotal23': 'Tvi2', 
                'DetallekWh24': 'Eve', 'DetallePrecio24': 'Pve', 'DetalleSubTotal24': 'Tve',
            }

            df_t = df_t.rename(columns=dftrcols)

            df = pd.concat([df, df_t], axis=1)
            df = df.reset_index(drop=True)

            # TA0 Datos de la cuenta
            TA0 = df.iloc[:, :13]

            # TA1 Datos de usuario
            TA1 = df.iloc[:, 13:23]

            # TA2  Fechas importantes, Monto total y consumo desagregado
            TA2 = df.iloc[:, 23:34]
            colfloat = ['Cargos', 'Días', 'KWh', 'KWh/d', '$/d']
            TA2[colfloat] = TA2[colfloat].astype(float)

            # Calcular la columna $/KWh
            TA2['$/KWh'] = TA2['Cargos'] / TA2['KWh']
            TA2['$/KWh'] = TA2['$/KWh'].round(2)
            TA2 = TA2.sort_values(by='Fecha fin', ascending=True)

            # TA3 Distribución de costos de energía y de la factura
            TA3 = df.iloc[:, [0] + list(range(34, 83))]
            TA3.loc[:, 'Reconexción'] = TA3['Reconexción'] + TA3['Reconexción2']
            TA3.loc[:, 'Factura'] = TA3['Factura'] + TA3['Factura2']
            TA3.loc[:, 'DAP'] = TA3['DAP'] + TA3['DAP2']
            TA3.drop(['Cargo fijo', '2% BT', 'Reconexción2', 'Factura2', 'DAP2', 'Bonificación FP', 'Cargo FP'], axis=1, inplace=False)

            # TAH DataFrame para históricos
            TAH = self.merge_columns_to_pdbt(df=df3)
        
            TAH[['KWh']] = TAH[['KWh']].astype(float)

            TAH['Fecha'] = TAH.apply(self.convert_to_fecha, axis=1)

            TAH['Fecha'] = TAH['Mes'] + ' ' + TAH['Año']
            TAH = TAH.drop(['Mes', 'Año'], axis=1)
            columns = TAH.columns.tolist()
            columns = ['Fecha'] + [col for col in columns if col != 'Fecha']
            TAH = TAH[columns]

            TAH = TAH.drop(TAH[TAH['Fecha'] == '00 0000'].index)

            TAH['Fecha'] = pd.to_datetime(TAH['Fecha'], format="%m %Y")

            TAH = pd.pivot_table(TAH, values=['KWh'], index=['Fecha'],
                                    aggfunc={
                                        'KWh': np.max
                                    })

        else:
            print('Estanos trabajando para incluir la tarifa que tienes contratada.')
        
        return df, TA0, TA1, TA2, TA3, TAH