class DataWeb:
    """
    Clase especializada para extraer datos financieros de Yahoo Finance
    Optimizada para Databricks Community Edition
    """
    
    def __init__(self, listado_indicadores=[]):
        self.listado_indicadores = listado_indicadores
        self.url_historicos = "https://es.finance.yahoo.com/quote/{}/history/"
        self.url_perfil = "https://es.finance.yahoo.com/quote/{}/"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
    
    def obtener_datos(self, indicador=""):
        """
        Extrae datos históricos de un indicador financiero
        """
        try:
            url = self.url_historicos.format(indicador)
            print(f"🔍 Extrayendo datos de: {indicador}")
            
            respuesta = requests.get(url, headers=self.headers, timeout=30)
            if respuesta.status_code != 200:
                print(f"❌ Error HTTP {respuesta.status_code} para {indicador}")
                return pd.DataFrame()
            
            soup = BeautifulSoup(respuesta.text, 'html.parser')
            tabla = soup.select_one('div[data-testid="history-table"] table')
            
            if not tabla:
                print(f"❌ No se encontró tabla de datos para {indicador}")
                return pd.DataFrame()
            
            # Extraer nombres de columnas
            nombre_columnas = [th.get_text(strip=True) for th in tabla.thead.find_all('th')]
            
            # Extraer filas de datos
            filas = []
            for tr in tabla.tbody.find_all('tr'):
                columnas = [td.get_text(strip=True) for td in tr.find_all('td')]
                if len(columnas) == len(nombre_columnas):
                    filas.append(columnas)
            
            # Crear DataFrame
            df = pd.DataFrame(filas, columns=nombre_columnas)
            
            # Normalizar nombres de columnas
            df = df.rename(columns=self._normalizar_columnas())
            
            # Convertir tipos de datos
            df = self.convertir_numericos(df)
            
            # Agregar metadatos
            df["cod_indicador"] = indicador
            df["fecha_extraccion"] = datetime.datetime.now()
            
            print(f"✅ Extraídos {len(df)} registros para {indicador}")
            return df
            
        except Exception as err:
            print(f"❌ Error en obtener_datos para {indicador}: {str(err)}")
            return pd.DataFrame()
    
    def obtener_metadatos(self, indicador=""):
        """
        Extrae metadatos del indicador (nombre, mercado, etc.)
        """
        try:
            url = self.url_perfil.format(indicador)
            respuesta = requests.get(url, headers=self.headers, timeout=30)
            
            if respuesta.status_code != 200:
                return self._metadatos_default(indicador)
            
            soup = BeautifulSoup(respuesta.text, 'html.parser')
            
            # Extraer información básica
            nombre = self._extraer_nombre(soup, indicador)
            moneda = self._extraer_moneda(soup)
            mercado = self._extraer_mercado(soup)
            clasificacion = self._inferir_clasificacion(indicador)
            pais = self._inferir_pais(indicador, mercado)
            
            metadatos = {
                'cod_indicador': indicador,
                'nombre': nombre,
                'pais': pais,
                'clasificacion': clasificacion,
                'moneda': moneda,
                'mercado': mercado,
                'fecha_actualizacion': datetime.datetime.now(),
                'activo': True
            }
            
            print(f"📋 Metadatos extraídos para {indicador}: {nombre}")
            return metadatos
            
        except Exception as err:
            print(f"⚠️ Error extrayendo metadatos para {indicador}: {str(err)}")
            return self._metadatos_default(indicador)
    
    def convertir_numericos(self, df=pd.DataFrame()):
        """
        Convierte columnas numéricas al formato correcto
        """
        df = df.copy()
        if len(df) > 0:
            columnas_numericas = ['abrir', 'max', 'min', 'cerrar', 'cierre_ajustado', 'volumen']
            for col in columnas_numericas:
                if col in df.columns:
                    df[col] = (df[col]
                              .str.replace(r"[^\d,.-]", "", regex=True)  # Eliminar caracteres no numéricos
                              .str.replace(r"\.(?=.*\.)", "", regex=True)  # Eliminar puntos excepto el decimal
                              .str.replace(",", ".")  # Convertir coma decimal a punto
                              .replace(["-", ""], "0")  # Reemplazar guiones y vacíos con 0
                              )
                    # Convertir a numérico
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        return df
    
    def _normalizar_columnas(self):
        """
        Mapeo de nombres de columnas en español a nombres normalizados
        """
        return {
            'Fecha': 'fecha',
            'Abrir': 'abrir',
            'Máx.': 'max',
            'Mín.': 'min',
            'Cerrar': 'cerrar',
            'Cierre ajustado': 'cierre_ajustado',
            'Volumen': 'volumen'
        }
    
    def _extraer_nombre(self, soup, indicador):
        """Extrae el nombre del instrumento"""
        try:
            # Buscar diferentes selectores para el nombre
            selectores = [
                'h1[data-testid="quote-header"]',
                'h1.yf-xxbei9',
                '.yf-xxbei9',
                'h1'
            ]
            
            for selector in selectores:
                elemento = soup.select_one(selector)
                if elemento:
                    nombre = elemento.get_text(strip=True)
                    if nombre and len(nombre) > 1:
                        return nombre[:100]  # Limitar longitud
            
            return f"Instrumento {indicador}"
        except:
            return f"Instrumento {indicador}"
    
    def _extraer_moneda(self, soup):
        """Extrae la moneda del instrumento"""
        try:
            # Buscar indicadores de moneda
            texto = soup.get_text().upper()
            if 'USD' in texto:
                return 'USD'
            elif 'EUR' in texto:
                return 'EUR'
            elif 'GBP' in texto:
                return 'GBP'
            elif 'JPY' in texto:
                return 'JPY'
            else:
                return 'USD'  # Por defecto
        except:
            return 'USD'
    
    def _extraer_mercado(self, soup):
        """Extrae información del mercado"""
        try:
            # Buscar información de mercado
            mercado_elem = soup.select_one('.exchange')
            if mercado_elem:
                return mercado_elem.get_text(strip=True)[:50]
            return 'N/A'
        except:
            return 'N/A'
    
    def _inferir_clasificacion(self, indicador):
        """Infiere la clasificación del instrumento basado en el código"""
        indicador_upper = indicador.upper()
        
        if '^' in indicador or 'IBEX' in indicador_upper or 'STOXX' in indicador_upper:
            return 'INDICE'
        elif '=F' in indicador or 'CL=' in indicador or 'GC=' in indicador:
            return 'COMMODITY'
        elif '=X' in indicador or 'USD' in indicador_upper or 'EUR' in indicador_upper:
            return 'DIVISA'
        elif 'BTC' in indicador_upper or 'ETH' in indicador_upper:
            return 'CRYPTO'
        elif '.MC' in indicador:
            return 'ACCION_ES'
        else:
            return 'ACCION'
    
    def _inferir_pais(self, indicador, mercado):
        """Infiere el país basado en el código y mercado"""
        if '.MC' in indicador or 'IBEX' in indicador.upper():
            return 'España'
        elif 'STOXX' in indicador.upper():
            return 'Europa'
        elif mercado and ('NYSE' in mercado.upper() or 'NASDAQ' in mercado.upper()):
            return 'Estados Unidos'
        else:
            return 'Internacional'
    
    def _metadatos_default(self, indicador):
        """Retorna metadatos por defecto cuando no se pueden extraer"""
        return {
            'cod_indicador': indicador,
            'nombre': f'Instrumento {indicador}',
            'pais': 'N/A',
            'clasificacion': self._inferir_clasificacion(indicador),
            'moneda': 'USD',
            'mercado': 'N/A',
            'fecha_actualizacion': datetime.datetime.now(),
            'activo': True
        }

# Ejemplo de uso
print("🚀 Clase DataWeb mejorada lista para usar")
