#!/usr/bin/env python3
"""
Clase refactorizada del scraper del SAT para uso con Streamlit
"""

import fitz  # PyMuPDF
from PIL import Image
import cv2
import numpy as np
import io
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from datetime import datetime
import urllib3
import warnings
import ssl
import subprocess
import sys
from typing import Dict, List, Optional, Tuple
from functools import lru_cache
import hashlib

class SATScraper:
    def __init__(self):
        self.results = []
        self.qr_detector = cv2.QRCodeDetector()
        self.setup_ssl_bypass()

        # Cache para sesiones HTTP y resultados
        self._session_cache = {}
        self._scraping_cache = {}

        # Configuración optimizada
        self.max_retries = 2
        self.request_timeout = 15  # Reducido de 30s
        self.delay_between_requests = 0.5  # Reducido de 1s

    def setup_ssl_bypass(self):
        """
        Configura múltiples estrategias para bypass SSL
        """
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        warnings.filterwarnings('ignore')
        
        try:
            ssl._create_default_https_context = ssl._create_unverified_context
        except:
            pass

    def extract_qr_from_pdf(self, pdf_bytes: bytes, filename: str) -> Optional[str]:
        """
        Extrae el código QR de la primera página de un PDF desde bytes
        """
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            page = doc[0]  # type: ignore

            mat = fitz.Matrix(3, 3)
            pix = page.get_pixmap(matrix=mat)  # type: ignore
            img_data = pix.tobytes("png")

            img = Image.open(io.BytesIO(img_data))
            img_array = np.array(img)

            if len(img_array.shape) == 3:
                gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY)
            else:
                gray = img_array

            data, bbox, _ = self.qr_detector.detectAndDecode(gray)
            doc.close()

            if data:
                return data
            else:
                return None

        except Exception as e:
            print(f"Error procesando {filename}: {str(e)}")
            return None

    def extract_pdf_text_data(self, pdf_bytes: bytes, filename: str) -> Dict:
        """
        Extrae datos directamente del texto del PDF desde bytes
        """
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")

            # Extraer texto de todas las páginas
            full_text = ""
            for page_num in range(len(doc)):
                page = doc[page_num]  # type: ignore
                text = page.get_text()  # type: ignore
                full_text += text + "\n"

            doc.close()

            # Limpiar el texto
            clean_text = re.sub(r'\s+', ' ', full_text).strip()

            # Extraer datos específicos del PDF
            pdf_data = self.parse_pdf_text(clean_text)
            return pdf_data

        except Exception as e:
            print(f"Error extrayendo texto del PDF {filename}: {str(e)}")
            return {}

    def parse_pdf_text(self, text: str) -> Dict:
        """
        Parsea el texto del PDF para extraer datos estructurados
        """
        data = {}

        try:
            # Patrones específicos para el formato real del PDF del SAT
            patterns = {
                'pdf_rfc': r'RFC:\s*([A-Z0-9]{12,13})',
                'pdf_curp': r'CURP:\s*([A-Z0-9]{18})',
                'pdf_nombre': r'Nombre\s*\(s\):\s*([A-ZÁÉÍÓÚÑ\s]+?)(?=\s*Primer|$)',
                'pdf_primer_apellido': r'Primer Apellido:\s*([A-ZÁÉÍÓÚÑ\s]+?)(?=\s*Segundo|$)',
                'pdf_segundo_apellido': r'Segundo Apellido:\s*([A-ZÁÉÍÓÚÑ\s]+?)(?=\s*Fecha|$)',
                'pdf_fecha_inicio_operaciones': r'Fecha inicio de operaciones:\s*([0-9A-ZÁÉÍÓÚÑ\s]+?)(?=\s*Estatus|$)',
                'pdf_estatus_padron': r'Estatus en el padrón:\s*([A-ZÁÉÍÓÚÑ\s]+?)(?=\s*Fecha|$)',
                'pdf_fecha_ultimo_cambio': r'Fecha de último cambio de estado:\s*([0-9A-ZÁÉÍÓÚÑ\s]+?)(?=\s*Nombre|$)',
                'pdf_nombre_comercial': r'Nombre Comercial:\s*([A-ZÁÉÍÓÚÑ\s]+?)(?=\s*Datos|$)',
                'pdf_codigo_postal': r'Código Postal:\s*(\d{5})',
                'pdf_tipo_vialidad': r'Tipo de Vialidad:\s*([A-ZÁÉÍÓÚÑ\s]+?)(?=\s*Nombre|$)',
                'pdf_nombre_vialidad': r'Nombre de Vialidad:\s*([A-ZÁÉÍÓÚÑ0-9\s]+?)(?=\s*Número|$)',
                'pdf_numero_exterior': r'Número Exterior:\s*([A-ZÁÉÍÓÚÑ0-9\s]+?)(?=\s*Número|$)',
                'pdf_numero_interior': r'Número Interior:\s*([A-ZÁÉÍÓÚÑ0-9\s]*?)(?=\s*Nombre|$)',
                'pdf_nombre_colonia': r'Nombre de la Colonia:\s*([A-ZÁÉÍÓÚÑ\s]*?)(?=\s*Nombre|$)',
                'pdf_nombre_localidad': r'Nombre de la Localidad:\s*([A-ZÁÉÍÓÚÑ\s]+?)(?=\s*Nombre|$)',
                'pdf_municipio': r'Nombre del Municipio o Demarcación Territorial:\s*([A-ZÁÉÍÓÚÑ\s]+?)(?=\s*Nombre|$)',
                'pdf_entidad_federativa': r'Nombre de la Entidad Federativa:\s*([A-ZÁÉÍÓÚÑ\s]+?)(?=\s*Entre|$)',
                'pdf_entre_calle': r'Entre Calle:\s*([A-ZÁÉÍÓÚÑ\s]+?)(?=\s*$)',
            }

            extracted_count = 0
            for key, pattern in patterns.items():
                match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
                if match:
                    value = match.group(1).strip()
                    if value and len(value) > 1:
                        data[key] = value
                        extracted_count += 1

            # Patrones alternativos si no se encontraron suficientes datos
            if extracted_count < 8:
                alt_patterns = {
                    'pdf_rfc_alt': r'([A-Z]{4}\d{6}[A-Z0-9]{3})',
                    'pdf_curp_alt': r'([A-Z]{4}\d{6}[HM][A-Z]{5}[0-9A-Z]\d)',
                    'pdf_nombre_alt': r'(?:Nombre|NOMBRE)[^:]*:?\s*([A-ZÁÉÍÓÚÑ\s]+?)(?=\s*(?:Primer|PRIMER|Apellido|APELLIDO))',
                    'pdf_primer_apellido_alt': r'(?:Primer Apellido|PRIMER APELLIDO)[^:]*:?\s*([A-ZÁÉÍÓÚÑ\s]+?)(?=\s*(?:Segundo|SEGUNDO))',
                    'pdf_segundo_apellido_alt': r'(?:Segundo Apellido|SEGUNDO APELLIDO)[^:]*:?\s*([A-ZÁÉÍÓÚÑ\s]+?)(?=\s*(?:Fecha|FECHA))',
                    'pdf_codigo_postal_alt': r'(?:Código Postal|CÓDIGO POSTAL)[^:]*:?\s*(\d{5})',
                    'pdf_entidad_federativa_alt': r'(?:Entidad Federativa|ENTIDAD FEDERATIVA)[^:]*:?\s*([A-ZÁÉÍÓÚÑ\s]+?)(?=\s*(?:Entre|ENTRE|$))',
                }

                for key, pattern in alt_patterns.items():
                    base_key = key.replace('_alt', '')
                    if base_key not in data:
                        match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
                        if match:
                            value = match.group(1).strip()
                            if value and len(value) > 1:
                                data[base_key] = value
                                extracted_count += 1

        except Exception as e:
            print(f"Error parseando texto del PDF: {str(e)}")

        return data

    def scrape_sat_url_strategy1(self, url: str) -> Optional[str]:
        """
        Estrategia 1: Requests con configuración SSL personalizada
        """
        try:
            session = requests.Session()
            session.verify = False
            
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'es-MX,es;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Cache-Control': 'max-age=0'
            })
            
            # Importaciones con manejo de errores
            try:
                from requests.adapters import HTTPAdapter
                from urllib3.util.ssl_ import create_urllib3_context
                
                class SSLAdapter(HTTPAdapter):
                    def init_poolmanager(self, *args, **kwargs):
                        context = create_urllib3_context()
                        context.set_ciphers('DEFAULT:@SECLEVEL=1')
                        kwargs['ssl_context'] = context
                        return super().init_poolmanager(*args, **kwargs)
                
                session.mount('https://', SSLAdapter())
            except ImportError:
                # Si no se pueden importar las dependencias SSL, continuar sin ellas
                pass
            
            response = session.get(url, timeout=self.request_timeout)
            
            if response.status_code == 200:
                return response.text
            else:
                return None
                
        except Exception:
            return None

    def scrape_sat_url_strategy2(self, url: str) -> Optional[str]:
        """
        Estrategia 2: Usar curl como subprocess
        """
        try:
            curl_command = [
                'curl',
                '-k',
                '--user-agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                '--connect-timeout', str(self.request_timeout),
                '--max-time', str(self.request_timeout * 2),
                '--location',
                '--compressed',
                '--header', 'Accept-Charset: UTF-8',
                '--header', 'Accept-Encoding: gzip, deflate',
                url
            ]
            
            result = subprocess.run(curl_command, capture_output=True, text=True, encoding='utf-8', timeout=self.request_timeout * 2)
            
            if result.returncode == 0 and result.stdout:
                return result.stdout
            else:
                return None
                
        except Exception:
            return None

    def scrape_sat_url_strategy3(self, url: str) -> Optional[str]:
        """
        Estrategia 3: Usar urllib con SSL context personalizado
        """
        try:
            import urllib.request
            import urllib.parse
            
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            context.set_ciphers('DEFAULT:@SECLEVEL=1')
            
            req = urllib.request.Request(url)
            req.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
            req.add_header('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8')
            
            with urllib.request.urlopen(req, context=context, timeout=30) as response:
                content = response.read().decode('utf-8')
                return content
                
        except Exception:
            return None

    def scrape_sat_url_strategy4(self, url: str) -> Optional[str]:
        """
        Estrategia 4: Usar requests con configuración legacy SSL
        """
        try:
            # Intentar importar con manejo de errores
            try:
                # import requests.packages.urllib3.util.ssl_  # type: ignore
                # requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += ':HIGH:!DH:!aNULL'
                pass
            except (ImportError, AttributeError):
                # Si no se puede importar, continuar sin configuración SSL específica
                pass
            
            session = requests.Session()
            session.verify = False
            
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'es-mx',
                'Connection': 'keep-alive'
            })
            
            response = session.get(url, timeout=self.request_timeout)
            
            if response.status_code == 200:
                return response.text
            else:
                return None
                
        except Exception:
            return None

    def install_curl_if_needed(self) -> bool:
        """
        Verifica si curl está disponible
        """
        try:
            subprocess.run(['curl', '--version'], capture_output=True, check=True)
            return True
        except:
            return False

    def _get_url_cache_key(self, url: str) -> str:
        """
        Genera una clave de cache para la URL
        """
        return hashlib.md5(url.encode()).hexdigest()

    def scrape_sat_data(self, url: str, pdf_filename: str) -> Dict:
        """
        Intenta hacer scraping de la URL del SAT usando múltiples estrategias con caching
        """
        # Verificar cache primero
        cache_key = self._get_url_cache_key(url)
        if cache_key in self._scraping_cache:
            cached_result = self._scraping_cache[cache_key].copy()
            cached_result['archivo_pdf'] = pdf_filename  # Actualizar nombre del archivo
            return cached_result

        sat_data = {
            'archivo_pdf': pdf_filename,
            'url': url,
            'fecha_extraccion': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Extraer RFC de la URL
        rfc_match = re.search(r'D3=(\d+)_([A-Z0-9]+)', url)
        if rfc_match:
            sat_data['numero_registro'] = rfc_match.group(1)
            sat_data['rfc'] = rfc_match.group(2)
        
        # Intentar scraping con múltiples estrategias
        html_content = None
        
        if not html_content:
            html_content = self.scrape_sat_url_strategy1(url)
        
        if not html_content and self.install_curl_if_needed():
            html_content = self.scrape_sat_url_strategy2(url)
        
        if not html_content:
            html_content = self.scrape_sat_url_strategy3(url)
        
        if not html_content:
            html_content = self.scrape_sat_url_strategy4(url)
        
        # Si obtuvimos contenido, parsearlo
        if html_content:
            parsed_data = self.parse_sat_content(html_content)
            sat_data.update(parsed_data)
            sat_data['scraping_exitoso'] = 'True'
        else:
            sat_data['scraping_exitoso'] = 'False'
            sat_data['error'] = 'No se pudo acceder al contenido con ninguna estrategia'

        # Guardar en cache (solo si tuvo éxito para evitar cache de errores)
        if sat_data.get('scraping_exitoso') == 'True':
            # Crear copia para cache sin el nombre del archivo específico
            cache_data = sat_data.copy()
            cache_data['archivo_pdf'] = 'cached'  # Marcador genérico
            self._scraping_cache[cache_key] = cache_data

        return sat_data

    def decode_special_characters(self, text: str) -> str:
        """
        Decodifica caracteres especiales del HTML del SAT
        """
        char_mapping = {
            'Ã¡': 'á', 'Ã©': 'é', 'Ã­': 'í', 'Ã³': 'ó', 'Ãº': 'ú', 'Ã±': 'ñ',
            'Ã': 'Á', 'Ã': 'É', 'Ã': 'Í', 'Ã': 'Ó', 'Ã': 'Ú', 'Ã': 'Ñ',
            'Ã³n': 'ón', 'Ã­stica': 'ística', 'Ã³n del': 'ón del',
            'Ãºltimo': 'último', 'Ãºmero': 'úmero', 'Ã±iga': 'ñiga',
            'Ã¡s': 'ás', 'Ã©s': 'és', 'Ã­a': 'ía', 'Ã³n': 'ón', 'Ãºa': 'úa',
            'Ã±o': 'ño', 'Ã±a': 'ña', 'Ã±e': 'ñe', 'Ã±i': 'ñi', 'Ã±u': 'ñu',
            'Ã¡n': 'án', 'Ã©n': 'én', 'Ã­n': 'ín', 'Ã³n': 'ón', 'Ãºn': 'ún',
            'Ã±n': 'ñn', 'Ã¡r': 'ár', 'Ã©r': 'ér', 'Ã­r': 'ír', 'Ã³r': 'ór',
            'Ãºr': 'úr', 'Ã±r': 'ñr', 'Ã¡s': 'ás', 'Ã©s': 'és', 'Ã­s': 'ís',
            'Ã³s': 'ós', 'Ãºs': 'ús', 'Ã±s': 'ñs', 'Ã¡t': 'át', 'Ã©t': 'ét',
            'Ã­t': 'ít', 'Ã³t': 'ót', 'Ãºt': 'út', 'Ã±t': 'ñt'
        }
        
        for wrong_char, correct_char in char_mapping.items():
            text = text.replace(wrong_char, correct_char)
        
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def parse_sat_content(self, html_content: str) -> Dict:
        """
        Parsea el contenido HTML del SAT para extraer datos estructurados
        """
        data = {}

        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            text = soup.get_text()
            text = self.decode_special_characters(text)

            patterns = {
                'web_curp': r'CURP:\s*([A-Z0-9]{18})',
                'web_nombre': r'Nombre:\s*([A-ZÁÉÍÓÚÑ\s]+?)(?=Apellido|$)',
                'web_apellido_paterno': r'Apellido Paterno:\s*([A-ZÁÉÍÓÚÑ\s]+?)(?=Apellido|$)',
                'web_apellido_materno': r'Apellido Materno:\s*([A-ZÁÉÍÓÚÑ\s]+?)(?=Fecha|$)',
                'web_fecha_nacimiento': r'Fecha Nacimiento:\s*(\d{2}-\d{2}-\d{4})',
                'web_fecha_inicio_operaciones': r'Fecha de Inicio de operaciones:\s*(\d{2}-\d{2}-\d{4})',
                'web_situacion_contribuyente': r'Situación del contribuyente:\s*([A-ZÁÉÍÓÚÑ\s]+?)(?=Fecha|$)',
                'web_fecha_ultimo_cambio': r'Fecha del último cambio de situación:\s*(\d{2}-\d{2}-\d{4})',
                'web_entidad_federativa': r'Entidad Federativa:\s*([A-ZÁÉÍÓÚÑ\s]+?)(?=Municipio|$)',
                'web_municipio': r'Municipio o delegación:\s*([A-ZÁÉÍÓÚÑ\s]+?)(?=Localidad|$)',
                'web_localidad': r'Localidad:\s*([A-ZÁÉÍÓÚÑ\s]+?)(?=Tipo|$|[0-9])',
                'web_tipo_vialidad': r'Tipo de vialidad:\s*([A-ZÁÉÍÓÚÑ\s]+?)(?=Nombre|$)',
                'web_nombre_vialidad': r'Nombre de la vialidad:\s*([A-ZÁÉÍÓÚÑ0-9\s]+?)(?=Número|$)',
                'web_numero_exterior': r'Número exterior:\s*([A-ZÁÉÍÓÚÑ0-9\s]+?)(?=Número|CP|$)',
                'web_numero_interior': r'Número interior:\s*([A-ZÁÉÍÓÚÑ0-9\s]*?)(?=CP|$)',
                'web_cp': r'CP:\s*(\d{5})',
                'web_correo_electronico': r'Correo electrónico:\s*([A-Za-z0-9@._-]+)',
                'web_al': r'AL:\s*([A-ZÁÉÍÓÚÑ\s0-9]+?)(?=Características|$)',
                'web_regimen': r'Régimen:\s*([^\\n]+?)(?=Fecha|$)',
                'web_fecha_alta': r'Fecha de alta:\s*(\d{2}-\d{2}-\d{4})',
            }

            extracted_count = 0
            for key, pattern in patterns.items():
                match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
                if match:
                    value = match.group(1).strip()
                    if value and len(value) > 1 and value != ':':
                        data[key] = value
                        extracted_count += 1

            # Patrones alternativos si no se encontraron suficientes datos
            if extracted_count < 10:
                alt_patterns = {
                    'web_curp_alt': r'([A-Z]{4}\d{6}[HM][A-Z]{5}[0-9A-Z]\d)',
                    'web_nombre_alt': r'(?:Nombre|NOMBRE)[:\s]*([A-ZÁÉÍÓÚÑ\s]+?)(?:Apellido|APELLIDO)',
                    'web_apellido_paterno_alt': r'(?:Apellido Paterno|APELLIDO PATERNO)[:\s]*([A-ZÁÉÍÓÚÑ\s]+?)(?:Apellido|APELLIDO|Fecha|FECHA)',
                    'web_fecha_nacimiento_alt': r'(?:Fecha Nacimiento|FECHA NACIMIENTO)[:\s]*(\d{2}-\d{2}-\d{4})',
                    'web_entidad_federativa_alt': r'(?:Entidad Federativa|ENTIDAD FEDERATIVA)[:\s]*([A-ZÁÉÍÓÚÑ\s]+?)(?:Municipio|MUNICIPIO)',
                    'web_municipio_alt': r'(?:Municipio|MUNICIPIO)[^:]*[:\s]*([A-ZÁÉÍÓÚÑ\s]+?)(?:Localidad|LOCALIDAD|Tipo|TIPO)',
                    'web_cp_alt': r'(?:CP|C\.P\.)[:\s]*(\d{5})',
                    'web_localidad_alt': r'Localidad:\s*([A-ZÁÉÍÓÚÑ\s]+?)(?:Tipo|TIPO|[0-9]|$)',
                }

                for key, pattern in alt_patterns.items():
                    base_key = key.replace('_alt', '')
                    if base_key not in data:
                        match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
                        if match:
                            value = match.group(1).strip()
                            if value and len(value) > 1:
                                data[base_key] = value
                                extracted_count += 1

            # Buscar tablas adicionales
            tables = soup.find_all('table')
            if tables:
                for i, table in enumerate(tables):
                    table_data = self.parse_table(table)
                    if table_data:
                        data[f'tabla_{i+1}'] = table_data

        except Exception as e:
            print(f"Error parseando contenido: {str(e)}")

        return data

    def parse_table(self, table) -> Optional[List]:
        """
        Parsea una tabla HTML
        """
        try:
            rows = []
            for row in table.find_all('tr'):
                cells = []
                for cell in row.find_all(['td', 'th']):
                    text = cell.get_text(strip=True)
                    if text:
                        cells.append(text)
                
                if cells:
                    rows.append(cells)
            
            return rows if rows else None
            
        except Exception:
            return None

    def process_pdf(self, pdf_bytes: bytes, filename: str) -> Dict:
        """
        Procesa un PDF individual y retorna los resultados
        """
        result = {
            'archivo_pdf': filename,
            'fecha_extraccion': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        # Extraer QR
        url = self.extract_qr_from_pdf(pdf_bytes, filename)
        result['url_encontrada'] = 'True' if url is not None else 'False'
        result['url'] = url if url else 'No encontrada'

        # Extraer datos del PDF
        pdf_data = self.extract_pdf_text_data(pdf_bytes, filename)
        result.update(pdf_data)
        result['extraccion_pdf_exitosa'] = 'True' if len(pdf_data) > 0 else 'False'

        if url:
            # Hacer scraping de la URL
            sat_data = self.scrape_sat_data(url, filename)
            result.update(sat_data)
        else:
            result['scraping_exitoso'] = 'False'
            result['error'] = 'No se pudo extraer código QR'

        return result

    def export_to_excel(self, results: List[Dict], filename: str = 'resultados_scraping_sat.xlsx') -> bytes:
        """
        Exporta los resultados del scraping a Excel y retorna los bytes del archivo
        """
        if not results:
            raise ValueError("No hay resultados para exportar")
        
        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                
                # Hoja 1: Resumen de scraping
                summary_data = []
                for result in results:
                    nombre_completo = ""
                    if result.get('web_nombre') and result.get('web_apellido_paterno'):
                        nombre_completo = f"{result.get('web_nombre', '')} {result.get('web_apellido_paterno', '')} {result.get('web_apellido_materno', '')}".strip()
                    elif result.get('pdf_nombre') and result.get('pdf_primer_apellido'):
                        nombre_completo = f"{result.get('pdf_nombre', '')} {result.get('pdf_primer_apellido', '')} {result.get('pdf_segundo_apellido', '')}".strip()
                    
                    rfc = result.get('web_rfc') or result.get('pdf_rfc') or result.get('rfc', '')
                    curp = result.get('web_curp') or result.get('pdf_curp') or result.get('curp', '')
                    situacion = result.get('web_situacion_contribuyente', '')
                    municipio = result.get('web_municipio') or result.get('pdf_municipio', '')
                    estado = result.get('web_entidad_federativa') or result.get('pdf_entidad_federativa', '')
                    
                    summary_row = {
                        'Archivo PDF': result.get('archivo_pdf', ''),
                        'RFC': rfc,
                        'Scraping Exitoso': 'SÍ' if result.get('scraping_exitoso') else 'NO',
                        'Nombre Completo': nombre_completo,
                        'CURP': curp,
                        'Situación': situacion,
                        'Municipio': municipio,
                        'Estado': estado,
                        'Error': result.get('error', ''),
                        'URL': result.get('url', '')
                    }
                    summary_data.append(summary_row)
                
                df_summary = pd.DataFrame(summary_data)
                df_summary.to_excel(writer, sheet_name='Resumen Scraping', index=False)
                
                # Hoja 2: Datos completos extraídos del web
                detailed_data = []
                for result in results:
                    if result.get('scraping_exitoso'):
                        detailed_row = {
                            'Archivo PDF': result.get('archivo_pdf', ''),
                            'RFC': result.get('rfc', ''),
                            'CURP (Web)': result.get('web_curp', ''),
                            'Nombre (Web)': result.get('web_nombre', ''),
                            'Apellido Paterno (Web)': result.get('web_apellido_paterno', ''),
                            'Apellido Materno (Web)': result.get('web_apellido_materno', ''),
                            'Fecha Nacimiento (Web)': result.get('web_fecha_nacimiento', ''),
                            'Fecha Inicio Operaciones (Web)': result.get('web_fecha_inicio_operaciones', ''),
                            'Situación Contribuyente (Web)': result.get('web_situacion_contribuyente', ''),
                            'Fecha Último Cambio (Web)': result.get('web_fecha_ultimo_cambio', ''),
                            'Entidad Federativa (Web)': result.get('web_entidad_federativa', ''),
                            'Municipio (Web)': result.get('web_municipio', ''),
                            'Localidad (Web)': result.get('web_localidad', ''),
                            'Tipo Vialidad (Web)': result.get('web_tipo_vialidad', ''),
                            'Nombre Vialidad (Web)': result.get('web_nombre_vialidad', ''),
                            'Número Exterior (Web)': result.get('web_numero_exterior', ''),
                            'Número Interior (Web)': result.get('web_numero_interior', ''),
                            'CP (Web)': result.get('web_cp', ''),
                            'Correo Electrónico (Web)': result.get('web_correo_electronico', ''),
                            'AL (Web)': result.get('web_al', ''),
                            'Régimen (Web)': result.get('web_regimen', ''),
                            'Fecha Alta (Web)': result.get('web_fecha_alta', ''),
                            'URL Original': result.get('url', '')
                        }
                        detailed_data.append(detailed_row)
                
                if detailed_data:
                    df_detailed = pd.DataFrame(detailed_data)
                    df_detailed.to_excel(writer, sheet_name='Datos Extraídos', index=False)

                # Hoja 3: Datos del PDF
                pdf_data = []
                for result in results:
                    if result.get('extraccion_pdf_exitosa'):
                        pdf_row = {
                            'Archivo PDF': result.get('archivo_pdf', ''),
                            'RFC (PDF)': result.get('pdf_rfc', result.get('pdf_rfc_alt', '')),
                            'CURP (PDF)': result.get('pdf_curp', result.get('pdf_curp_alt', '')),
                            'Nombre(s)': result.get('pdf_nombre', result.get('pdf_nombre_alt', '')),
                            'Primer Apellido': result.get('pdf_primer_apellido', result.get('pdf_apellido_alt', '')),
                            'Segundo Apellido': result.get('pdf_segundo_apellido', ''),
                            'Fecha Inicio Operaciones': result.get('pdf_fecha_inicio_operaciones', ''),
                            'Estatus en el Padrón': result.get('pdf_estatus_padron', ''),
                            'Fecha Último Cambio Estado': result.get('pdf_fecha_ultimo_cambio', ''),
                            'Nombre Comercial': result.get('pdf_nombre_comercial', ''),
                            'Código Postal': result.get('pdf_codigo_postal', ''),
                            'Tipo de Vialidad': result.get('pdf_tipo_vialidad', ''),
                            'Nombre de Vialidad': result.get('pdf_nombre_vialidad', ''),
                            'Número Exterior': result.get('pdf_numero_exterior', ''),
                            'Número Interior': result.get('pdf_numero_interior', ''),
                            'Nombre de la Colonia': result.get('pdf_nombre_colonia', ''),
                            'Nombre de la Localidad': result.get('pdf_nombre_localidad', ''),
                            'Municipio o Demarcación': result.get('pdf_municipio', ''),
                            'Entidad Federativa': result.get('pdf_entidad_federativa', ''),
                            'Entre Calle': result.get('pdf_entre_calle', ''),
                        }
                        pdf_data.append(pdf_row)

                if pdf_data:
                    df_pdf = pd.DataFrame(pdf_data)
                    df_pdf.to_excel(writer, sheet_name='Datos del PDF', index=False)

                # Hoja 4: Estadísticas
                successful_scraping = len([r for r in results if r.get('scraping_exitoso')])
                successful_pdf_extraction = len([r for r in results if r.get('extraccion_pdf_exitosa')])
                total_files = len(results)

                stats_data = [
                    ['Métrica', 'Valor'],
                    ['Total PDFs procesados', total_files],
                    ['Scraping web exitoso', successful_scraping],
                    ['Extracción PDF exitosa', successful_pdf_extraction],
                    ['Scraping web fallido', total_files - successful_scraping],
                    ['Extracción PDF fallida', total_files - successful_pdf_extraction],
                    ['Tasa éxito scraping web', f"{(successful_scraping/total_files*100):.1f}%" if total_files > 0 else "0%"],
                    ['Tasa éxito extracción PDF', f"{(successful_pdf_extraction/total_files*100):.1f}%" if total_files > 0 else "0%"],
                    ['Fecha procesamiento', datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
                ]

                df_stats = pd.DataFrame(stats_data[1:], columns=stats_data[0])
                df_stats.to_excel(writer, sheet_name='Estadísticas', index=False)
            
            # Leer el archivo generado y retornar bytes
            with open(filename, 'rb') as f:
                excel_bytes = f.read()
            
            # Eliminar archivo temporal
            os.remove(filename)
            
            return excel_bytes

        except Exception as e:
            print(f"Error exportando: {str(e)}")
            raise 