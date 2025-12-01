#!/usr/bin/env python3
"""
Utilidades para la aplicación Streamlit del Scraper SAT
"""

import streamlit as st
import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime
import gc

def clear_memory():
    """
    Libera memoria no necesaria
    """
    gc.collect()

def format_status_icon(status: bool) -> str:
    """
    Retorna un emoji para indicar el estado de una operación
    """
    return "✅" if status else "❌"

def format_status_text(status: bool) -> str:
    """
    Retorna texto formateado para el estado
    """
    return "Exitoso" if status else "Fallido"

def create_summary_dataframe(results: List[Dict]) -> pd.DataFrame:
    """
    Crea un DataFrame de resumen para mostrar en Streamlit (optimizado para memoria)
    """
    if not results:
        return pd.DataFrame()

    # Usar dictionary comprehension más eficiente
    summary_data = []

    for result in results:
        # Construir nombre completo
        nombre_completo = ""
        if result.get('web_nombre') and result.get('web_apellido_paterno'):
            nombre_completo = f"{result.get('web_nombre', '')} {result.get('web_apellido_paterno', '')} {result.get('web_apellido_materno', '')}".strip()
        elif result.get('pdf_nombre') and result.get('pdf_primer_apellido'):
            nombre_completo = f"{result.get('pdf_nombre', '')} {result.get('pdf_primer_apellido', '')} {result.get('pdf_segundo_apellido', '')}".strip()
        
        # Obtener datos principales
        rfc = result.get('web_rfc') or result.get('pdf_rfc') or result.get('rfc', '')
        curp = result.get('web_curp') or result.get('pdf_curp') or result.get('curp', '')
        situacion = result.get('web_situacion_contribuyente', '')
        municipio = result.get('web_municipio') or result.get('pdf_municipio', '')
        estado = result.get('web_entidad_federativa') or result.get('pdf_entidad_federativa', '')
        
        summary_row = {
            '📄 Archivo': result.get('archivo_pdf', ''),
            '🆔 RFC': rfc,
            '🌐 Scraping Web': format_status_icon(result.get('scraping_exitoso', False)),
            '📋 Nombre': nombre_completo,
            '🆔 CURP': curp,
            '📊 Situación': situacion,
            '🏘️ Municipio': municipio,
            '🏛️ Estado': estado,
            '❌ Error': result.get('error', ''),
            '🔗 URL': result.get('url', '')[:50] + '...' if len(result.get('url', '')) > 50 else result.get('url', '')
            }
        summary_data.append(summary_row)

    # Crear DataFrame y liberar memoria
    df = pd.DataFrame(summary_data)
    del summary_data  # Liberar memoria de la lista
    clear_memory()    # Liberar memoria adicional

    return df

def create_detailed_dataframe(results: List[Dict]) -> pd.DataFrame:
    """
    Crea un DataFrame detallado con todos los datos extraídos
    """
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
    
    return pd.DataFrame(detailed_data)

def create_pdf_dataframe(results: List[Dict]) -> pd.DataFrame:
    """
    Crea un DataFrame con los datos extraídos del PDF
    """
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
    
    return pd.DataFrame(pdf_data)

def create_stats_dataframe(results: List[Dict]) -> pd.DataFrame:
    """
    Crea un DataFrame con estadísticas del procesamiento
    """
    successful_scraping = len([r for r in results if r.get('scraping_exitoso')])
    successful_pdf_extraction = len([r for r in results if r.get('extraccion_pdf_exitosa')])
    total_files = len(results)
    
    # Crear DataFrame con datos y columnas por separado para evitar conflictos de tipos
    df = pd.DataFrame([
        ['📊 Total PDFs procesados', str(total_files)],
        ['✅ Scraping web exitoso', str(successful_scraping)],
        ['📄 Extracción PDF exitosa', str(successful_pdf_extraction)],
        ['❌ Scraping web fallido', str(total_files - successful_scraping)],
        ['❌ Extracción PDF fallida', str(total_files - successful_pdf_extraction)],
        ['📈 Tasa éxito scraping web', f"{(successful_scraping/total_files*100):.1f}%" if total_files > 0 else "0%"],
        ['📈 Tasa éxito extracción PDF', f"{(successful_pdf_extraction/total_files*100):.1f}%" if total_files > 0 else "0%"],
        ['🕒 Fecha procesamiento', datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
    ])
    
    # Asignar nombres de columnas después de crear el DataFrame
    df.columns = ['Métrica', 'Valor']
    return df

def display_file_info(uploaded_file) -> None:
    """
    Muestra información del archivo cargado
    """
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("📄 Nombre", uploaded_file.name)
    
    with col2:
        st.metric("📏 Tamaño", f"{uploaded_file.size / 1024:.1f} KB")
    
    with col3:
        st.metric("📅 Tipo", uploaded_file.type)

def display_processing_status(current: int, total: int, filename: str, status: str) -> None:
    """
    Muestra el estado del procesamiento actual
    """
    progress = current / total if total > 0 else 0
    
    st.progress(progress)
    st.write(f"📄 Procesando: {filename}")
    st.write(f"🔄 Estado: {status}")
    st.write(f"📊 Progreso: {current}/{total} ({progress*100:.1f}%)")

def validate_pdf_file(uploaded_file) -> bool:
    """
    Valida que el archivo sea un PDF válido
    """
    if uploaded_file is None:
        return False
    
    if not uploaded_file.name.lower().endswith('.pdf'):
        return False
    
    if uploaded_file.size == 0:
        return False
    
    return True

def get_file_icon(filename: str) -> str:
    """
    Retorna un emoji apropiado para el tipo de archivo
    """
    if filename.lower().endswith('.pdf'):
        return "📄"
    elif filename.lower().endswith('.xlsx'):
        return "📊"
    else:
        return "📁"

def format_error_message(error: str) -> str:
    """
    Formatea mensajes de error para mejor legibilidad
    """
    if not error:
        return "Sin errores"
    
    # Truncar mensajes muy largos
    if len(error) > 100:
        return error[:100] + "..."
    
    return error

def create_download_filename() -> str:
    """
    Crea un nombre de archivo para descarga con timestamp
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return f"resultados_scraping_sat_{timestamp}.xlsx" 