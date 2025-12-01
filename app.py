#!/usr/bin/env python3
"""
Aplicación Streamlit para Scraper de CSF
"""

import streamlit as st
import pandas as pd
import time
from typing import List, Dict
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from sat_scraper import SATScraper
import utils

# Configuración de la página con estilo corporativo
st.set_page_config(
    page_title="Scraper CSF SAT",
    page_icon="📂",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personalizado con estilo corporativo verde oscuro
st.markdown("""
<style>
    /* Paleta de Colores Corporativa */
    :root {
        --verde-principal: #06752e;
        --verde-claro: #1a7f37;
        --verde-oscuro: #0a8c3d;
        --fondo-oscuro: #0d1117;
        --fondo-semi-transparente: rgba(13, 17, 23, 0.8);
        --fondo-transparente: rgba(13, 17, 23, 0.6);
        --texto-principal: #e6edf3;
        --texto-secundario: #c9d1d9;
        --exito-verde: #1a7f37;
        --advertencia-amarillo: #ffc107;
        --error-rojo: #dc3545;
    }

    /* Reset general */
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1200px;
    }

    /* Header Principal */
    .main-header {
        background: linear-gradient(135deg, #0d1117 0%, #06752e 50%, #0d1117 100%);
        padding: 2rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin-bottom: 2rem;
        box-shadow: 0 4px 12px rgba(6, 117, 46, 0.3);
        border: 1px solid rgba(6, 117, 46, 0.3);
    }

    .main-header h1 {
        margin: 0;
        font-size: 2.5rem;
        font-weight: 700;
        color: #ffffff;
        text-shadow: 0 2px 4px rgba(0,0,0,0.5);
    }

    .main-header p {
        color: #c9d1d9;
        margin: 0.5rem 0 0 0;
        font-size: 1.1rem;
    }

    .sub-header {
        color: #c9d1d9;
        text-align: center;
        margin-bottom: 2rem;
        font-size: 1.1rem;
    }

    /* Tarjetas de Estadísticas */
    .stats-card {
        background: rgba(13, 17, 23, 0.8);
        padding: 1.5rem;
        border-radius: 8px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
        margin: 0.5rem 0;
        border-left: 4px solid #06752e;
        color: #c9d1d9;
    }

    /* Cajas de Mensajes */
    .success-box {
        background-color: rgba(26, 127, 55, 0.15);
        border: 1px solid #1a7f37;
        border-radius: 6px;
        padding: 1rem;
        margin: 1rem 0;
        color: #1a7f37;
    }

    .warning-box {
        background-color: rgba(255, 193, 7, 0.15);
        border: 1px solid #ffc107;
        border-radius: 6px;
        padding: 1rem;
        margin: 1rem 0;
        color: #ffc107;
    }

    .error-box {
        background-color: rgba(220, 53, 69, 0.15);
        border: 1px solid #dc3545;
        border-radius: 6px;
        padding: 1rem;
        margin: 1rem 0;
        color: #dc3545;
    }

    .info-box {
        background-color: rgba(13, 17, 23, 0.8);
        border: 1px solid rgba(6, 117, 46, 0.3);
        border-radius: 6px;
        padding: 1rem;
        margin: 1rem 0;
        color: #e6edf3;
    }

    /* Botones */
    .stButton > button {
        background-color: #06752e !important;
        color: white !important;
        border: 1px solid #06752e !important;
        border-radius: 6px !important;
        transition: all 0.3s ease !important;
        font-weight: 500 !important;
        padding: 0.5rem 1.5rem !important;
    }

    .stButton > button:hover {
        background-color: #0a8c3d !important;
        border-color: #0a8c3d !important;
        box-shadow: 0 2px 8px rgba(6, 117, 46, 0.4) !important;
        transform: translateY(-1px) !important;
    }

    .stDownloadButton > button {
        background-color: #06752e !important;
        color: white !important;
        border: 1px solid #06752e !important;
        border-radius: 6px !important;
        font-weight: 500 !important;
    }

    .stDownloadButton > button:hover {
        background-color: #0a8c3d !important;
        border-color: #0a8c3d !important;
        box-shadow: 0 2px 8px rgba(6, 117, 46, 0.4) !important;
        transform: translateY(-1px) !important;
    }

    /* Métricas */
    .stMetric {
        background-color: rgba(13, 17, 23, 0.8) !important;
        border: 1px solid rgba(6, 117, 46, 0.3) !important;
        padding: 1rem !important;
        border-radius: 8px !important;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3) !important;
    }

    .stMetric > div > div > div > div > div {
        color: #1a7f37 !important;
        font-weight: bold !important;
    }

    .stMetric > div > div > div > div > div > div {
        color: #c9d1d9 !important;
    }

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        background-color: rgba(13, 17, 23, 0.8) !important;
        border-radius: 8px !important;
        border: 1px solid rgba(6, 117, 46, 0.3) !important;
    }

    .stTabs [data-baseweb="tab"] {
        background-color: rgba(6, 117, 46, 0.2) !important;
        color: #e6edf3 !important;
        border-radius: 6px !important;
    }

    .stTabs [data-baseweb="tab"]:hover {
        background-color: rgba(6, 117, 46, 0.4) !important;
    }

    /* File Uploader */
    .stFileUploader {
        border: 2px dashed rgba(6, 117, 46, 0.5) !important;
        border-radius: 8px !important;
        background-color: rgba(13, 17, 23, 0.4) !important;
    }

    /* Sliders */
    .stSlider > div > div > div {
        background-color: #06752e !important;
    }

    /* Selectbox y otros inputs */
    .stSelectbox > div > div > div {
        background-color: rgba(13, 17, 23, 0.8) !important;
        border: 1px solid rgba(6, 117, 46, 0.3) !important;
        color: #e6edf3 !important;
    }

    /* DataFrames */
    .stDataFrame {
        background-color: rgba(13, 17, 23, 0.8) !important;
        border: 1px solid rgba(6, 117, 46, 0.3) !important;
        border-radius: 8px !important;
    }

    .dataframe-container {
        border-radius: 8px !important;
        overflow: hidden !important;
    }

    /* Expanders */
    .streamlit-expanderHeader {
        background-color: rgba(13, 17, 23, 0.6) !important;
        border-radius: 6px !important;
        border: 1px solid rgba(6, 117, 46, 0.2) !important;
    }

    /* Progress bar */
    .stProgress > div > div > div > div {
        background-color: #06752e !important;
    }

    /* Footer */
    .footer {
        text-align: center;
        color: #e6edf3;
        padding: 20px;
        border-top: 1px solid rgba(6, 117, 46, 0.3);
        background: rgba(13, 17, 23, 0.8);
        border-radius: 8px;
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
        margin-top: 2rem;
    }

    /* Textos generales */
    h1, h2, h3, h4, h5, h6 {
        color: #e6edf3 !important;
    }

    .stMarkdown {
        color: #c9d1d9 !important;
    }

    .element-container {
        background-color: transparent !important;
    }
</style>
""", unsafe_allow_html=True)

def create_main_header(title, subtitle=""):
    """Crea un header principal con estilo corporativo"""
    st.markdown(f"""
    <div class="main-header">
        <h1>{title}</h1>
        <p>{subtitle}</p>
    </div>
    """, unsafe_allow_html=True)

def create_sidebar_header(title="⚙️ Configuración"):
    """Crea un header consistente para sidebar"""
    st.sidebar.markdown(f"""
    <div class="sidebar-header">
        <h2 style="margin: 0; font-size: 1.2em;">{title}</h2>
    </div>
    """, unsafe_allow_html=True)

def create_section_header(title, icon="📁"):
    """Crea un header de sección consistente"""
    st.markdown(f"""
    <div style='background: rgba(13, 17, 23, 0.6); border: 1px solid rgba(6, 117, 46, 0.3);
                padding: 1.5rem; border-radius: 8px; margin-bottom: 1rem;
                box-shadow: 0 4px 8px rgba(0,0,0,0.2);'>
        <h2 style='color: #1a7f37; margin-top: 0; margin-bottom: 1rem;'>{icon} {title}</h2>
    </div>
    """, unsafe_allow_html=True)

def success_message(message):
    """Crea un mensaje de éxito consistente"""
    st.markdown(f"""
    <div class="success-box">
        ✅ <strong>{message}</strong>
    </div>
    """, unsafe_allow_html=True)

def warning_message(message):
    """Crea un mensaje de advertencia consistente"""
    st.markdown(f"""
    <div class="warning-box">
        ⚠️ <strong>{message}</strong>
    </div>
    """, unsafe_allow_html=True)

def info_message(message):
    """Crea un mensaje de información consistente"""
    st.markdown(f"""
    <div class="info-box">
        ℹ️ <strong>{message}</strong>
    </div>
    """, unsafe_allow_html=True)

def create_footer(app_name, description):
    """Crea un footer consistente"""
    st.markdown("---")
    st.markdown(f"""
    <div class="footer">
        <p style="font-size: 1.1em; margin-bottom: 10px;">🏢 <strong style="color: #1a7f37;">{app_name}</strong></p>
        <p style="color: #c9d1d9; margin: 5px 0;">{description}</p>
        <p style="color: #1a7f37; font-weight: 500; margin-top: 10px;">✨ Optimizado para extracción de datos del SAT</p>
    </div>
    """, unsafe_allow_html=True)

def main():
    """
    Función principal de la aplicación
    """
    
    # Header principal con estilo corporativo
    create_main_header(
        title="📂 Scraper CSF SAT",
        subtitle="Extrae códigos QR, realiza scraping web y obtiene datos estructurados de PDFs de las CSF del SAT"
    )

    # Descripción de pestañas
    st.markdown("""
    <p class="sub-header">
        📤 <strong>Cargar PDFs</strong>: Sube archivos PDF de las CSF y configura opciones de procesamiento<br>
        📊 <strong>Resultados</strong>: Visualiza datos extraídos en tablas interactivas<br>
        📈 <strong>Estadísticas</strong>: Analiza métricas de éxito y rendimiento<br>
        📥 <strong>Descargar</strong>: Exporta resultados a archivos Excel
    </p>
    """, unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        create_sidebar_header()
        
        # Opciones de procesamiento
        st.subheader("🔧 Opciones")
        enable_web_scraping = st.checkbox("🌐 Habilitar scraping web", value=True, help="Extrae datos adicionales de las URLs de las CSF")
        enable_pdf_extraction = st.checkbox("📄 Habilitar extracción PDF", value=True, help="Extrae datos directamente del contenido del PDF")

        # Opciones de rendimiento
        st.subheader("⚡ Rendimiento")
        col1, col2 = st.columns(2)
        with col1:
            max_workers = st.slider("🔄 Procesamiento paralelo", min_value=1, max_value=2, value=2,
                                   help="Número máximo de archivos procesando simultáneamente (óptimo para hasta 10 archivos)")
        with col2:
            timeout = st.slider("⏱️ Timeout (segundos)", min_value=5, max_value=60, value=15,
                              help="Tiempo máximo de espera por solicitud")
        
        # Información
        st.subheader("ℹ️ Información")
        st.info("""
        **Funcionalidades:**
        - 📱 Extracción de códigos QR
        - 🌐 Scraping web con múltiples estrategias
        - 📄 Extracción de datos del PDF
        - 📊 Exportación a Excel
        
        **Formatos soportados:**
        - PDF (archivos CSF)
        """)
        
        # Estadísticas de sesión
        if 'results' in st.session_state and st.session_state.results:
            st.subheader("📊 Estadísticas de Sesión")
            total_files = len(st.session_state.results)
            successful_scraping = len([r for r in st.session_state.results if r.get('scraping_exitoso')])
            successful_pdf = len([r for r in st.session_state.results if r.get('extraccion_pdf_exitosa')])
            
            st.metric("📄 Total procesados", total_files)
            st.metric("✅ Scraping exitoso", successful_scraping)
            st.metric("📄 PDF exitoso", successful_pdf)
    
    # Contenido principal
    tab1, tab2, tab3, tab4 = st.tabs(["📤 Cargar PDFs", "📊 Resultados", "📈 Estadísticas", "📥 Descargar"])
    
    with tab1:
        st.header("📤 Carga de Archivos PDF")
        
        # Información de límites
        info_message("⚡ **Versión Online Gratuita** - Máximo 10 archivos por sesión (óptimo para archivos de ~140KB)")

        # Carga de archivos
        uploaded_files = st.file_uploader(
            "Selecciona archivos PDF (máximo 10)",
            type=['pdf'],
            accept_multiple_files=True,
            help="Puedes cargar múltiples archivos PDF a la vez. Máximo 10 archivos por procesamiento. Ideal para archivos de 136-140KB."
        )

        # Limitar número de archivos
        if uploaded_files and len(uploaded_files) > 10:
            warning_message("Para mejor rendimiento, se procesarán solo los primeros 10 archivos.")
            uploaded_files = uploaded_files[:10]

        if uploaded_files:
            success_message(f"{len(uploaded_files)} archivo(s) cargado(s) correctamente")
            
            # Mostrar información de archivos
            st.subheader("📋 Información de Archivos")
            for i, file in enumerate(uploaded_files):
                with st.expander(f"📄 {file.name}"):
                    utils.display_file_info(file)
            
            # Botón de procesamiento
            if st.button("🚀 Iniciar Procesamiento", type="primary", use_container_width=True):
                process_files(uploaded_files, enable_web_scraping, enable_pdf_extraction, max_workers, timeout)
    
    with tab2:
        st.header("📊 Resultados del Procesamiento")
        
        if 'results' in st.session_state and st.session_state.results:
            # Pestañas para diferentes vistas
            result_tab1, result_tab2, result_tab3 = st.tabs(["📋 Resumen", "🌐 Datos Web", "📄 Datos PDF"])
            
            with result_tab1:
                st.subheader("📋 Resumen General")
                summary_df = utils.create_summary_dataframe(st.session_state.results)
                st.dataframe(summary_df, use_container_width=True)
            
            with result_tab2:
                st.subheader("🌐 Datos Extraídos del Web")
                detailed_df = utils.create_detailed_dataframe(st.session_state.results)
                if not detailed_df.empty:
                    st.dataframe(detailed_df, use_container_width=True)
                else:
                    st.info("No hay datos del web para mostrar")
            
            with result_tab3:
                st.subheader("📄 Datos Extraídos del PDF")
                pdf_df = utils.create_pdf_dataframe(st.session_state.results)
                if not pdf_df.empty:
                    st.dataframe(pdf_df, use_container_width=True)
                else:
                    st.info("No hay datos del PDF para mostrar")
        else:
            st.info("📤 No hay resultados para mostrar. Carga archivos PDF y procesa para ver los resultados.")
    
    with tab3:
        st.header("📈 Estadísticas del Procesamiento")
        
        if 'results' in st.session_state and st.session_state.results:
            # Métricas principales
            col1, col2, col3, col4 = st.columns(4)
            
            total_files = len(st.session_state.results)
            successful_scraping = len([r for r in st.session_state.results if r.get('scraping_exitoso')])
            successful_pdf = len([r for r in st.session_state.results if r.get('extraccion_pdf_exitosa')])
            files_with_qr = len([r for r in st.session_state.results if r.get('url_encontrada')])
            
            with col1:
                st.metric("📄 Total PDFs", total_files)
            
            with col2:
                st.metric("✅ Scraping Web", successful_scraping, f"{successful_scraping/total_files*100:.1f}%")
            
            with col3:
                st.metric("📄 Extracción PDF", successful_pdf, f"{successful_pdf/total_files*100:.1f}%")
            
            with col4:
                st.metric("📱 Códigos QR", files_with_qr, f"{files_with_qr/total_files*100:.1f}%")
            
            # Tabla de estadísticas detalladas
            st.subheader("📊 Estadísticas Detalladas")
            stats_df = utils.create_stats_dataframe(st.session_state.results)
            st.dataframe(stats_df, use_container_width=True)
            
            # Gráficos (si hay datos)
            if total_files > 0:
                st.subheader("📊 Gráficos")
                
                # Gráfico de éxito por tipo
                success_data = {
                    'Tipo': ['Scraping Web', 'Extracción PDF', 'Códigos QR'],
                    'Exitosos': [successful_scraping, successful_pdf, files_with_qr],
                    'Fallidos': [total_files - successful_scraping, total_files - successful_pdf, total_files - files_with_qr]
                }
                
                success_df = pd.DataFrame(success_data)
                st.bar_chart(success_df.set_index('Tipo'))
        else:
            st.info("📤 No hay datos para mostrar estadísticas. Procesa archivos primero.")
    
    with tab4:
        st.header("📥 Descargar Resultados")
        
        if 'results' in st.session_state and st.session_state.results:
            st.subheader("📊 Exportar a Excel")
            
            # Información del archivo
            filename = utils.create_download_filename()
            st.info(f"📄 Archivo a generar: {filename}")
            
            # Botón de descarga
            if st.button("📥 Descargar Excel", type="primary", use_container_width=True):
                try:
                    with st.spinner("🔄 Generando archivo Excel..."):
                        # Crear instancia del scraper
                        scraper = SATScraper()
                        
                        # Generar archivo Excel
                        excel_bytes = scraper.export_to_excel(st.session_state.results, filename)
                        
                        # Crear botón de descarga
                        st.download_button(
                            label="💾 Descargar Archivo Excel",
                            data=excel_bytes,
                            file_name=filename,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True
                        )
                        
                        st.success("✅ Archivo Excel generado correctamente")
                        
                except Exception as e:
                    st.error(f"❌ Error generando archivo: {str(e)}")
            
            # Información del archivo
            st.subheader("📋 Contenido del Archivo")
            st.info("""
            El archivo Excel contiene 4 hojas:
            1. **Resumen Scraping** - Estado general de cada archivo
            2. **Datos Extraídos** - Datos completos del scraping web
            3. **Datos del PDF** - Datos extraídos del contenido del PDF
            4. **Estadísticas** - Métricas del procesamiento
            """)
        else:
            st.info("📤 No hay resultados para descargar. Procesa archivos primero.")

def process_single_file(args):
    """
    Procesa un solo archivo PDF - función worker para procesamiento paralelo
    """
    uploaded_file, enable_web_scraping, enable_pdf_extraction, timeout = args

    try:
        # Crear scraper local para este hilo con configuración optimizada
        scraper = SATScraper()
        scraper.request_timeout = timeout

        # Leer bytes del archivo
        pdf_bytes = uploaded_file.read()

        # Procesar PDF
        result = scraper.process_pdf(pdf_bytes, uploaded_file.name)

        # Aplicar configuraciones
        if not enable_web_scraping:
            result['scraping_exitoso'] = False
            result['error'] = 'Scraping web deshabilitado'

        if not enable_pdf_extraction:
            result['extraccion_pdf_exitosa'] = False

        return result

    except Exception as e:
        return {
            'archivo_pdf': uploaded_file.name,
            'error': f'Error procesando archivo: {str(e)}',
            'scraping_exitoso': 'False',
            'extraccion_pdf_exitosa': 'False',
            'url_encontrada': 'False',
            'url': 'No encontrada'
        }

def process_files(uploaded_files: List, enable_web_scraping: bool, enable_pdf_extraction: bool, max_workers: int = 4, timeout: int = 15):
    """
    Procesa los archivos cargados usando procesamiento paralelo
    """
    results = []

    # Barra de progreso
    progress_bar = st.progress(0)
    status_text = st.empty()

    # Limitar workers para Streamlit Cloud (recursos limitados)
    cloud_limit = 2  # Streamlit Cloud gratuito tiene 1 CPU
    max_workers = min(max_workers, cloud_limit, len(uploaded_files))

    # Preparar argumentos para procesamiento paralelo
    file_args = [(file, enable_web_scraping, enable_pdf_extraction, timeout) for file in uploaded_files]

    # Procesamiento paralelo
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Enviar todas las tareas
        future_to_file = {
            executor.submit(process_single_file, args): args[0].name
            for args in file_args
        }

        completed_count = 0

        # Procesar resultados a medida que se completan
        for future in as_completed(future_to_file):
            try:
                result = future.result()
                results.append(result)
                completed_count += 1

                # Actualizar progreso
                progress = completed_count / len(uploaded_files)
                progress_bar.progress(progress)
                filename = future_to_file[future]
                status_text.text(f"📄 Completado: {filename} ({completed_count}/{len(uploaded_files)})")

            except Exception as e:
                filename = future_to_file[future]
                st.error(f"❌ Error procesando {filename}: {str(e)}")
                completed_count += 1
                progress = completed_count / len(uploaded_files)
                progress_bar.progress(progress)
  
    # Guardar resultados en sesión
    st.session_state.results = results
    
    # Mostrar resumen
    progress_bar.empty()
    status_text.empty()
    
    # Calcular estadísticas
    total_files = len(results)
    successful_scraping = len([r for r in results if r.get('scraping_exitoso')])
    successful_pdf = len([r for r in results if r.get('extraccion_pdf_exitosa')])
    
    # Mostrar mensaje de éxito
    st.success(f"""
    ✅ **Procesamiento completado**
    
    📊 **Resumen:**
    - 📄 Total archivos: {total_files}
    - ✅ Scraping web exitoso: {successful_scraping}
    - 📄 Extracción PDF exitosa: {successful_pdf}
    - 📈 Tasa éxito general: {((successful_scraping + successful_pdf) / (total_files * 2) * 100):.1f}%
    """)
    
    # Mostrar detalles de errores si los hay
    errors = [r for r in results if r.get('error')]
    if errors:
        with st.expander("⚠️ Errores encontrados"):
            for error in errors:
                st.error(f"📄 {error['archivo_pdf']}: {error['error']}")

  # Footer de la aplicación
    create_footer(
        app_name="Scraper CSF SAT",
        description="Herramienta profesional para la extracción de datos de PDFs del SAT con optimización de rendimiento y múltiples estrategias de scraping web."
    )

if __name__ == "__main__":
    main() 