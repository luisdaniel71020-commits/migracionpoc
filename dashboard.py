# dashboard.py
import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
from datetime import datetime

# --- Configuración de la página ---
st.set_page_config(
    page_title="Dashboard de Contrataciones",
    page_icon="👥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CSS personalizado ---
st.markdown("""
<style>
    .main > div {
        padding-top: 2rem;
    }
    .stSelectbox > div > div > div {
        background-color: #f0f2f6;
    }
    .metric-card {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 0.5rem 0;
    }
    .section-header {
        background: linear-gradient(90deg, #11998e 0%, #38ef7d 100%);
        color: white;
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
        text-align: center;
        font-size: 1.5rem;
        font-weight: bold;
    }
    .info-box {
        background: #f8f9ff;
        padding: 1rem;
        border-left: 4px solid #4f46e5;
        border-radius: 5px;
        margin: 1rem 0;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2px;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #f0f2f6;
        border-radius: 10px 10px 0 0;
        padding: 0.5rem 1rem;
        font-weight: bold;
    }
    .stTabs [aria-selected="true"] {
        background-color: #4f46e5;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

# --- Configuración de API ---
API_URL = os.getenv("API_URL", "http://api:5000")
API_KEY = os.getenv("API_KEY", "tu_api_key_aqui")
HEADERS = {"x-api-key": API_KEY}

# --- Funciones auxiliares ---
def get_available_years():
    """Obtiene los años disponibles desde los datos"""
    try:

        current_year = datetime.now().year
        return list(range(1993, current_year + 1))
    except:
        return list(range(1993, 2025))

@st.cache_data(ttl=300)
def get_hired_by_quarter(year):
    """Obtiene datos de contrataciones por trimestre"""
    url = f"{API_URL}/analytics/hired_by_quarter/{year}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        return pd.DataFrame(resp.json())
    except requests.exceptions.RequestException as e:
        st.error(f"Error de conexión: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error al obtener datos de contrataciones: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_departments_above_average(year):
    """Obtiene departamentos con contrataciones sobre el promedio"""
    url = f"{API_URL}/analytics/departments_above_average/{year}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        return pd.DataFrame(resp.json())
    except requests.exceptions.RequestException as e:
        st.error(f"Error de conexión: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error al obtener datos de departamentos: {e}")
        return pd.DataFrame()

def create_quarterly_chart(df, year):
    """Crea gráfico de contrataciones por trimestre"""
    if df.empty:
        return None
    
    # Preparar datos para visualización
    df_melt = df.melt(
        id_vars=["department", "job"],
        value_vars=["Q1", "Q2", "Q3", "Q4"],
        var_name="Trimestre",
        value_name="Contrataciones"
    )
    
    # Crear subplots para cada trimestre
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=["Primer Trimestre", "Segundo Trimestre", 
                       "Tercer Trimestre", "Cuarto Trimestre"],
        specs=[[{"secondary_y": False}, {"secondary_y": False}],
               [{"secondary_y": False}, {"secondary_y": False}]]
    )
    
    quarters = ["Q1", "Q2", "Q3", "Q4"]
    colors = px.colors.qualitative.Set3
    
    for i, quarter in enumerate(quarters):
        row = (i // 2) + 1
        col = (i % 2) + 1
        
        quarter_data = df_melt[df_melt["Trimestre"] == quarter]
        
        # Agrupar por departamento y sumar contrataciones
        dept_summary = quarter_data.groupby("department")["Contrataciones"].sum().sort_values(ascending=False)
        
        fig.add_trace(
            go.Bar(
                x=dept_summary.index,
                y=dept_summary.values,
                name=quarter,
                marker_color=colors[i % len(colors)],
                text=dept_summary.values,
                textposition='outside',
                showlegend=False
            ),
            row=row, col=col
        )
    
    fig.update_layout(
        title={
            'text': f"📊 Contrataciones por Departamento y Trimestre - {year}",
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 20, 'color': '#2c3e50'}
        },
        height=600,
        showlegend=False,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    
    fig.update_xaxes(tickangle=45)
    fig.update_yaxes(title_text="Número de Contrataciones")
    
    return fig

def create_departments_chart(df, year):
    """Crea gráfico de departamentos sobre promedio"""
    if df.empty:
        return None
    
    # Ordenar por contrataciones descendente
    df_sorted = df.sort_values('Hired', ascending=True)
    
    # Crear gráfico de barras horizontales
    fig = go.Figure(data=[
        go.Bar(
            x=df_sorted['Hired'],
            y=df_sorted['Department'],
            orientation='h',
            marker=dict(
                color=df_sorted['Hired'],
                colorscale='Viridis',
                colorbar=dict(title="Contrataciones")
            ),
            text=df_sorted['Hired'],
            textposition='outside',
            hovertemplate='<b>%{y}</b><br>Contrataciones: %{x}<br><extra></extra>'
        )
    ])
    
    fig.update_layout(
        title={
            'text': f"🎯 Departamentos con Contrataciones Sobre el Promedio - {year}",
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 18, 'color': '#2c3e50'}
        },
        xaxis_title="Número de Contrataciones",
        yaxis_title="Departamento",
        height=max(400, len(df_sorted) * 50),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=20, r=20, t=80, b=20)
    )
    
    return fig

def create_detailed_view(df):
    """Crea vista detallada por cargo y departamento"""
    if df.empty:
        return None
    
    # Crear heatmap
    df_pivot = df.pivot_table(
        index='department', 
        columns='job', 
        values=['Q1', 'Q2', 'Q3', 'Q4'], 
        aggfunc='sum', 
        fill_value=0
    )
    
    # Sumar todos los trimestres
    df_total = df_pivot.sum(axis=1, level=0).sum(axis=1)
    df_total = df_total.sort_values(ascending=False)
    
    fig = px.imshow(
        df_pivot.values,
        x=df_pivot.columns.get_level_values(1).unique(),
        y=df_pivot.index,
        color_continuous_scale='Blues',
        aspect='auto'
    )
    
    fig.update_layout(
        title="🔍 Mapa de Calor: Contrataciones por Cargo y Departamento",
        xaxis_title="Cargo",
        yaxis_title="Departamento",
        height=600
    )
    
    return fig

# --- Interface Principal ---
st.markdown("""
<div style='text-align: center; padding: 2rem 0;'>
    <h1 style='color: #2c3e50; font-size: 3rem; margin-bottom: 0;'>👥 Dashboard de Contrataciones</h1>
    <p style='color: #7f8c8d; font-size: 1.2rem;'>Análisis integral de contrataciones por departamento y trimestre</p>
</div>
""", unsafe_allow_html=True)

# --- Sidebar ---
with st.sidebar:
    st.markdown("""
    <div class='section-header'>
        ⚙️ Configuración
    </div>
    """, unsafe_allow_html=True)
    
    available_years = get_available_years()
    year = st.selectbox(
        "📅 Selecciona el año:",
        options=available_years,
        index=len(available_years) - 1,  # Último año por defecto
        help="Selecciona el año para analizar las contrataciones"
    )
    
    st.markdown("---")
    
    # Botón para actualizar datos
    if st.button("🔄 Actualizar Datos", type="primary"):
        st.cache_data.clear()
        st.rerun()
    
    st.markdown("""
    <div class='info-box'>
        <h4>ℹ️ Información</h4>
        <p>Este dashboard muestra:</p>
        <ul>
            <li>Contrataciones por trimestre</li>
            <li>Departamentos sobre promedio</li>
            <li>Análisis detallado por cargo</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)

# --- Contenido Principal ---
col1, col2, col3 = st.columns([1, 2, 1])
with col2:
    st.markdown(f"""
    <div class='metric-card'>
        <h2>📊 Análisis para el año {year}</h2>
        <p>Datos actualizados automáticamente</p>
    </div>
    """, unsafe_allow_html=True)

# Tabs para organizar contenido
tab1, tab2, tab3 = st.tabs(["📈 Por Trimestres", "🏆 Sobre Promedio", "🔍 Vista Detallada"])

with tab1:
    st.markdown("### 📊 Contrataciones por Trimestre")
    
    with st.spinner("Cargando datos de contrataciones por trimestre..."):
        df_quarters = get_hired_by_quarter(year)
    
    if not df_quarters.empty:
        # Métricas resumen
        total_hires = df_quarters[['Q1', 'Q2', 'Q3', 'Q4']].sum().sum()
        total_departments = df_quarters['department'].nunique()
        total_jobs = df_quarters['job'].nunique()
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Contrataciones", f"{total_hires:,}")
        with col2:
            st.metric("Departamentos", total_departments)
        with col3:
            st.metric("Tipos de Cargo", total_jobs)
        with col4:
            avg_per_quarter = total_hires / 4
            st.metric("Promedio por Trimestre", f"{avg_per_quarter:.1f}")
        
        # Gráfico principal
        fig1 = create_quarterly_chart(df_quarters, year)
        if fig1:
            st.plotly_chart(fig1, use_container_width=True)
        
        # Tabla detallada expandible
        with st.expander("📋 Ver datos detallados"):
            st.dataframe(
                df_quarters.style.highlight_max(axis=0, subset=['Q1', 'Q2', 'Q3', 'Q4']),
                use_container_width=True
            )
    else:
        st.warning(f"⚠️ No se encontraron datos para el año {year}")

with tab2:
    st.markdown("### 🏆 Departamentos con Contrataciones Sobre el Promedio")
    
    with st.spinner("Cargando datos de departamentos..."):
        df_avg = get_departments_above_average(year)
    
    if not df_avg.empty:
        # Métricas
        avg_hires = df_avg['Hired'].mean()
        top_dept = df_avg.loc[df_avg['Hired'].idxmax()]
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Departamentos Destacados", len(df_avg))
        with col2:
            st.metric("Promedio de Contrataciones", f"{avg_hires:.1f}")
        with col3:
            st.metric("Departamento Líder", f"{top_dept['Department']}", f"{top_dept['Hired']} contrataciones")
        
        # Gráfico
        fig2 = create_departments_chart(df_avg, year)
        if fig2:
            st.plotly_chart(fig2, use_container_width=True)
        
        # Tabla
        with st.expander("📊 Ranking completo"):
            df_display = df_avg.sort_values('Hired', ascending=False).reset_index(drop=True)
            df_display.index += 1
            st.dataframe(df_display, use_container_width=True)
    else:
        st.info(f"ℹ️ No hay departamentos por encima del promedio en {year}")

with tab3:
    st.markdown("### 🔍 Análisis Detallado")
    
    df_quarters_detail = get_hired_by_quarter(year)
    
    if not df_quarters_detail.empty:
        # Vista por departamento
        dept_selected = st.selectbox(
            "Selecciona un departamento:",
            options=["Todos"] + sorted(df_quarters_detail['department'].unique().tolist())
        )
        
        if dept_selected != "Todos":
            df_filtered = df_quarters_detail[df_quarters_detail['department'] == dept_selected]
        else:
            df_filtered = df_quarters_detail
        
        # Gráfico de tendencia trimestral
        df_trend = df_filtered[['Q1', 'Q2', 'Q3', 'Q4']].sum()
        
        fig_trend = go.Figure()
        fig_trend.add_trace(go.Scatter(
            x=['Q1', 'Q2', 'Q3', 'Q4'],
            y=df_trend.values,
            mode='lines+markers',
            line=dict(width=4, color='#3498db'),
            marker=dict(size=12, color='#e74c3c'),
            text=df_trend.values,
            textposition='top center'
        ))
        
        fig_trend.update_layout(
            title=f"📈 Tendencia Trimestral - {dept_selected}",
            xaxis_title="Trimestre",
            yaxis_title="Contrataciones",
            height=400
        )
        
        st.plotly_chart(fig_trend, use_container_width=True)
        
        # Top cargos
        if dept_selected == "Todos":
            top_jobs = df_quarters_detail.set_index('job')[['Q1', 'Q2', 'Q3', 'Q4']].sum(axis=1).sort_values(ascending=False).head(10)
        else:
            top_jobs = df_filtered.set_index('job')[['Q1', 'Q2', 'Q3', 'Q4']].sum(axis=1).sort_values(ascending=False)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 🏅 Top Cargos por Contrataciones")
            for i, (job, hires) in enumerate(top_jobs.head(5).items(), 1):
                st.markdown(f"**{i}.** {job}: **{hires}** contrataciones")
        
        with col2:
            st.markdown("#### 📊 Distribución por Cargo")
            fig_pie = px.pie(
                values=top_jobs.head(8).values,
                names=top_jobs.head(8).index,
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            fig_pie.update_layout(height=400)
            st.plotly_chart(fig_pie, use_container_width=True)

# --- Footer ---
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #7f8c8d; padding: 1rem 0;'>
    <p>Dashboard de Contrataciones | Desarrollado usando Streamlit y Plotly</p>
    <p style='font-size: 0.8rem;'>Datos actualizados automáticamente desde BigQuery</p>
</div>
""", unsafe_allow_html=True)