# Pipeline ETL de Datos Meteorológicos con PySpark

Un pipeline ETL listo para producción construido con PySpark para procesar datos meteorológicos, con análisis avanzados, verificaciones de calidad de datos y monitoreo integral.

## 🌤️ Qué Hace Este Proyecto

Este proyecto es un **sistema completo de procesamiento de datos meteorológicos** que toma datos meteorológicos en bruto y los transforma en conjuntos de datos limpios y listos para análisis. Aquí te explicamos qué hace en términos simples:

### **Entrada**: Datos Meteorológicos en Bruto
- Toma mediciones meteorológicas por hora desde archivos CSV
- Incluye temperatura, humedad, presión, velocidad del viento y otras métricas meteorológicas
- Maneja datos de múltiples períodos de tiempo (años de historial meteorológico)

### **Procesamiento**: Transformación Inteligente de Datos
- **Limpia los datos** eliminando duplicados, lecturas inválidas y errores
- **Valida la calidad de los datos** para asegurar precisión y completitud
- **Agrega datos por hora** en resúmenes diarios (temperatura promedio, máxima, mínima)
- **Añade características avanzadas** como promedios móviles, patrones estacionales y análisis de tendencias
- **Optimiza el rendimiento** para manejar grandes conjuntos de datos de manera eficiente

### **Salida**: Conjunto de Datos de Análisis Listo para Usar
- **Resúmenes meteorológicos diarios** con más de 4,000 registros
- **Características de series temporales** para análisis de tendencias y pronósticos
- **Métricas estadísticas** para cada variable meteorológica
- **Indicadores estacionales** y patrones de fin de semana/día laboral
- **Datos en formato Parquet** para análisis rápidos y aprendizaje automático

### **Perfecto Para**:
- 🌡️ **Análisis meteorológico** e investigación climática
- 📊 **Proyectos de ciencia de datos** y aprendizaje automático
- 📈 **Inteligencia de negocios** e informes
- 🎓 **Aprender PySpark** y procesamiento de big data
- 🏢 **Pipelines ETL de producción** para datos meteorológicos

### **Beneficios Clave**:
- ✅ **Maneja grandes conjuntos de datos** de manera eficiente (96K+ registros procesados)
- ✅ **Limpia datos automáticamente** y elimina lecturas inválidas
- ✅ **Añade análisis avanzados** automáticamente
- ✅ **Listo para producción** con Docker, pruebas y monitoreo
- ✅ **Fácil de usar** con documentación clara y ejemplos

## 🏗️ Arquitectura

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Fuente de     │───▶│   Módulo de     │───▶│   Módulo de     │
│   Datos         │    │   Extracción    │    │   Transformación│
│   (Archivos CSV)│    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Lake     │◀───│   Módulo de     │◀───│   Validación de │
│   (Parquet)     │    │   Carga         │    │   Calidad de    │
│                 │    │                 │    │   Datos         │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │   Análisis      │
                       │   (Jupyter)     │
                       └─────────────────┘
```

## 🚀 Características

- **Pipeline ETL Escalable**: Construido con PySpark para procesamiento de big data
- **Verificaciones de Calidad de Datos**: Validación y monitoreo integral
- **Análisis Avanzados**: Análisis de series temporales con funciones de ventana
- **Listo para Producción**: Containerización con Docker, CI/CD y logging
- **Pruebas Integrales**: Pruebas unitarias con cobertura del 90%+
- **Optimización de Rendimiento**: Particionado, caché y optimización de consultas

## 📊 Procesamiento de Datos

El pipeline procesa datos meteorológicos con las siguientes transformaciones:
- **Limpieza de Datos**: Eliminación de duplicados, manejo de nulos, conversión de tipos, filtrado de valores inválidos
- **Agregación**: Estadísticas diarias de temperatura (promedio, máximo, mínimo, desv. estándar)
- **Análisis de Series Temporales**: Promedios móviles, patrones estacionales, características de retraso
- **Calidad de Datos**: Validación de esquema, verificaciones de rango, métricas de completitud, validación de presión
- **Optimización de Rendimiento**: Particionado adecuado de ventanas, caché estratégico, optimización de consultas

## 🛠️ Stack Tecnológico

- **Python 3.12+**
- **PySpark 3.5.0** - Procesamiento distribuido de datos
- **Pandas** - Análisis y visualización de datos
- **SciPy** - Computación científica y análisis estadístico
- **Matplotlib & Seaborn** - Visualización de datos
- **Plotly** - Visualizaciones interactivas
- **Scikit-learn** - Capacidades de aprendizaje automático
- **Docker** - Containerización
- **GitHub Actions** - Pipeline CI/CD
- **pytest** - Framework de pruebas
- **Black & Flake8** - Formateo de código y linting

## 📋 Prerrequisitos

- Python 3.12+
- Java 8+ (requerido para PySpark)
- Docker (opcional, para despliegue containerizado)
- Git

## 🚀 Inicio Rápido

### Desarrollo Local

1. **Clonar el repositorio**
   ```bash
   git clone <repository-url>
   cd weather-etl-pipeline
   ```

2. **Crear entorno virtual**
   ```bash
   python -m venv venv
   source venv/bin/activate  # En Windows: venv\Scripts\activate
   ```

3. **Instalar dependencias**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configurar variables de entorno**
   ```bash
   cp .env.example .env
   # Editar .env con tu configuración
   ```

5. **Ejecutar el pipeline ETL**
   ```bash
   python dags/etl_pipeline.py
   ```

6. **Ejecutar pruebas**
   ```bash
   pytest tests/ -v
   ```

### Despliegue con Docker

1. **Construir y ejecutar con Docker Compose**
   ```bash
   docker-compose up --build
   ```

2. **Ejecutar servicios específicos**
   ```bash
   docker-compose up etl-pipeline
   ```

## 📁 Estructura del Proyecto

```
weather-etl-pipeline/
├── dags/                    # Orquestación ETL
│   └── etl_pipeline.py
├── src/                     # Código fuente
│   ├── extract/            # Extracción de datos
│   ├── transform/          # Transformación de datos
│   ├── load/               # Carga de datos
│   ├── utils/              # Utilidades
│   └── config.py           # Configuración
├── data/                   # Almacenamiento de datos
│   ├── raw/               # Datos en bruto
│   └── processed/         # Datos procesados
├── notebooks/             # Jupyter notebooks
├── tests/                 # Archivos de prueba
├── .github/workflows/     # Pipelines CI/CD
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## 🔧 Configuración

La aplicación utiliza configuración basada en variables de entorno. Configuraciones clave:

- `RAW_PATH`: Ruta a archivos de datos en bruto
- `PROCESSED_DIR`: Directorio de salida para datos procesados
- `LOG_LEVEL`: Nivel de logging (DEBUG, INFO, WARNING, ERROR)
- `SPARK_MASTER`: URL del master de Spark

## 📈 Características de Rendimiento

- **Particionado de Datos**: Estrategia de particionado optimizada para grandes conjuntos de datos
- **Operaciones de Ventana**: Particionado adecuado para funciones de ventana de series temporales
- **Caché**: Caché estratégico de DataFrames frecuentemente accedidos
- **Joins de Broadcast**: Operaciones de join eficientes para tablas de búsqueda pequeñas
- **Optimización de Consultas**: Planes de explicación y monitoreo de rendimiento
- **Calidad de Datos**: Filtrado automático de datos inválidos y validación

## 🧪 Pruebas

Ejecutar la suite completa de pruebas:

```bash
# Ejecutar todas las pruebas
pytest tests/ -v

# Ejecutar con cobertura
pytest tests/ --cov=src --cov-report=html

# Ejecutar archivo de prueba específico
pytest tests/test_transform.py -v
```

## 📊 Análisis

El notebook de Jupyter (`notebooks/exploratory_analysis.ipynb`) proporciona:
- Exploración y visualización de datos
- Análisis estadístico e insights
- Descomposición de series temporales
- Análisis de correlación
- Informes listos para exportar

## 🔄 Mejoras Recientes

### v1.2.0 - Mejoras de Rendimiento y Calidad de Datos
- **Calidad de Datos Mejorada**: Añadida validación de presión y filtrado de datos inválidos
- **Optimización de Operaciones de Ventana**: Particionado adecuado para funciones de series temporales
- **Validación de Esquema**: Corregido manejo de timestamps y validación de tipos
- **Gestión de Dependencias**: Añadidas librerías de computación científica (SciPy, Matplotlib, etc.)
- **Monitoreo de Rendimiento**: Mejorado logging y seguimiento de rendimiento

### Características Clave Añadidas:
- ✅ Filtrado automático de valores de presión inválidos (lecturas de 0.0)
- ✅ Operaciones de ventana optimizadas con particionado por año/mes
- ✅ Validación de calidad de datos mejorada con reportes detallados
- ✅ Integración de librerías de computación científica
- ✅ Manejo de errores y logging mejorado

## 🚀 CI/CD

El proyecto incluye workflows de GitHub Actions para:
- Pruebas automatizadas en Python 3.12
- Verificaciones de calidad de código (Black, Flake8)
- Escaneo de seguridad
- Construcción de imágenes Docker

## 📝 Contribuir

1. Fork el repositorio
2. Crear una rama de feature (`git checkout -b feature/amazing-feature`)
3. Commit tus cambios (`git commit -m 'Add amazing feature'`)
4. Push a la rama (`git push origin feature/amazing-feature`)
5. Abrir un Pull Request

## 📄 Licencia

Este proyecto está licenciado bajo la Licencia MIT - ver el archivo [LICENSE](LICENSE) para detalles.

## 👨‍💻 Autor

**Andres Miller**
- LinkedIn: www.linkedin.com/in/andres-miller
- Email: andlincod@outlook.com

## 🙏 Agradecimientos

- Datos meteorológicos proporcionados por [fuente de datos]
- Comunidad de PySpark por la excelente documentación
- Contribuidores de código abierto

---

**Construido con ❤️ para el equipo Luca TIC**
