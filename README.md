# Weather Data ETL Pipeline with PySpark

A production-ready ETL pipeline built with PySpark for processing weather data, featuring advanced analytics, data quality checks, and comprehensive monitoring.

## What This Project Does

This project is a **complete weather data processing system** that takes raw weather data and transforms it into clean, analysis-ready datasets. Here's what it does in simple terms:

### **Input**: Raw Weather Data
- Takes hourly weather measurements from CSV files
- Includes temperature, humidity, pressure, wind speed, and other weather metrics
- Handles data from multiple time periods (years of weather history)

### **Processing**: Smart Data Transformation
- **Cleans the data** by removing duplicates, invalid readings, and errors
- **Validates data quality** to ensure accuracy and completeness
- **Aggregates hourly data** into daily summaries (average, max, min temperatures)
- **Adds advanced features** like rolling averages, seasonal patterns, and trend analysis
- **Optimizes performance** for handling large datasets efficiently

### **Output**: Ready-to-Use Analytics Dataset
- **Daily weather summaries** with 4,000+ records
- **Time series features** for trend analysis and forecasting
- **Statistical metrics** for each weather variable
- **Seasonal indicators** and weekend/weekday patterns
- **Data in Parquet format** for fast analytics and machine learning

### **Perfect For**:
-  **Weather analysis** and climate research
-  **Data science projects** and machine learning
-  **Business intelligence** and reporting
-  **Learning PySpark** and big data processing
-  **Production ETL pipelines** for weather data

### **Key Benefits**:
-  **Handles large datasets** efficiently (96K+ records processed)
-  **Automatically cleans data** and removes invalid readings
-  **Adds advanced analytics** features automatically
-  **Production-ready** with Docker, testing, and monitoring
-  **Easy to use** with clear documentation and examples

##  Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Source   │───▶│   Extract       │───▶│   Transform     │
│   (CSV Files)   │    │   Module        │    │   Module        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Lake     │◀───│   Load          │◀───│   Data Quality  │
│   (Parquet)     │    │   Module        │    │   Validation    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │   Analytics     │
                       │   (Jupyter)     │
                       └─────────────────┘
```

##  Features

- **Scalable ETL Pipeline**: Built with PySpark for big data processing
- **Data Quality Checks**: Comprehensive validation and monitoring
- **Advanced Analytics**: Time-series analysis with window functions
- **Production Ready**: Docker containerization, CI/CD, and logging
- **Comprehensive Testing**: Unit tests with 90%+ coverage
- **Performance Optimization**: Partitioning, caching, and query optimization

##  Data Processing

The pipeline processes weather data with the following transformations:
- **Data Cleaning**: Duplicate removal, null handling, type conversion, invalid value filtering
- **Aggregation**: Daily temperature statistics (avg, max, min, std)
- **Time Series Analysis**: Rolling averages, seasonal patterns, lag features
- **Data Quality**: Schema validation, range checks, completeness metrics, pressure validation
- **Performance Optimization**: Proper window partitioning, strategic caching, query optimization

##  Tech Stack

- **Python 3.12+**
- **PySpark 3.5.0** - Distributed data processing
- **Pandas** - Data analysis and visualization
- **SciPy** - Scientific computing and statistical analysis
- **Matplotlib & Seaborn** - Data visualization
- **Plotly** - Interactive visualizations
- **Scikit-learn** - Machine learning capabilities
- **Docker** - Containerization
- **GitHub Actions** - CI/CD pipeline
- **pytest** - Testing framework
- **Black & Flake8** - Code formatting and linting

##  Prerequisites

- Python 3.12+
- Java 11+ (required for PySpark)
- Docker (optional, for containerized deployment)
- Git

##  Quick Start

### Local Development

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd weather-etl-pipeline
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

5. **Run the ETL pipeline**
   ```bash
   python dags/etl_pipeline.py
   ```

6. **Run tests**
   ```bash
   pytest tests/ -v
   ```

### Docker Deployment

1. **Build and run with Docker Compose**
   ```bash
   docker-compose up --build
   ```

2. **Run specific services**
   ```bash
   docker-compose up etl-pipeline
   ```

##  Project Structure

```
weather-etl-pipeline/
├── dags/                    # ETL orchestration
│   └── etl_pipeline.py
├── src/                     # Source code
│   ├── extract/            # Data extraction
│   ├── transform/          # Data transformation
│   ├── load/               # Data loading
│   ├── utils/              # Utilities
│   └── config.py           # Configuration
├── data/                   # Data storage
│   ├── raw/               # Raw data
│   └── processed/         # Processed data
├── notebooks/             # Jupyter notebooks
├── tests/                 # Test files
├── .github/workflows/     # CI/CD pipelines
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## 🔧 Configuration

The application uses environment-based configuration. Key settings:

- `RAW_PATH`: Path to raw data files
- `PROCESSED_DIR`: Output directory for processed data
- `LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR)
- `SPARK_MASTER`: Spark master URL

##  Performance Features

- **Data Partitioning**: Optimized partitioning strategy for large datasets
- **Window Operations**: Proper partitioning for time-series window functions
- **Caching**: Strategic caching of frequently accessed DataFrames
- **Broadcast Joins**: Efficient join operations for small lookup tables
- **Query Optimization**: Explain plans and performance monitoring
- **Data Quality**: Automated invalid data filtering and validation

##  Testing

Run the complete test suite:

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html

# Run specific test file
pytest tests/test_transform.py -v
```

## 📊 Analytics

The Jupyter notebook (`notebooks/exploratory_analysis.ipynb`) provides:
- Data exploration and visualization
- Statistical analysis and insights
- Time-series decomposition
- Correlation analysis
- Export-ready reports

##  Recent Improvements

### v1.2.0 - Performance & Data Quality Enhancements
- **Enhanced Data Quality**: Added pressure validation and invalid data filtering
- **Window Operation Optimization**: Proper partitioning for time-series functions
- **Schema Validation**: Fixed timestamp handling and type validation
- **Dependency Management**: Added scientific computing libraries (SciPy, Matplotlib, etc.)
- **Performance Monitoring**: Improved logging and performance tracking

### Key Features Added:
-  Automatic filtering of invalid pressure values (0.0 readings)
-  Optimized window operations with year/month partitioning
-  Enhanced data quality validation with detailed reporting
-  Scientific computing library integration
-  Improved error handling and logging

##  CI/CD

The project includes GitHub Actions workflows for:
- Automated testing on Python 3.12
- Code quality checks (Black, Flake8)
- Security scanning
- Docker image building

##  Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

##  License

This project is licensed under the MIT License
##  Author

**Andres Miller**
- LinkedIn: www.linkedin.com/in/andres-miller
- Email: andlincod@outlook.com

##  Acknowledgments

- Weather data provided by "https://www.kaggle.com/datasets/muthuj7/weather-dataset/data"
- PySpark community for excellent documentation
- Open source contributors

---

##  Troubleshooting: Common Errors and Solutions

This section outlines common issues you might encounter while setting up or running this project, along with step-by-step solutions to resolve them.

- **Error: 'Java not found' when initializing PySpark**  
  **Solution**: Ensure Java 8+ is installed and added to your PATH. Install it via your package manager (e.g., `sudo apt install default-jre` on Ubuntu) and verify with `java -version`.

- **Error: Module not found or import errors after installing dependencies**  
  **Solution**: Activate your virtual environment first (e.g., `source venv/bin/activate`), then run `pip install -r requirements.txt`. If issues persist, check for version conflicts in requirements.txt.

- **Error: Permission denied when writing files (e.g., .env.example)**  
  **Solution**: This can occur due to file system restrictions. Use `sudo` for elevated privileges or ensure the directory is writable. Alternatively, create the file manually or via a script, and add it to .gitignore if needed.

- **Error: Spark session failures due to configuration**  
  **Solution**: Check your .env file for SPARK_MASTER settings. If running locally, set it to 'local[*]'. Verify Spark installation and restart your environment.

- **Error: Data quality issues like invalid schemas or null values**  
  **Solution**: Run data validation explicitly via the ETL pipeline (e.g., `python dags/etl_pipeline.py`). Inspect logs in the specified LOG_FILE for details and adjust configurations in config.py.
