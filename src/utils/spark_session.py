from pyspark.sql import SparkSession

def get_spark(app_name: str = "etl_pyspark_app", master: str = "local[*]"):
    """
    Crea y devuelve una Spark Session en modolo local.
    Ajusta configuraciones aquí si necesitas mas memoria o paquetes.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", "2") # Pruebas locales rapidas
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    return spark