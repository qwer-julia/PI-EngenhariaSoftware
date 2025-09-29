#!/usr/bin/env python3
"""
ETL PySpark: Ingestão ENEM + Censo Escolar -> Parquet particionado (ano, codigo_municipio)
Autor: Julia Fonseca
Propósito: converter microdados do INEP (ENEM + Censo Escolar) em dataset analítico anon.
Uso: spark-submit --master yarn --deploy-mode cluster etl_enem_censo_pyspark.py --enem-path s3://raw/enem/2023/ --censo-path s3://raw/censo/2023/ --output-path s3://processed/education/ --year 2023 --municipio 3524902 --salt YOUR_SECRET_SALT
"""

import argparse
import hashlib
import logging
import sys
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, LongType
)

# ---------------------------
# Configuração de logging
# ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("ETL_ENEM_CENSO")

# ---------------------------
# Funções utilitárias
# ---------------------------
def create_spark(app_name: str, extra_conf: dict = None) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    # Tuning básico para arquivos grandes; ajustar conforme cluster
    builder = builder.config("spark.sql.shuffle.partitions", "200")
    builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "true")
    builder = builder.config("spark.driver.memory", "8g")
    builder = builder.config("spark.executor.memory", "8g")
    if extra_conf:
        for k, v in extra_conf.items():
            builder = builder.config(k, v)
    spark = builder.getOrCreate()
    logger.info("SparkSession criada")
    return spark

def sha256_hash_udf(salt: str):
    def _hash(value: str) -> str:
        if value is None:
            return None
        h = hashlib.sha256()
        raw = (salt + str(value)).encode("utf-8")
        h.update(raw)
        return h.hexdigest()
    from pyspark.sql.types import StringType
    return F.udf(_hash, StringType())

def safe_select(df: DataFrame, cols: List[str]) -> DataFrame:
    existing = [c for c in cols if c in df.columns]
    return df.select(*existing)

# ---------------------------
# Schemas
# ---------------------------
ENEM_SCHEMA = StructType([
    StructField("NU_INSCRICAO", StringType(), True),
    StructField("CO_MUNICIPIO_PROVA", StringType(), True),   
    StructField("NO_MUNICIPIO_PROVA", StringType(), True),
    StructField("SG_UF_PROVA", StringType(), True),
    StructField("TP_SEXO", IntegerType(), True),
    StructField("NU_IDADE", IntegerType(), True),
    StructField("NU_NOTA_MT", DoubleType(), True),
    StructField("NU_NOTA_CN", DoubleType(), True),
    StructField("NU_NOTA_CH", DoubleType(), True),
    StructField("NU_NOTA_LC", DoubleType(), True),
    StructField("NU_NOTA_REDACAO", DoubleType(), True),
    StructField("TP_ST_CONCLUSAO", IntegerType(), True),
    StructField("CO_ESCOLA", StringType(), True),
])

CENSO_SCHEMA = StructType([
    StructField("CO_ESCOLA", StringType(), True),
    StructField("NU_CEP", StringType(), True),
    StructField("NO_MUNICIPIO", StringType(), True),
    StructField("CO_MUNICIPIO", StringType(), True),
    StructField("SG_UF", StringType(), True),
    StructField("LABORATORIO_CIENCIAS", StringType(), True),
    StructField("BIBLIOTECA", StringType(), True),
    StructField("INTERNET", StringType(), True),
    StructField("QT_MATRICULAS", IntegerType(), True),
    StructField("TP_DEPENDENCIA", StringType(), True),  
])

# ---------------------------
# Leitura dos microdados
# ---------------------------
def read_large_csv(spark: SparkSession, path: str, schema=None, header=True, inferSchema=False) -> DataFrame:
    """
    Leitura robusta de CSVs muito grandes.
    path pode ser um prefixo como s3://bucket/enem/2023/*.csv
    """
    logger.info(f"Lendo CSVs de: {path}")
    reader = spark.read.option("header", "true").option("escape", '"').option("multiLine", "false")
    if schema:
        reader = reader.schema(schema)
    if inferSchema:
        reader = reader.option("inferSchema", "true")
    df = reader.csv(path)
    logger.info(f"Registros lidos: {df.count() if df.rdd.getNumPartitions() < 10 else 'count skipped for perf'} (partitions={df.rdd.getNumPartitions()})")
    return df

# ---------------------------
# Transformações específicas
# ---------------------------
def normalize_enem(df: DataFrame, municipio_code: str, year: int) -> DataFrame:
    logger.info("Normalizando ENEM")
    mun_cols = [c for c in ("CO_MUNICIPIO_INSCRICAO", "CO_MUNICIPIO_PROVA", "CO_MUNICIPIO") if c in df.columns]
    mun_col = mun_cols[0] if mun_cols else None
    if mun_col is None:
        raise ValueError("Coluna de código do município não encontrada no ENEM — verifique dicionário")
    nota_map = {
        "NU_NOTA_MT": "nota_mt",
        "NU_NOTA_CN": "nota_cn",
        "NU_NOTA_CH": "nota_ch",
        "NU_NOTA_LC": "nota_lc",
        "NU_NOTA_REDACAO": "nota_redacao"
    }
    select_cols = [mun_col, "NU_INSCRICAO", "CO_ESCOLA"] + [c for c in nota_map.keys() if c in df.columns] + ["TP_SEXO", "NU_IDADE"]
    enem = safe_select(df, select_cols)
    for src, tgt in nota_map.items():
        if src in enem.columns:
            enem = enem.withColumnRenamed(src, tgt)
    enem = enem.withColumnRenamed(mun_col, "codigo_municipio_inscricao")
    enem = enem.filter(F.col("codigo_municipio_inscricao") == municipio_code)
    for c in ["nota_mt", "nota_cn", "nota_ch", "nota_lc", "nota_redacao"]:
        if c in enem.columns:
            enem = enem.withColumn(c, F.col(c).cast("double"))
    enem = enem.withColumn("ano", F.lit(year).cast(IntegerType()))
    logger.info("ENEM normalizado")
    return enem

def normalize_censo(df: DataFrame, municipio_code: str, year: int) -> DataFrame:
    logger.info("Normalizando Censo Escolar")
    mun_cols = [c for c in ("CO_MUNICIPIO", "CO_MUNICIPIO_ESCOLA", "COD_MUNICIPIO") if c in df.columns]
    mun_col = mun_cols[0] if mun_cols else None
    if mun_col is None:
        raise ValueError("Coluna de código do município não encontrada no Censo — verifique dicionário")
    censo = df.filter(F.col(mun_col) == municipio_code)
    infra_map = {
        "LABORATORIO_CIENCIAS": "has_lab",
        "BIBLIOTECA": "has_library",
        "INTERNET": "has_internet"
    }
    for src, tgt in infra_map.items():
        if src in censo.columns:
            censo = censo.withColumn(tgt, F.when(F.col(src).isin("1", "SIM", "S", 1, "True", "true"), 1).otherwise(0))
        else:
            censo = censo.withColumn(tgt, F.lit(None).cast(IntegerType()))
    if "CO_ESCOLA" not in censo.columns and "CO_ENTIDADE" in censo.columns:
        censo = censo.withColumnRenamed("CO_ENTIDADE", "CO_ESCOLA")
    censo = censo.withColumnRenamed(mun_col, "codigo_municipio")
    censo = censo.withColumn("ano", F.lit(year).cast(IntegerType()))
    logger.info("Censo normalizado")
    return censo

# ---------------------------
# Anonimização e derivação de features
# ---------------------------
def anonymize_and_derive(enem_df: DataFrame, censo_df: DataFrame, salt: str) -> DataFrame:
    logger.info("Anonimizando e derivando features")
    h_udf = sha256_hash_udf(salt)

    if "NU_INSCRICAO" in enem_df.columns:
        enem_df = enem_df.withColumn("aluno_anon_id", h_udf(F.col("NU_INSCRICAO")))
    else:
        enem_df = enem_df.withColumn("aluno_anon_id", h_udf(F.concat_ws("_", *[c for c in ["TP_SEXO", "NU_IDADE"] if c in enem_df.columns])))

  if "CO_ESCOLA" in enem_df.columns:
        agg = enem_df.groupBy("CO_ESCOLA", "ano").agg(
            F.avg("nota_mt").alias("avg_mt"),
            F.avg("nota_cn").alias("avg_cn"),
            F.avg("nota_ch").alias("avg_ch"),
            F.avg("nota_lc").alias("avg_lc"),
            F.avg("nota_redacao").alias("avg_redacao"),
            F.count("*").alias("count_inscritos")
        )
        if "CO_ESCOLA" in censo_df.columns:
            joined = censo_df.join(agg, on=[F.col("CO_ESCOLA") == F.col("CO_ESCOLA")], how="left")
        else:
            joined = censo_df.join(agg, on="CO_ESCOLA", how="left")
    else:
        agg = enem_df.groupBy("codigo_municipio_inscricao", "ano").agg(
            F.avg("nota_mt").alias("avg_mt"),
            F.avg("nota_cn").alias("avg_cn"),
            F.avg("nota_ch").alias("avg_ch"),
            F.avg("nota_lc").alias("avg_lc"),
            F.avg("nota_redacao").alias("avg_redacao"),
            F.count("*").alias("count_inscritos")
        )
        joined = censo_df.join(agg, censo_df.codigo_municipio == agg.codigo_municipio_inscricao, how="left")

    out_cols = [
        "CO_ESCOLA", "codigo_municipio", "ano",
        "has_lab", "has_library", "has_internet",
        "avg_mt", "avg_cn", "avg_ch", "avg_lc", "avg_redacao",
        "count_inscritos"
    ]
    out_cols_existing = [c for c in out_cols if c in joined.columns]
    result = joined.select(*out_cols_existing)
    infra_cols = [c for c in ["has_lab", "has_library", "has_internet"] if c in result.columns]
    if infra_cols:
        result = result.withColumn("infra_index", sum([F.coalesce(F.col(c).cast("int"), F.lit(0)) for c in infra_cols]))
    logger.info("Anonimização e derivação concluidas")
    return result

# ---------------------------
# Escrita em Parquet particionado
# ---------------------------
def write_parquet(df: DataFrame, output_path: str, partition_cols: List[str], mode: str = "overwrite"):
    logger.info(f"Escrevendo Parquet em {output_path} particionado por {partition_cols}")
    df.write.mode(mode).option("compression", "snappy").partitionBy(*partition_cols).parquet(output_path)
    logger.info("Escrita concluída")

# ---------------------------
# Fluxo principal
# ---------------------------
def main(args):
    spark = create_spark("ETL_ENEM_CENSO")

    enem_df = read_large_csv(spark, args.enem_path, schema=None, header=True, inferSchema=False)
    censo_df = read_large_csv(spark, args.censo_path, schema=None, header=True, inferSchema=False)

    enem_norm = normalize_enem(enem_df, args.municipio, args.year)
    censo_norm = normalize_censo(censo_df, args.municipio, args.year)

    processed = anonymize_and_derive(enem_norm, censo_norm, args.salt)

    partition_cols = []
    if "ano" in processed.columns:
        partition_cols.append("ano")
    if "codigo_municipio" in processed.columns:
        partition_cols.append("codigo_municipio")

    if not partition_cols:
        raise RuntimeError("Nenhuma coluna para particionamento encontrada. Ajuste mapeamento de colunas.")

    write_parquet(processed, args.output_path, partition_cols, mode="overwrite")

    logger.info("ETL finalizado com sucesso")
    spark.stop()

# ---------------------------
# CLI
# ---------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL ENEM + Censo -> Parquet particionado")
    parser.add_argument("--enem-path", required=True, help="Prefixo/pasta com arquivos ENEM (ex: s3://bucket/enem/2023/)")
    parser.add_argument("--censo-path", required=True, help="Prefixo/pasta com arquivos Censo Escolar (ex: s3://bucket/censo/2023/)")
    parser.add_argument("--output-path", required=True, help="Local de saída para Parquet (ex: s3://processed/education/)")
    parser.add_argument("--year", required=True, type=int, help="Ano dos microdados (ex: 2023)")
    parser.add_argument("--municipio", required=True, help="Código do município (IBGE), ex: 3524902")
    parser.add_argument("--salt", required=True, help="Salt secreto para hashing (guardar no KeyVault/Secrets Manager)")
    args = parser.parse_args()
    main(args)
