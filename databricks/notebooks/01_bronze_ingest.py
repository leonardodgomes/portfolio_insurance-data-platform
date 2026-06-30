# Databricks notebook source
# MAGIC %pip install -r ../../requirements.txt

# Task: [INS-103] - Ingestão de Dados Brutos para Tabelas Delta Lake Bronze
from pyspark.sql.functions import current_timestamp  # Removido input_file_name

# 1. DEFINIÇÃO DA INFRAESTRUTURA DO UNITY CATALOG
VOLUME_PATH = "/Volumes/insurance_platform/bronze/insurance_landing"
TARGET_CATALOG = "insurance_platform"
TARGET_SCHEMA = "bronze"

print(f"🚀 A iniciar ingestão do Volume: {VOLUME_PATH}")

def ingest_raw_csv_to_delta(entity_name):
    """
    Lê o ficheiro CSV do volume do Unity Catalog, anexa metadados corporativos
    e grava como uma tabela Delta Lake no esquema Bronze.
    """
    source_file = f"{VOLUME_PATH}/{entity_name}.csv"
    target_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.raw_{entity_name}"
    
    print(f"⏳ A processar entidade: {entity_name}...")
    
    try:
        # Ler o CSV brutos mantendo a tipagem original via inferência
        df_raw = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(source_file)
        
        # CORREÇÃO CRÍTICA PARA UNITY CATALOG:
        # Substituição de input_file_name() pela coluna nativa _metadata.file_path
        df_bronze = df_raw \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file_path", df_raw["_metadata.file_path"])
        
        # Gravar no formato Delta Lake
        df_bronze.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(target_table)
            
        print(f"✅ Sucesso! Tabela '{target_table}' criada/atualizada.")
        
    except Exception as e:
        print(f"❌ Erro crítico ao processar {entity_name}: {str(e)}")
        raise e

# 2. ORQUESTRAR A INGESTÃO DAS TRÊS TABELAS DE SEGUROS
ingest_raw_csv_to_delta("customers")
ingest_raw_csv_to_delta("policies")
ingest_raw_csv_to_delta("claims")
