# Databricks notebook source
# MAGIC %pip install -r ../../requirements.txt

# Task: [INS-201] - Limpeza, Deduplicação e Enriquecimento (Camada Silver)
from pyspark.sql.functions import col, when, to_date, current_timestamp

# 1. DEFINIÇÃO DOS AMBIENTES NO UNITY CATALOG
CATALOG = "insurance_platform"
SOURCE_SCHEMA = "bronze"
TARGET_SCHEMA = "silver"

# Garantir que o esquema silver existe antes de gravar
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{TARGET_SCHEMA}")

print("🚀 A iniciar o processamento da Camada Silver...")

# ====================================================
# ENTIDADE 1: CUSTOMERS (Limpeza e Deduplicação)
# ====================================================
df_bronze_cust = spark.read.table(f"{CATALOG}.{SOURCE_SCHEMA}.raw_customers")

df_silver_cust = df_bronze_cust \
    .dropDuplicates(["customer_id"]) \
    .filter((col("age") >= 18) & (col("age") <= 100)) \
    .withColumn("created_at", to_date(col("created_at"))) \
    .withColumn("silver_timestamp", current_timestamp())

df_silver_cust.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{TARGET_SCHEMA}.dim_customers")

print("✅ Tabela 'dim_customers' gravada na Silver.")

# ====================================================
# ENTIDADE 2: POLICIES (Limpeza e Padronização)
# ====================================================
df_bronze_pol = spark.read.table(f"{CATALOG}.{SOURCE_SCHEMA}.raw_policies")

df_silver_pol = df_bronze_pol \
    .dropDuplicates(["policy_id"]) \
    .filter(col("premium_amount") > 0) \
    .withColumn("start_date", to_date(col("start_date"))) \
    .withColumn("silver_timestamp", current_timestamp())

df_silver_pol.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{TARGET_SCHEMA}.fact_policies")

print("✅ Tabela 'fact_policies' gravada na Silver.")

# ====================================================
# ENTIDADE 3: CLAIMS (Data Quality Gates & Quarantine)
# ====================================================
# 1. Ler os dados brutos da camada Bronze
df_bronze_clm = spark.read.table(f"{CATALOG}.{SOURCE_SCHEMA}.raw_claims")

# 2. DATA QUALITY GATE: Adicionar uma coluna booleana que valida as regras de negócio
# Validamos: se o ID não é nulo, se o ID da apólice existe e se o valor do sinistro não é negativo
df_claims_validated = df_bronze_clm \
    .withColumn("is_record_valid", 
        (col("claim_id").isNotNull()) & 
        (col("policy_id").isNotNull()) & 
        (col("claim_amount") >= 0)
    )

# 3. ROTA DE QUARENTENA: Desviar dados corrompidos para auditoria de compliance
df_quarantine_claims = df_claims_validated.filter(col("is_record_valid") == False)

# Se existirem dados inválidos, gravamos numa tabela separada sem bloquear o pipeline
if df_quarantine_claims.count() > 0:
    # Criamos a tabela de quarentena na Silver se não existir
    df_quarantine_claims.drop("is_record_valid").write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(f"{CATALOG}.{TARGET_SCHEMA}.quarantine_claims")
    print(f"⚠️ Alerta de Governação: {df_quarantine_claims.count()} registos corrompidos enviados para a Quarentena.")

# 4. ROTA DE PRODUÇÃO: Avançar apenas com os dados 100% limpos
df_clean_claims = df_claims_validated.filter(col("is_record_valid") == True).drop("is_record_valid")

# 5. Aplicar as transformações e deduplicações de negócio habituais nos dados limpos
df_silver_clm = df_clean_claims \
    .dropDuplicates(["claim_id"]) \
    .withColumn("claim_date", to_date(col("claim_date"))) \
    .withColumn("is_high_value_risk", when(col("claim_amount") > 30000, 1).otherwise(0)) \
    .withColumn("silver_timestamp", current_timestamp())

# 6. Gravar na tabela oficial Fact para consumo da camada Gold e do Power BI
df_silver_clm.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{TARGET_SCHEMA}.fact_claims")

print("✅ Tabela 'fact_claims' validada e gravada com sucesso na Silver.")
