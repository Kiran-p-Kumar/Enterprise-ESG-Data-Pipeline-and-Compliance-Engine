# ==============================================================================
# PROJECT: Enterprise ESG (Environmental, Social, & Governance) Data Pipeline
# TOOL: Databricks (PySpark & Unity Catalog)
# ==============================================================================

from pyspark.sql import functions as F

# ---------------------------------------------------------
# STEP 1: READING THE 1 MILLION ROW RAW FILES FROM VOLUMES
# ---------------------------------------------------------
df_profile = spark.read.csv("/Volumes/workspace/default/esg_raw_data/raw_data_files/company_profiles.csv", header=True, inferSchema=True)
df_emissions = spark.read.csv("/Volumes/workspace/default/esg_raw_data/raw_data_files/emissions_raw.csv", header=True, inferSchema=True)
df_energy = spark.read.csv("/Volumes/workspace/default/esg_raw_data/raw_data_files/energy_usage_raw.csv", header=True, inferSchema=True)

print(f"Initial Company Profile Records: {df_profile.count()}")
print(f"Initial Emissions Records: {df_emissions.count()}")
print(f"Initial Energy Usage Records: {df_energy.count()}")


# ---------------------------------------------------------
# STEP 2: SILVER LAYER TRANSFORMATIONS (Data Cleaning)
# ---------------------------------------------------------

# 1. Cleaning Emissions Data: Handling nulls and converting 'Tons' to 'KG'
df_emissions_clean = df_emissions.dropna(subset=["Emissions_Value"]) \
    .withColumn("Emissions_in_KG", 
                F.when(F.col("Unit_Type") == "Tons", F.col("Emissions_Value") * 1000)
                .otherwise(F.col("Emissions_Value"))) \
    .drop("Emissions_Value", "Unit_Type")

# 2. Cleaning Energy Usage Data: Renaming columns and handling nulls
df_energy_clean = df_energy.dropna(subset=["power_consumed_mwh"]) \
    .withColumnRenamed("ID", "Company_ID") \
    .withColumnRenamed("power_consumed_mwh", "Power_MWh")


# ---------------------------------------------------------
# STEP 3: GOLD LAYER MERGE (Handling Duplicate Column Errors)
# ---------------------------------------------------------
df_esg_gold_final = df_profile.join(df_emissions_clean, df_profile.Company_ID == df_emissions_clean.cid, "inner") \
    .join(df_energy_clean, df_profile.Company_ID == df_energy_clean.Company_ID, "inner") \
    .select(
        df_profile["Company_ID"], 
        df_profile["Comp_Name"], 
        df_profile["Region"], 
        df_emissions_clean["Emissions_in_KG"], 
        df_energy_clean["Power_MWh"], 
        df_energy_clean["Year"]
    )

print(f"Total Rows after cleaning and joining: {df_esg_gold_final.count()}")


# ---------------------------------------------------------
# STEP 4: WRITING TO DELTA LAKE & CSV EXPORT
# ---------------------------------------------------------

# Saving as a Delta Table in Unity Catalog
df_esg_gold_final.write.format("delta").mode("overwrite").saveAsTable("workspace.default.esg_final_gold")

# Exporting the final 9.2 Lakh clean records to a CSV folder in Volumes for download
df_esg_gold_final.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save("/Volumes/workspace/default/esg_raw_data/final_output_csv")

print("Boom! 💥 Success! Final Cleaned Gold Layer data saved as Delta Table and CSV!")