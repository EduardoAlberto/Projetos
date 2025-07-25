# data_processing/batch_transformations.py
from pyspark.sql import DataFrame, functions as F
# No início do batch_transformations.py
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))  # Adiciona DTM/ ao path
from config import settings
from src.security.data_masking import DataMasker



class DataTransformer:
    def __init__(self):
        self.masker = DataMasker()
    
    def apply_batch_transformations(self, df: DataFrame) -> DataFrame:
        df = df.na.drop(subset=["patient_id", "timestamp"])
        df = df.withColumn("processing_time", F.current_timestamp())
        
        for field in settings.Config.SENSITIVE_FIELDS:
            if field in df.columns:
                df = self.masker.mask_data(df, field)
        
        return df
    
    def transform_healthcare_data(self, df: DataFrame) -> DataFrame:
        df = df.withColumn(
            "blood_pressure_array",
            F.split(F.regexp_replace("blood_pressure", r"\s+", ""), "/"))
        
        df = df.withColumn("sistolica", df["blood_pressure_array"].getItem(0).cast("integer"))
        df = df.withColumn("diastolica", df["blood_pressure_array"].getItem(1).cast("integer"))
        
        df = df.withColumn(
            "pressure_category",
            F.when(
                (df["sistolica"] < 90) | (df["diastolica"] < 60), "baixa"
            ).when(
                (df["sistolica"] >= 140) | (df["diastolica"] >= 90), "alta"
            ).otherwise("normal"))
        
        return df.drop("blood_pressure_array")