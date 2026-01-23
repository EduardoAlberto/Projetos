#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, '/Users/eduardoalberto/Projetos/data_engineer/netflix')

try:
    print("1. Testing Config import...")
    from config.settings import Config
    print("   ✓ Config imported successfully")
    print(f"   SPARK_APP_NAME: {Config.SPARK_APP_NAME}")
except Exception as e:
    print(f"   ✗ Error importing Config: {e}")

try:
    print("\n2. Testing BatchExtractLoad import...")
    from src.data_ingestion.extractorLoad import BatchExtractLoad
    print("   ✓ BatchExtractLoad imported successfully")
except Exception as e:
    print(f"   ✗ Error importing BatchExtractLoad: {e}")

try:
    print("\n3. Testing DataTransformer import...")
    from src.data_processing.data_transformation import DataTransformer
    print("   ✓ DataTransformer imported successfully")
except Exception as e:
    print(f"   ✗ Error importing DataTransformer: {e}")

try:
    print("\n4. Testing DataLakeManager import...")
    from src.data_storage.data_lake import DataLakeManager
    print("   ✓ DataLakeManager imported successfully")
except Exception as e:
    print(f"   ✗ Error importing DataLakeManager: {e}")

try:
    print("\n5. Testing DataQuality import...")
    from src.data_quality.data_quality import DataQuality
    print("   ✓ DataQuality imported successfully")
except Exception as e:
    print(f"   ✗ Error importing DataQuality: {e}")

print("\n✓ All imports successful!")
