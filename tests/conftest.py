"""
conftest.py — POSE_ETL
Agrega ETL_BaseA2/src/ingesta/ al path para importar modulos
de ingesta directamente en tests que no usan paquete completo
(test_schema_contract, test_transformer, test_writer).
"""

import os
import sys

# Modulos de ingesta viven en ETL_BaseA2/src/ingesta/
sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "ETL_BaseA2",
        "src",
        "ingesta",
    ),
)
