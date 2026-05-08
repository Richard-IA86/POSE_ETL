"""
conftest.py — planif_pose
Agrega src/normalizador/ al path para importar módulos directamente.
"""

import os
import sys

# Los módulos de normalizador se importan sin paquete (por convencion del
# proyecto), por lo que se agrega la carpeta al sys.path.
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "src", "normalizador"),
)
