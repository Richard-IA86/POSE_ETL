import pandas as pd
import os
from pathlib import Path
import logging

class FileReader:
    """Busca y lee los archivos crudos que se depositan en el Buzón Único (data/inbox)"""
    
    def __init__(self, inbox_dir: str):
        self.inbox_dir = Path(inbox_dir)
        if not self.inbox_dir.exists():
            os.makedirs(self.inbox_dir, exist_ok=True)
            
    def buscar_archivos(self, patron: str = "*.xlsx") -> list[Path]:
        """Devuelve todos los archivos en el inbox que coinciden con el patrón."""
        esperados = list(self.inbox_dir.glob(patron))
        logging.info(f"Se encontraron {len(esperados)} archivos con patrón '{patron}' en {self.inbox_dir}.")
        return esperados

    def leer_excel(self, ruta: Path, **kwargs) -> pd.DataFrame:
        """Lee un DataFrame básico de pandas."""
        logging.info(f"Leyendo: {ruta.name}")
        return pd.read_excel(ruta, **kwargs)
