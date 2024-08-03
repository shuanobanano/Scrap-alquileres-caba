import sys
import os

# Añade la ruta del directorio 'project_root' al sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.zonaprop import main_scrap_zonaprop

if __name__ == "__main__":
    main_scrap_zonaprop(
        type_operation="venta",
        type_building="departamentos",
        export_final_results=True
        )