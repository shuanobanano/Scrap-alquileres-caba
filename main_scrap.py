from src.zonaprop import main_scrap_zonaprop

if __name__ == "__main__":
    main_scrap_zonaprop(
        type_operation="alquiler",
        type_building="departamentos",
        export_final_results=True
        )
    
    main_scrap_zonaprop(
        type_operation="venta",
        type_building="departamentos",
        export_final_results=True
        )
    
    main_scrap_zonaprop(
        type_operation="alquiler",
        type_building="locales-comerciales",
        export_final_results=True
        )
    
    main_scrap_zonaprop(
        type_operation="venta",
        type_building="locales-comerciales",
        export_final_results=True
        )
    
    
    main_scrap_zonaprop(
        type_operation="alquiler",
        type_building="oficinas-comerciales",
        export_final_results=True
        )
    
    main_scrap_zonaprop(
        type_operation="venta",
        type_building="oficinas-comerciales",
        export_final_results=True
        )