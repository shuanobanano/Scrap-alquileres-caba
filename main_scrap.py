from src.zonaprop import main_scrap_zonaprop
import time

if __name__ == "__main__":
    inicio = time.time()
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
    
    final = time.time()
    tiempo_total = final - inicio
    # Convertir el tiempo a formato horas: minutos: segundos
    horas, resto = divmod(tiempo_total, 3600)
    minutos, segundos = divmod(resto, 60)
    
    # Guardar el tiempo de ejecución en un archivo .txt
    with open("tiempo_ejecucion.txt", "w") as file:
        file.write(f"Tiempo total de ejecución: {int(horas)}:{int(minutos)}:{int(segundos)}\n")