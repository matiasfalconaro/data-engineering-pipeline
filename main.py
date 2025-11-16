#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pipeline Datalakehouse para API de clima
Data Engineering - CEL UTN
Alumno: Matias Falconaro
"""

import logging

from src.core.config import PipelineConfig 
from src.core.clients import MinIOClient
from src.utils.file_ops import (save_to_delta_lake,
                                verify_delta_table,
                                show_bucket_tree)
from src.utils.quality import clean_weather_data
from src.layers.bronze import (WeatherAPIClient,
                               WeatherDataTransformer)
from src.layers.silver import (enrich_temporal_features,
                               create_weather_categories)
from src.layers.gold import create_city_daily_aggregates
from src.orchestation.flows import (extract_and_save_weather_data,
                                    refresh_city_metadata,
                                    process_weather_data)


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class WeatherDataPipeline:
    """
    Pipeline principal para datos climáticos.
    Encapsula toda la lógica de orquestación en una clase.
    """
    
    def __init__(self, config_file: str = "pipeline.conf"):
        self.config_manager = PipelineConfig(config_file)
        self.config = self.config_manager.data
        self.minio_client = MinIOClient(self.config)
        self.api_client = WeatherAPIClient(self.config)
        self.data_transformer = WeatherDataTransformer()
        
        logger.info("Pipeline de datos climáticos inicializado")
    
    def run(self, 
            enable_extraction: bool = True,
            enable_processing: bool = True, 
            enable_verification: bool = True,
            enable_metadata_refresh: bool = True) -> bool:
        """
        Ejecuta el pipeline completo de datos climáticos.
        """
        try:
            logger.info("INICIANDO PIPELINE DE DATOS CLIMÁTICOS")

            # Validar infraestructura
            if not self._validate_infrastructure():
                return False

            # Extracción de datos
            if enable_extraction:
                if not self._run_extraction():
                    return False

            # Metadatos
            if enable_metadata_refresh:
                if not self._run_metadata_refresh():
                    return False

            # Procesamiento
            if enable_processing:
                if not self._run_processing():
                    return False

            # Verificaciones
            if enable_verification:
                if not self._run_verification():
                    return False

            logger.info("PIPELINE COMPLETADO EXITOSAMENTE")
            return True

        except Exception as e:
            logger.error(f"ERROR CRÍTICO: {e}")
            return False
    
    def _validate_infrastructure(self) -> bool:
        """Valida la infraestructura necesaria."""
        logger.info("Validando infraestructura...")
        
        if not self.minio_client.test_connection():
            logger.error("Falló la conexión a MinIO")
            return False
            
        if not self.config_manager.validate_required_keys():
            logger.error("Configuración incompleta")
            return False
            
        logger.info("Infraestructura validada exitosamente")
        return True
    
    def _run_extraction(self) -> bool:
        """Ejecuta la extracción de datos climáticos."""
        logger.info("=== EXTRACCIÓN Y ALMACENAMIENTO DE DATOS CLIMÁTICOS (TP1) ===")
        
        success = extract_and_save_weather_data(
            self.config,
            self.api_client, 
            self.data_transformer,
            save_to_delta_lake
        )
        
        if not success:
            logger.error("Falló la extracción de datos climáticos")
            
        return success
    
    def _run_metadata_refresh(self) -> bool:
        """Ejecuta la actualización de metadatos."""
        logger.info("=== ACTUALIZACIÓN DE METADATOS (TP1) ===")
        
        success = refresh_city_metadata(
            self.config,
            self.api_client,
            self.data_transformer, 
            save_to_delta_lake
        )
        
        if not success:
            logger.error("Falló la actualización de metadatos")
            
        return success
    
    def _run_processing(self) -> bool:
        """Ejecuta el procesamiento y transformación."""
        logger.info("=== TRANSFORMACIÓN Y ENRIQUECIMIENTO (TP2) ===")
        
        success = process_weather_data(
            self.config,
            clean_weather_data,
            enrich_temporal_features, 
            create_weather_categories,
            create_city_daily_aggregates,
            save_to_delta_lake
        )
        
        if not success:
            logger.error("Falló el procesamiento TP2")
            
        return success
    
    def _run_verification(self) -> bool:
        """Ejecuta las verificaciones de datos."""
        logger.info("=== VERIFICACIÓN DE DATOS ===")

        verification_results = []

        # Verificar datos climáticos
        verification_results.append(
            verify_delta_table(
                self.config['temporal_data_path'], 
                "Datos Climáticos (Crudos)", 
                self.config['storage_options']
            )
        )
        print("-" * 60 + "\n")

        # Verificar metadatos
        verification_results.append(
            verify_delta_table(
                self.config['static_data_path'],
                "Metadatos de Ciudades", 
                self.config['storage_options']
            )
        )
        print("-" * 60 + "\n")

        # Verificar datos procesados
        processed_path = self.config.get('processed_data_path')
        if processed_path:
            verification_results.append(
                verify_delta_table(
                    processed_path,
                    "Datos Procesados (Agregados Diarios)", 
                    self.config['storage_options']
                )
            )
            print("-" * 60 + "\n")

        # Estructura de almacenamiento
        show_bucket_tree(self.minio_client)

        # Resumen
        success_count = sum(verification_results)
        total_checks = len(verification_results)
        logger.info(f"Verificaciones: {success_count}/{total_checks} exitosas")
        
        return success_count == total_checks


def main():
    """Función principal para ejecución del pipeline."""
    print("INICIANDO EJECUCIÓN DEL PIPELINE...")
    
    pipeline = WeatherDataPipeline("pipeline.conf")
    
    success = pipeline.run(
        enable_extraction=True,
        enable_processing=True, 
        enable_verification=True,
        enable_metadata_refresh=True
    )

    if success:
        print("PIPELINE COMPLETADO EXITOSAMENTE")
        return 0
    else:
        print("PIPELINE FALLÓ")
        return 1


if __name__ == "__main__":
    exit(main())
