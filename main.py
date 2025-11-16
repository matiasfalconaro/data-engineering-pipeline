#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pipeline Datalakehouse para API de clima
Data Engineering - CEL UTN
Alumno: Matias Falconaro
"""

import logging
import os
import pandas as pd
import time

from datetime import (datetime,
                      timezone)
from pathlib import Path
from treelib import Tree
from typing import (Dict,
                    List,
                    Optional,
                    Union)

from src.core.config import load_config
from src.core.clients import test_minio_connection
from src.utils.file_ops import (save_to_delta_lake, 
                                         verify_delta_table, 
                                         show_bucket_tree)
from src.utils.quality import clean_weather_data
from src.layers.bronze import (get_current_weather, get_city_metadata, 
                                       weather_to_dataframe, metadata_to_dataframe)
from src.layers.silver import (enrich_temporal_features,
                                        create_weather_categories)
from src.layers.gold import create_city_daily_aggregates
from src.orchestation.flows import (extract_and_save_weather_data, 
                                            refresh_city_metadata, 
                                            process_weather_data)


def setup_logging() -> logging.Logger:
    """Configura el sistema de logging para el pipeline."""

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S',
        force=True
    )

    logger = logging.getLogger(__name__)
    logger.info("Pipeline de datos climáticos iniciado")

    return logger


config = load_config()
logger = setup_logging()


def main(enable_extraction: bool = True,
        enable_processing: bool = True,
        enable_verification: bool = True,
        enable_metadata_refresh: bool = True) -> bool:
    """
    Función principal que orquesta el pipeline completo de datos climáticos.
    """
    try:
        logger.info("INICIANDO PIPELINE DE DATOS CLIMÁTICOS")

        if not test_minio_connection(config):
            logger.error("Falló la conexión a MinIO")
            return False

        if enable_extraction:
            logger.info("=== EXTRACCIÓN T ALMACENAMIENTO DE DATOS CLIMÁTICOS (TP1) ===")
            if not extract_and_save_weather_data(config,
                                                 weather_to_dataframe,
                                                 save_to_delta_lake):
                logger.error("Falló la extracción de datos climáticos")
                return False

        if enable_metadata_refresh:
            logger.info("=== ACTUALIZACIÓN DE METADATOS (TP1) ===")
            if not refresh_city_metadata(config,
                                         get_city_metadata,
                                         metadata_to_dataframe,
                                         save_to_delta_lake):
                logger.error("Falló la actualización de metadatos")
                return False

        if enable_processing:
            logger.info("=== TRANSFORMACIÓN Y ENRIQUECIMIENTO (TP2) ===")
            if not process_weather_data(config,
                                        clean_weather_data,
                                        enrich_temporal_features, 
                                        create_weather_categories,
                                        create_city_daily_aggregates,
                                        save_to_delta_lake):
                logger.error("Falló el procesamiento TP2")
                return False

        # VERIFICACIONES
        if enable_verification:
            logger.info("=== VERIFICACIÓN DE DATOS ===")

            verification_results = []

            verification_results.append(
                verify_delta_table(config['temporal_data_path'], "Datos Climáticos (Crudos)", config['storage_options'])
            )
            print("-" * 60 + "\n")

            verification_results.append(
                verify_delta_table(config['static_data_path'], "Metadatos de Ciudades", config['storage_options'])
            )
            print("-" * 60 + "\n")

            processed_path = config.get('processed_data_path')
            if processed_path and enable_processing:
                verification_results.append(
                    verify_delta_table(processed_path, "Datos Procesados (Agregados Diarios)", config['storage_options'])
                )
                print("-" * 60 + "\n")

            show_bucket_tree(config)

            # Resumen
            success_count = sum(verification_results)
            total_checks = len(verification_results)
            logger.info(f"Verificaciones: {success_count}/{total_checks} exitosas")

        logger.info("PIPELINE COMPLETADO EXITOSAMENTE")
        return True

    except Exception as e:
        logger.error(f"ERROR CRÍTICO: {e}")
        return False


if __name__ == "__main__":
    print("INICIANDO EJECUCIÓN DEL PIPELINE...")
    success = main(enable_extraction=False,
              enable_processing=True,
              enable_verification=True,
              enable_metadata_refresh=False)

    if success:
        print("PIPELINE COMPLETADO EXITOSAMENTE")
    else:
        print("PIPELINE FALLÓ")
