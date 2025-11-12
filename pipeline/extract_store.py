#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pipeline Datalakehouse para API de clima
Data Engineering - CEL UTN
Alumno: Matias Falconaro
"""

# ============================================================================
# IMPORTS
# ============================================================================

import boto3
import configparser
import json
import logging
import os
import pandas as pd
import pyarrow as pa
import requests
import time

from botocore.exceptions import ClientError
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError
from datetime import datetime, timezone
from pathlib import Path
from treelib import Tree
from typing import Dict, List, Optional, Union


# ============================================================================
# CONFIGURACIÓN
# ============================================================================

def load_config(config_file="pipeline.conf"):
    """Carga configuración desde el archivo pipeline.conf"""
    if not Path(config_file).exists():
        raise FileNotFoundError(f"Configuration file '{config_file}' not found")

    config = configparser.ConfigParser()
    config.read(config_file)

    minio = {k: config.get('minio', k) for k in
             ['endpoint_url', 'access_key', 'secret_key', 'region', 'bucket_name']}

    base = f"s3://{minio['bucket_name']}/{config.get('data_lake', 'base_path')}"

    return {
        'api_key': config.get('api', 'api_key'),
        'base_url': config.get('api', 'base_url'),
        'api_timeout': config.getint('api', 'api_timeout'),
        'max_retries': config.getint('api', 'max_retries'),
        'cities': [c.strip() for c in config.get('api', 'cities').split(',')],
        'minio_config': minio,
        'storage_options': {
            "AWS_ENDPOINT_URL": minio["endpoint_url"],
            "AWS_ACCESS_KEY_ID": minio["access_key"],
            "AWS_SECRET_ACCESS_KEY": minio["secret_key"],
            "AWS_REGION": minio["region"],
            "AWS_ALLOW_HTTP": "true"
        },
        'data_lake_base': base,
        'temporal_data_path': f"{base}/{config.get('data_lake', 'temporal_path')}",
        'static_data_path': f"{base}/{config.get('data_lake', 'static_path')}"
    }


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


# ============================================================================
# API CLIENT
# ============================================================================

def make_api_request(endpoint: str, params: Dict) -> Optional[Dict]:
    """
    Realiza peticiones a la API con manejo de errores y reintentos automáticos.
    """
    for attempt in range(config['max_retries']):
        try:
            response = requests.get(endpoint, params=params, timeout=config['api_timeout'])
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            if attempt == config['max_retries'] - 1:
                logger.error(f"Timeout al conectar con {endpoint} después de {config['max_retries']} intentos")
                return None
            time.sleep(2 ** attempt)
        except requests.exceptions.RequestException as e:
            if attempt == config['max_retries'] - 1:
                logger.error(f"Error en petición a {endpoint}: {e}")
                return None
            time.sleep(2 ** attempt)

    return None


def get_current_weather(city: str) -> Optional[Dict]:
    """
    Extrae datos del clima actual para una ciudad específica.
    """
    endpoint = f"{config['base_url']}/weather"
    params = {
        "q": city,
        "appid": config['api_key'],
        "units": "metric",
        "lang": "es"
    }

    logger.info(f"Extrayendo datos climáticos para {city}")

    try:
        data = make_api_request(endpoint, params)

        if data and data.get("cod") == 200:
            # Agrego timestamp de extracción
            data['extraction_timestamp'] = datetime.now(timezone.utc).isoformat()
            data['extraction_city'] = city
            return data
        elif data:
            logger.warning(f"API retornó código {data.get('cod')} para {city}: {data.get('message', 'Sin mensaje')}")
            return None
        else:
            logger.warning(f"No se recibió respuesta para {city}")
            return None

    except Exception as e:
        logger.error(f"Error para {city}: {e}")
        return None


def get_city_metadata(city_list: List[str]) -> List[Dict]:
    """
    Genera metadatos de la ciudad a partir de la respuesta de la API.
    Sirve como datos estaticos/referencia.
    """
    metadata = []

    for city in city_list:
        endpoint = f"{config['base_url']}/weather"
        params = {
            "q": city,
            "appid": config['api_key']
        }

        logger.info(f"Extrayendo metadatos para {city}")
        data = make_api_request(endpoint, params)

        if data:
            # Metadatos de ciudades
            metadata.append({
                "city_id": data.get("id"),
                "city_name": data.get("name"),
                "country": data.get("sys", {}).get("country"),
                "latitude": data.get("coord", {}).get("lat"),
                "longitude": data.get("coord", {}).get("lon"),
                "timezone_offset": data.get("timezone"),
                "last_updated": datetime.now(timezone.utc).isoformat()
            })

    return metadata


# ============================================================================
# TRANSFORMACIÓN DE DATOS
# ============================================================================

def weather_to_dataframe(weather_data: Union[Dict, List[Dict]]) -> pd.DataFrame:
    """
    Convierte respuestas crudas del clima de la API a DataFrame.
    """
    if isinstance(weather_data, dict):
        weather_data = [weather_data]

    records = []

    for data in weather_data:
        if data is None:
            continue

        # Tiempo de observación del clima (cuando se registró el dato)
        observation_dt = datetime.fromtimestamp(
            data.get("dt", 0), tz=timezone.utc
        )

        record = {
            # Identificadores
            "city_id": data.get("id"),
            "city_name": data.get("name"),
            "country": data.get("sys", {}).get("country"),

            # Condiciones climáticas
            "weather_main": data.get("weather", [{}])[0].get("main"),
            "weather_description": data.get("weather", [{}])[0].get("description"),

            # Datos de temperatura
            "temperature": data.get("main", {}).get("temp"),
            "feels_like": data.get("main", {}).get("feels_like"),
            "temp_min": data.get("main", {}).get("temp_min"),
            "temp_max": data.get("main", {}).get("temp_max"),

            # Otras métricas
            "pressure": data.get("main", {}).get("pressure"),
            "humidity": data.get("main", {}).get("humidity"),
            "visibility": data.get("visibility"),
            "wind_speed": data.get("wind", {}).get("speed"),
            "wind_direction": data.get("wind", {}).get("deg"),
            "cloudiness": data.get("clouds", {}).get("all"),

            # Marcas de tiempo
            "observation_time": datetime.fromtimestamp(
                data.get("dt", 0), tz=timezone.utc
            ).isoformat(),
            "extraction_timestamp": data.get("extraction_timestamp"),

            # Columnas de particionamiento
            "date": observation_dt.strftime("%Y-%m-%d"),
            "hour": observation_dt.strftime("%H")
        }

        records.append(record)

    df = pd.DataFrame(records)
    logger.info(f"DataFrame creado con {len(df)} registros")

    return df


def metadata_to_dataframe(metadata: List[Dict]) -> pd.DataFrame:
    """
    Convierte los metadatos de la ciudad a Dataframe.
    """
    df = pd.DataFrame(metadata)
    logger.info(f"DataFrame de metadatos creado con {len(df)} registros")

    return df


# ============================================================================
# INFRAESTRUCTURA
# ============================================================================

def test_minio_connection():
    """
    Prueba la conexión a MinIO
    """
    logger.info("Probando conexión a MinIO")

    try:
        logger.info(f"Conectando a: {config['minio_config']['endpoint_url']}")

        s3_client = boto3.client(
            's3',
            endpoint_url=config['minio_config']["endpoint_url"],
            aws_access_key_id=config['minio_config']["access_key"],
            aws_secret_access_key=config['minio_config']["secret_key"],
            region_name=config['minio_config']["region"]
        )

        # Listo buckets
        response = s3_client.list_buckets()
        buckets = [b['Name'] for b in response['Buckets']]

        logger.info("Conexión a MinIO exitosa")
        logger.info(f"Número de buckets: {len(buckets)}")
        logger.info(f"Buckets disponibles: {buckets}")
        return True

    except Exception as e:
        logger.error(f"Error conectando a MinIO: {e}")
        logger.error("Verificar: URL, credenciales, y que MinIO no este caido")
        return False


# ============================================================================
# DATA LAKE
# ============================================================================

def save_to_delta_lake(df: pd.DataFrame,
                      base_path: str,
                      partition_cols: Optional[List[str]] = None,
                      mode: str = "append",
                      merge_predicate: Optional[str] = None) -> None:
    """
    Guarda un DataFrame en formato Delta Lake en MinIO/S3.

    Args:
        df: DataFrame a guardar
        base_path: Ruta en S3/MinIO donde se guardará la tabla
        partition_cols: Columnas por las cuales particionar (opcional)
        mode: Modo de escritura - 'append', 'overwrite' o 'merge' (default: 'append')
        merge_predicate: Condición SQL para merge (requerido solo si mode='merge')

    Modos soportados:
        - 'append': Agrega nuevos registros sin verificar duplicados
        - 'overwrite': Reemplaza todos los datos existentes
        - 'merge': Actualiza registros existentes e inserta nuevos (upsert)

    Aplica constraints de integridad automáticamente en la primera creación.
    """
    try:
        logger.info(f"Guardando {len(df)} registros en {base_path} (modo: {mode})")

        # Convierto a PyArrow
        pa_table = pa.Table.from_pandas(df)

        if mode == "merge":
            if not merge_predicate:
                raise ValueError("merge_predicate es requerido para mode='merge'")

            try:
                dt = DeltaTable(base_path, storage_options=config['storage_options'])
                dt.merge(
                    source=df,
                    predicate=merge_predicate,
                    source_alias="source",
                    target_alias="target"
                ).when_matched_update_all().when_not_matched_insert_all().execute()
                logger.info(f"MERGE exitoso: {len(df)} registros procesados")

            except TableNotFoundError:
                logger.info("Tabla no existe, creando inicialmente")
                write_deltalake(
                    base_path,
                    pa_table,
                    mode="overwrite",
                    partition_by=partition_cols,
                    storage_options=config['storage_options']
                )
                dt = DeltaTable(base_path, storage_options=config['storage_options'])
                _apply_constraints(dt, base_path)
                logger.info(f"Tabla creada: {len(df)} registros")

        elif mode in ["append", "overwrite"]:
            write_deltalake(
                base_path,
                pa_table,
                mode=mode,
                partition_by=partition_cols,
                storage_options=config['storage_options']
            )
            logger.info(f"{mode.upper()} exitoso: {len(df)} registros")

            # Aplico constraints solo en overwrite (primera creación)
            if mode == "overwrite":
                dt = DeltaTable(base_path, storage_options=config['storage_options'])
                _apply_constraints(dt, base_path)

        else:
            raise ValueError(f"Modo no soportado: {mode}. Usar: append, overwrite o merge")

    except Exception as e:
        logger.error(f"Error escribiendo en Delta Lake ({base_path}): {e}")
        raise


def _apply_constraints(dt: DeltaTable, base_path: str) -> None:
    """
    Aplica constraints de integridad de forma individual.
    """
    constraints = {}

    if "temporal" in base_path:
        constraints = {
            "pk_not_null": "city_id IS NOT NULL AND date IS NOT NULL AND hour IS NOT NULL",
            "valid_humidity_range": "humidity >= 0 AND humidity <= 100"
        }

    elif "metadata" in base_path:
        constraints = {
            "city_id_not_null": "city_id IS NOT NULL",
            "valid_coordinates": "latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180"
        }

    # Aplico cada constraint individualmente
    for constraint_name, constraint_expr in constraints.items():
        try:
            dt.alter.add_constraint({constraint_name: constraint_expr})
            logger.info(f"Constraint '{constraint_name}' aplicado exitosamente")
        except Exception as e:
            logger.warning(f"Error aplicando constraint '{constraint_name}': {e}")


# ============================================================================
# PIPELINES DE DATOS
# ============================================================================

def extract_and_save_weather_data() -> bool:
    """Extrae datos climáticos actuales y los guarda en Delta Lake."""

    weather_data = []
    for city in config['cities']:
        data = get_current_weather(city)
        if data:
            weather_data.append(data)

    if weather_data:
        df_weather = weather_to_dataframe(weather_data)

        # Validacion basica
        if df_weather.empty:
            logger.warning("No hay datos para guardar")
            return False
        elif df_weather[['city_id', 'observation_time']].isnull().any().any():
            logger.error("Datos incompletos detectados - abortando escritura")
            return False
        else:
            save_to_delta_lake(
                df=df_weather,
                base_path=config['temporal_data_path'],
                partition_cols=["date", "hour"],
                mode="merge",
                # Claves de negocio: date, hour y city_id
                merge_predicate="target.city_id = source.city_id AND target.date = source.date AND target.hour = source.hour"
            )
            logger.info(f"Pipeline ejecutado exitosamente: {len(df_weather)} registros procesados")
            return True
    else:
        logger.warning("No se obtuvieron datos climáticos")
        return False


def refresh_city_metadata() -> bool:
    """Actualiza los metadatos de ciudades en Delta Lake (full refresh)."""

    metadata = get_city_metadata(config['cities'])

    if metadata:
        df_metadata = metadata_to_dataframe(metadata)
        save_to_delta_lake(
            df=df_metadata,
            base_path=config['static_data_path'],
            mode="overwrite"  # Full refresh para datos estáticos
        )
        logger.info(f"Metadatos actualizados exitosamente: {len(df_metadata)} ciudades")
        return True
    else:
        logger.warning("No se obtuvieron metadatos de ciudades")
        return False


# ============================================================================
# VERIFICACIONES
# ============================================================================

def verify_delta_table(path, table_name):
    """
    Verificación de tablas Delta
    NOTA 1: Logger para el flujo del proceso, print/display para visualización de datos.
    NOTA 2: Función temporal para 1ra entrega (extracción y Datalakehouse).
    """
    logger.info(f"Verificando: {table_name}")

    try:
        dt = DeltaTable(path, storage_options=config['storage_options'])
        df = dt.to_pandas()

        logger.info(f"CONEXIÓN EXITOSA - Registros: {len(df):,}\n")

        # Info general
        df.info()
        print()

        # Datos
        # Aunque actualmente solo hay 5 registros, se aplica head(5)
        # anticipando el crecimiento del dataset en futuras entregas del pipeline
        print("PRIMERAS 5 FILAS:")
        print(df.head(5))
        print("\n")

        # Estadísticas
        numeric_cols = df.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            print("ESTADÍSTICAS:")
            print(df[numeric_cols].describe())
            print()

        # Valido merge (Duplicados)
        if 'city_id' in df.columns:
            key_cols = ['city_id', 'date', 'hour'] if 'date' in df.columns else ['city_id']
            duplicados = df.duplicated(subset=key_cols).sum()
            print(f"Duplicados: {duplicados}")
            print()

        return True

    except Exception as e:
        logger.error(f"Error verificando {table_name}: {e}")
        return False


def show_bucket_tree():
    """Muestra árbol completo del data lake"""

    print("ESTRUCTURA DEL DATA LAKE")
    print()

    s3 = boto3.client('s3',
                      endpoint_url=config['minio_config']["endpoint_url"],
                      aws_access_key_id=config['minio_config']["access_key"],
                      aws_secret_access_key=config['minio_config']["secret_key"])

    bucket = config['minio_config']["bucket_name"]

    # Creo árbol
    tree = Tree()
    tree.create_node(bucket, bucket)

    # Proceso objetos
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket):
        for obj in page.get('Contents', []):
            parts = obj['Key'].split('/')
            parent = bucket

            for i, part in enumerate(parts):
                node_id = '/'.join(parts[:i+1])

                if not tree.contains(node_id):
                    tree.create_node(part, node_id, parent=parent)

                parent = node_id

    tree.show()


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Función principal que orquesta el pipeline"""
    
    # PREPARACION DEL ENTORNO DE TRABAJO
    global config, logger
    config = load_config()  # Cargo configuración
    logger = setup_logging()  # Configuro logs
    test_minio_connection()  # Pruebo conexión

    # PROCESO DE EXTRACCION Y ESTANDARIZACION DE DATOS
    extract_and_save_weather_data()  # Extraigo y guardo datos climaticos
    refresh_city_metadata()  # Refresco metadatos de ciudades

    logger.info("Iniciando verificación\n")

    # VERIFICACIONES y REPORTE DEL PROCESO
    verify_delta_table(config['temporal_data_path'], "Datos Climáticos")  # Verifico Delta Table para datos climaticos
    print("-" * 60 + "\n")
    verify_delta_table(config['static_data_path'], "Metadatos Ciudades")  # Verifico Delta Table para metadatos de ciudades
    print("-" * 60 + "\n")
    show_bucket_tree()  # Muestro estructura del Datalakehouse

    logger.info("Verificación completa.")


if __name__ == "__main__":
    main()
