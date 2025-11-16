import boto3
import pandas as pd
import pyarrow as pa
import logging

from treelib import Tree
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError
from typing import List, Optional


logger = logging.getLogger(__name__)


def save_to_delta_lake(df: pd.DataFrame,
                      base_path: str,
                      storage_options: dict,
                      partition_cols: Optional[List[str]] = None,
                      mode: str = "append",
                      merge_predicate: Optional[str] = None) -> None:
    """
    Guarda un DataFrame en formato Delta Lake en MinIO/S3.
    """
    try:
        logger.info(f"Guardando {len(df)} registros en {base_path} (modo: {mode})")

        pa_table = pa.Table.from_pandas(df)

        if mode == "merge":
            if not merge_predicate:
                raise ValueError("merge_predicate es requerido para mode='merge'")

            try:
                dt = DeltaTable(base_path, storage_options=storage_options)
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
                    storage_options=storage_options
                )
                dt = DeltaTable(base_path, storage_options=storage_options)
                _apply_initial_constraints(dt, base_path, storage_options)
                logger.info(f"Tabla creada con constraints: {len(df)} registros")

        elif mode in ["append", "overwrite"]:
            table_exists = True
            try:
                DeltaTable(base_path, storage_options=storage_options)
            except TableNotFoundError:
                table_exists = False

            write_deltalake(
                base_path,
                pa_table,
                mode=mode,
                partition_by=partition_cols,
                storage_options=storage_options
            )
            logger.info(f"{mode.upper()} exitoso: {len(df)} registros")

            if not table_exists and mode == "overwrite":
                dt = DeltaTable(base_path, storage_options=storage_options)
                _apply_initial_constraints(dt, base_path, storage_options)
                logger.info("Constraints iniciales aplicadas")

        else:
            raise ValueError(f"Modo no soportado: {mode}. Usar: append, overwrite o merge")

    except Exception as e:
        logger.error(f"Error escribiendo en Delta Lake ({base_path}): {e}")
        raise

def _apply_initial_constraints(dt: DeltaTable, base_path: str, storage_options: dict) -> None:
    """
    Aplica constraints de integridad solo durante la creación inicial de la tabla.
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

    for constraint_name, constraint_expr in constraints.items():
        try:
            dt.alter.add_constraint({constraint_name: constraint_expr})
            logger.info(f"Constraint inicial '{constraint_name}' aplicado exitosamente")
        except Exception as e:
            logger.warning(f"Error aplicando constraint inicial '{constraint_name}': {e}")


def verify_delta_table(path: str, table_name: str, storage_options: dict) -> bool:
    """
    Verificación de tablas Delta
    """
    logger.info(f"Verificando: {table_name}")

    try:
        dt = DeltaTable(path, storage_options=storage_options)
        df = dt.to_pandas()

        logger.info(f"CONEXIÓN EXITOSA - Registros: {len(df):,}\n")

        # Info general
        print(f"Información del DataFrame '{table_name}':")
        print(f"Forma: {df.shape}")
        print(f"Columnas: {list(df.columns)}")
        print()

        # Datos
        print("PRIMERAS 5 FILAS:")
        print(df.head(5).to_string())
        print("\n")

        # Estadísticas
        numeric_cols = df.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            print("ESTADÍSTICAS:")
            print(df[numeric_cols].describe().to_string())
            print()

        return True

    except Exception as e:
        logger.error(f"Error verificando {table_name}: {e}")
        return False


def show_bucket_tree(config: dict) -> None:
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
