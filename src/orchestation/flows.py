import logging
from datetime import datetime, timezone
from typing import Optional
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

logger = logging.getLogger(__name__)


def extract_and_save_weather_data(config, api_client, data_transformer, save_to_delta_lake) -> bool:
    """
    Extracción INCREMENTAL usando Delta Lake transaction log.
    """
    logger.info("EXTRACCIÓN INCREMENTAL: Datos Climáticos")

    # Verificar cambios (usando transaction log)
    if not _should_extract_incremental(config, interval_minutes=10):
        return True

    # Extraer datos
    extraction_start = datetime.now(timezone.utc)
    weather_data = []
    
    for city in config['cities']:
        logger.info(f"Extrayendo: {city}")
        data = api_client.get_current_weather(city)
        if data:
            weather_data.append(data)

    if not weather_data:
        logger.warning("No se obtuvieron datos")
        return False

    df_weather = data_transformer.weather_to_dataframe(weather_data)
    logger.info(f"Datos extraídos: {len(df_weather)} registros")

    # MERGE incremental
    logger.info("Ejecutando MERGE incremental...")
    save_to_delta_lake(
        df=df_weather,
        base_path=config['temporal_data_path'],
        storage_options=config['storage_options'],
        partition_cols=["date", "hour"],
        mode="merge",
        merge_predicate="target.city_id = source.city_id AND target.date = source.date AND target.hour = source.hour"
    )

    # Mostrar métricas simplificadas
    duration = (datetime.now(timezone.utc) - extraction_start).total_seconds()

    logger.info("EXTRACCIÓN INCREMENTAL COMPLETADA:")
    logger.info(f"   • Registros procesados:   {len(df_weather)}")
    logger.info(f"   • Duración:               {duration:.2f}s")
    logger.info(f"   • Método checkpoint:      Delta Lake transaction log")

    return True


def _get_last_extraction_timestamp(config) -> Optional[datetime]:
    """Lee timestamp del Delta Lake transaction log"""
    try:
        dt = DeltaTable(
            config['temporal_data_path'],
            storage_options=config['storage_options']
        )
        history = dt.history(limit=1).to_pandas()

        if len(history) > 0:
            last_timestamp = history['timestamp'].iloc[0]
            if last_timestamp.tzinfo is None:
                last_timestamp = last_timestamp.replace(tzinfo=timezone.utc)
            return last_timestamp

    except TableNotFoundError:
        return None
    except Exception as e:
        logger.warning(f"Error leyendo Delta log: {e}")
        return None

    return None


def _should_extract_incremental(config, interval_minutes: int = 10) -> bool:
    """Valida si debe ejecutarse extracción usando metadatos de Delta Lake."""
    last_extraction = _get_last_extraction_timestamp(config)

    if last_extraction is None:
        logger.info("Primera ejecución - proceder con extracción")
        return True

    elapsed = datetime.now(timezone.utc) - last_extraction
    elapsed_minutes = elapsed.total_seconds() / 60

    if elapsed_minutes < interval_minutes:
        logger.info(f"Omitiendo extracción: última hace {elapsed_minutes:.1f} min")
        logger.info(f"Próxima extracción en: {interval_minutes - elapsed_minutes:.1f} min")
        return False

    logger.info(f"Proceder: última extracción hace {elapsed_minutes:.1f} min")
    return True


def refresh_city_metadata(config, api_client, data_transformer, save_to_delta_lake) -> bool:
    """
    Extracción FULL de metadatos de ciudades.
    """
    metadata = []
    
    for city in config['cities']:
        logger.info(f"Extrayendo metadatos para: {city}")
        city_metadata = api_client.get_city_metadata(city)
        if city_metadata:
            metadata.append(city_metadata)

    if metadata:
        df_metadata = data_transformer.metadata_to_dataframe(metadata)

        # EXTRACCIÓN FULL
        save_to_delta_lake(
            df=df_metadata,
            base_path=config['static_data_path'],
            storage_options=config['storage_options'],
            mode="overwrite"
        )
        logger.info(f"Extracción FULL completada: {len(df_metadata)} ciudades")
        return True

    logger.warning("No se obtuvieron metadatos de ciudades")
    return False


def process_weather_data(config, clean_weather_data, enrich_temporal_features, 
                        create_weather_categories, create_city_daily_aggregates, 
                        save_to_delta_lake) -> bool:
    """
    Orquesta todo el pipeline de procesamiento
    """
    try:
        logger.info("=== INICIANDO PIPELINE DE PROCESAMIENTO ===")

        # 1. Datos crudos (TP1)
        logger.info("Leyendo datos climáticos crudos...")
        raw_weather_df = DeltaTable(
            config['temporal_data_path'],
            storage_options=config['storage_options']
        ).to_pandas()

        logger.info(f"Datos crudos cargados: {len(raw_weather_df)} registros")

        # 2. Transformaciones
        logger.info("Aplicando transformaciones...")

        cleaned_df = clean_weather_data(raw_weather_df)
        enriched_df = enrich_temporal_features(cleaned_df)
        categorized_df = create_weather_categories(enriched_df)
        daily_aggregates_df = create_city_daily_aggregates(categorized_df)

        # 3. Guardar datos procesados
        logger.info("Guardando datos procesados...")
        processed_path = config.get('processed_data_path', f"{config['data_lake_base']}/processed_data")

        save_to_delta_lake(
            df=daily_aggregates_df,
            base_path=processed_path,
            storage_options=config['storage_options'],
            partition_cols=["year", "month"],
            mode="overwrite"
        )

        logger.info("=== PIPELINE DE PROCESAMIENTO COMPLETADO ===")
        logger.info(f"Datos procesados guardados en: {processed_path}")

        return True

    except Exception as e:
        logger.error(f"Error en el pipeline de procesamiento: {e}")
        return False
