import pandas as pd
import logging

logger = logging.getLogger(__name__)


def clean_weather_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transformación 1: Limpieza de datos
    """
    logger.info("Iniciando limpieza de datos climáticos")

    cleaned_df = df.copy()

    # 1. Duplicados
    initial_count = len(cleaned_df)
    cleaned_df = cleaned_df.drop_duplicates()
    duplicates_removed = initial_count - len(cleaned_df)
    logger.info(f"Duplicados eliminados: {duplicates_removed}")

    # 2. Nulos
    critical_columns = ['temperature', 'humidity', 'pressure', 'wind_speed']
    for col in critical_columns:
        if col in cleaned_df.columns:
            null_count = cleaned_df[col].isnull().sum()
            if null_count > 0:
                cleaned_df[col] = cleaned_df.groupby('city_id')[col].transform(
                    lambda x: x.fillna(x.median())
                )
                logger.info(f"Valores nulos en {col}: {null_count} reemplazados")

    # Temperatura Argentina (Rango: -20/+50)
    temp_mask = (cleaned_df['temperature'] >= -20) & (cleaned_df['temperature'] <= 50)
    outliers_temp = len(cleaned_df) - temp_mask.sum()
    if outliers_temp > 0:
        logger.warning(f"Valores de temperatura fuera de rango: {outliers_temp}")
        cleaned_df = cleaned_df[temp_mask]

    # Humedad (Rango: 0-100%)
    humidity_mask = (cleaned_df['humidity'] >= 0) & (cleaned_df['humidity'] <= 100)
    outliers_humidity = len(cleaned_df) - humidity_mask.sum()
    if outliers_humidity > 0:
        logger.warning(f"Valores de humedad fuera de rango: {outliers_humidity}")
        cleaned_df = cleaned_df[humidity_mask]

    logger.info(f"Limpieza completada. Registros finales: {len(cleaned_df)}")
    return cleaned_df
