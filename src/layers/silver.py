import pandas as pd
import logging
from typing import Dict

logger = logging.getLogger(__name__)


def enrich_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transformación 2: Enriquecimiento temporal
    """
    logger.info("Enriqueciendo características temporales")

    enriched_df = df.copy()

    enriched_df['observation_datetime'] = pd.to_datetime(enriched_df['observation_time'])
    enriched_df['day_of_week'] = enriched_df['observation_datetime'].dt.dayofweek
    enriched_df['is_weekend'] = enriched_df['day_of_week'].isin([5, 6]).astype(int)
    enriched_df['time_category'] = enriched_df['hour'].apply(_get_time_category)
    enriched_df['month'] = enriched_df['observation_datetime'].dt.month
    enriched_df['season'] = enriched_df['month'].apply(_get_southern_season)

    logger.info("Enriquecimiento temporal completado")
    return enriched_df


def _get_time_category(hour):
    hour = int(hour)
    if 5 <= hour < 12:
        return 'mañana'
    elif 12 <= hour < 18:
        return 'tarde'
    elif 18 <= hour < 24:
        return 'noche'
    else:
        return 'madrugada'


def _get_southern_season(month):
    if 12 <= month or month <= 2:
        return 'verano'
    elif 3 <= month <= 5:
        return 'otoño'
    elif 6 <= month <= 8:
        return 'invierno'
    else:
        return 'primavera'


def create_weather_categories(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transformación 3: Categorización climática
    """
    logger.info("Creando categorías climáticas")

    categorized_df = df.copy()
    categorized_df['temp_category'] = categorized_df['temperature'].apply(_categorize_temperature)
    categorized_df['humidity_category'] = categorized_df['humidity'].apply(_categorize_humidity)
    categorized_df['wind_intensity'] = categorized_df['wind_speed'].apply(_categorize_wind_speed)
    categorized_df['weather_summary'] = categorized_df.apply(
        lambda x: _summarize_weather_condition(x['weather_main'], x['weather_description']),
        axis=1
    )

    logger.info("Categorización climática completada")
    return categorized_df


def _categorize_temperature(temp):
    if temp < 10:
        return 'frío'
    elif 10 <= temp < 20:
        return 'templado'
    elif 20 <= temp < 30:
        return 'cálido'
    else:
        return 'caluroso'


def _categorize_humidity(humidity):
    if humidity < 30:
        return 'seco'
    elif 30 <= humidity < 60:
        return 'confortable'
    elif 60 <= humidity < 80:
        return 'húmedo'
    else:
        return 'muy húmedo'


def _categorize_wind_speed(speed):
    if speed < 1:
        return 'calma'
    elif 1 <= speed < 5:
        return 'brisa leve'
    elif 5 <= speed < 10:
        return 'brisa moderada'
    elif 10 <= speed < 15:
        return 'ventoso'
    else:
        return 'muy ventoso'


def _summarize_weather_condition(main, description):
    clear_conditions = ['clear', 'cielo claro']
    cloudy_conditions = ['clouds', 'nubes', 'few clouds', 'scattered clouds']
    rainy_conditions = ['rain', 'drizzle', 'lluvia', 'llovizna']

    if any(cond in str(main).lower() or cond in str(description).lower() for cond in clear_conditions):
        return 'despejado'
    elif any(cond in str(main).lower() or cond in str(description).lower() for cond in cloudy_conditions):
        return 'nublado'
    elif any(cond in str(main).lower() or cond in str(description).lower() for cond in rainy_conditions):
        return 'lluvioso'
    else:
        return 'otros'
