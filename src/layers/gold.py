import pandas as pd
import logging


logger = logging.getLogger(__name__)


def create_city_daily_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transformación 4: Agregaciones diarias por ciudad
    """
    logger.info("Creando agregados diarios por ciudad")

    df['date_dt'] = pd.to_datetime(df['date'])
    
    daily_agg = df.groupby(['city_id', 'city_name', 'date_dt']).agg({
        'temperature': ['mean', 'max', 'min', 'std', 'count'],
        'humidity': 'mean',
        'pressure': 'mean', 
        'wind_speed': 'mean',
        'weather_summary': lambda x: x.mode()[0] if len(x.mode()) > 0 else 'desconocido',
        'temp_category': lambda x: x.mode()[0] if len(x.mode()) > 0 else 'desconocido'
    }).reset_index()

    daily_agg.columns = ['city_id',
                        'city_name',
                        'date',
                        'avg_temperature',
                        'max_temperature',
                        'min_temperature',
                        'temp_std',
                        'temp_count',
                        'avg_humidity',
                        'avg_pressure',
                        'avg_wind_speed',
                        'predominant_weather',
                        'predominant_temp_category']

    daily_agg['daily_temp_range'] = daily_agg['max_temperature'] - daily_agg['min_temperature']
    daily_agg['temp_std'] = daily_agg['temp_std'].fillna(0)

    numeric_cols = ['avg_temperature',
                    'max_temperature',
                    'min_temperature',
                    'temp_std',
                    'avg_humidity',
                    'avg_pressure',
                    'avg_wind_speed',
                    'daily_temp_range']
    
    for col in numeric_cols:
        daily_agg[col] = daily_agg[col].round(2)

    daily_agg['year'] = daily_agg['date'].dt.year
    daily_agg['month'] = daily_agg['date'].dt.month
    daily_agg['day'] = daily_agg['date'].dt.day

    logger.info(f"Agregados diarios creados: {len(daily_agg)} registros")
    return daily_agg
