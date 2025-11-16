import requests
import time
import pandas as pd
import logging

from datetime import datetime, timezone
from typing import Dict, List, Optional
from typing import Dict, List, Optional, Union

logger = logging.getLogger(__name__)


def make_api_request(endpoint: str, params: Dict, config: dict) -> Optional[Dict]:
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


def get_current_weather(city: str, config: dict) -> Optional[Dict]:
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
        data = make_api_request(endpoint, params, config)

        if data and data.get("cod") == 200:
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


def get_city_metadata(city_list: List[str], config: dict) -> List[Dict]:
    """
    Genera metadatos de la ciudad a partir de la respuesta de la API.
    """
    metadata = []

    for city in city_list:
        endpoint = f"{config['base_url']}/weather"
        params = {
            "q": city,
            "appid": config['api_key']
        }

        logger.info(f"Extrayendo metadatos para {city}")
        data = make_api_request(endpoint, params, config)

        if data:
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
