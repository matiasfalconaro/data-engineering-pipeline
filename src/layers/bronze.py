import requests
import time
import pandas as pd
import logging

from datetime import (datetime,
                      timezone)
from typing import (Dict,
                    List,
                    Optional,
                    Union)


logger = logging.getLogger(__name__)


class WeatherAPIClient:
    """Cliente para OpenWeather API con manejo de errores y reintentos."""
    
    def __init__(self, config: dict):
        self.config = config
        self.base_url = config['base_url']
        self.api_key = config['api_key']
        self.timeout = config['api_timeout']
        self.max_retries = config['max_retries']
        
    def _make_api_request(self, endpoint: str, params: Dict) -> Optional[Dict]:
        """Método interno para requests con reintentos."""
        for attempt in range(self.max_retries):
            try:
                response = requests.get(endpoint, params=params, timeout=self.timeout)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.Timeout:
                if attempt == self.max_retries - 1:
                    logger.error(f"Timeout después de {self.max_retries} intentos")
                    return None
                time.sleep(2 ** attempt)
            except requests.exceptions.RequestException as e:
                if attempt == self.max_retries - 1:
                    logger.error(f"Error en petición: {e}")
                    return None
                time.sleep(2 ** attempt)
        return None
    
    def get_current_weather(self, city: str) -> Optional[Dict]:
        """Obtiene clima actual para una ciudad - SIN LOG DUPLICADO"""
        endpoint = f"{self.base_url}/weather"
        params = {
            "q": city,
            "appid": self.api_key,
            "units": "metric",
            "lang": "es"
        }
        
        data = self._make_api_request(endpoint, params)
        
        if data and data.get("cod") == 200:
            data['extraction_timestamp'] = datetime.now(timezone.utc).isoformat()
            data['extraction_city'] = city
            return data
        elif data:
            logger.warning(f"API retornó código {data.get('cod')} para {city}")
            return None
        return None
    
    def get_city_metadata(self, city: str) -> Optional[Dict]:
        """Obtiene metadatos de una ciudad - SIN LOG DUPLICADO"""
        endpoint = f"{self.base_url}/weather"
        params = {"q": city, "appid": self.api_key}
        
        data = self._make_api_request(endpoint, params)
        
        if data:
            return {
                "city_id": data.get("id"),
                "city_name": data.get("name"),
                "country": data.get("sys", {}).get("country"),
                "latitude": data.get("coord", {}).get("lat"),
                "longitude": data.get("coord", {}).get("lon"),
                "timezone_offset": data.get("timezone"),
                "last_updated": datetime.now(timezone.utc).isoformat()
            }
        return None


class WeatherDataTransformer:
    """Transforma datos de API a DataFrames."""
    
    @staticmethod
    def weather_to_dataframe(weather_data: Union[Dict, List[Dict]]) -> pd.DataFrame:
        """Convierte respuestas de clima a DataFrame."""
        if isinstance(weather_data, dict):
            weather_data = [weather_data]

        records = []
        for data in weather_data:
            if data is None:
                continue
                
            observation_dt = datetime.fromtimestamp(data.get("dt", 0), tz=timezone.utc)
            
            record = {
                "city_id": data.get("id"),
                "city_name": data.get("name"),
                "country": data.get("sys", {}).get("country"),
                "weather_main": data.get("weather", [{}])[0].get("main"),
                "weather_description": data.get("weather", [{}])[0].get("description"),
                "temperature": data.get("main", {}).get("temp"),
                "feels_like": data.get("main", {}).get("feels_like"),
                "temp_min": data.get("main", {}).get("temp_min"),
                "temp_max": data.get("main", {}).get("temp_max"),
                "pressure": data.get("main", {}).get("pressure"),
                "humidity": data.get("main", {}).get("humidity"),
                "visibility": data.get("visibility"),
                "wind_speed": data.get("wind", {}).get("speed"),
                "wind_direction": data.get("wind", {}).get("deg"),
                "cloudiness": data.get("clouds", {}).get("all"),
                "observation_time": observation_dt.isoformat(),
                "extraction_timestamp": data.get("extraction_timestamp"),
                "date": observation_dt.strftime("%Y-%m-%d"),
                "hour": observation_dt.strftime("%H")
            }
            records.append(record)

        df = pd.DataFrame(records)
        logger.info(f"DataFrame creado con {len(df)} registros")
        return df
    
    @staticmethod
    def metadata_to_dataframe(metadata: List[Dict]) -> pd.DataFrame:
        """Convierte metadatos a DataFrame."""
        df = pd.DataFrame(metadata)
        logger.info(f"DataFrame de metadatos creado con {len(df)} registros")
        return df
