import configparser
import logging

from pathlib import Path
from typing import Dict, Any


class PipelineConfig:
    """
    Maneja la configuración del pipeline con validación y cache.
    Auto-configura su propio logger para ser independiente.
    """
    
    def __init__(self, config_file: str = "pipeline.conf"):
        self.config_file = Path(config_file)
        self._config_data: Dict[str, Any] = None
        self._setup_logger()
        self._validate_config_file()
    
    def _setup_logger(self) -> None:
        """Configura logger privado para esta clase."""
        self._logger = logging.getLogger(f"{__name__}.PipelineConfig")
        
        if not self._logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s',
                datefmt='%H:%M:%S'
            )
            handler.setFormatter(formatter)
            self._logger.addHandler(handler)
            self._logger.setLevel(logging.INFO)
            self._logger.propagate = False
    
    def _validate_config_file(self) -> None:
        """Valida que el archivo de configuración exista."""
        if not self.config_file.exists():
            raise FileNotFoundError(f"Configuration file '{self.config_file}' not found")
        self._logger.info(f"Config file validated: {self.config_file}")
    
    def _load_and_parse_config(self) -> Dict[str, Any]:
        """Carga y parsea el archivo de configuración."""
        config = configparser.ConfigParser()
        
        try:
            config.read(self.config_file, encoding='utf-8')
        except UnicodeDecodeError:
            config.read(self.config_file, encoding='latin-1')
            self._logger.warning("Used latin-1 encoding as fallback")
        
        minio_config = {k: config.get('minio', k) for k in 
                       ['endpoint_url', 'access_key', 'secret_key', 'region', 'bucket_name']}
        
        base_path = f"s3://{minio_config['bucket_name']}/{config.get('data_lake', 'base_path')}"
        
        return {
            'api_key': config.get('api', 'api_key'),
            'base_url': config.get('api', 'base_url'),
            'api_timeout': config.getint('api', 'api_timeout'),
            'max_retries': config.getint('api', 'max_retries'),
            'cities': [c.strip() for c in config.get('api', 'cities').split(',')],
            'minio_config': minio_config,
            'storage_options': {
                "AWS_ENDPOINT_URL": minio_config["endpoint_url"],
                "AWS_ACCESS_KEY_ID": minio_config["access_key"],
                "AWS_SECRET_ACCESS_KEY": minio_config["secret_key"],
                "AWS_REGION": minio_config["region"],
                "AWS_ALLOW_HTTP": "true"
            },
            'data_lake_base': base_path,
            'temporal_data_path': f"{base_path}/{config.get('data_lake', 'temporal_path')}",
            'static_data_path': f"{base_path}/{config.get('data_lake', 'static_path')}",
            'processed_data_path': f"{base_path}/{config.get('data_lake', 'processed_path', fallback='processed_data')}",
            'processed_detailed_path': f"{base_path}/{config.get('data_lake', 'processed_detailed_path', fallback='processed_detailed')}"
        }
    
    @property
    def data(self) -> Dict[str, Any]:
        """Propiedad que devuelve la configuración (con cache)."""
        if self._config_data is None:
            self._logger.info("Loading configuration...")
            self._config_data = self._load_and_parse_config()
            self._logger.info("Configuration loaded successfully")
        return self._config_data
    
    def get(self, key: str, default: Any = None) -> Any:
        """Obtiene un valor de configuración con valor por defecto."""
        return self.data.get(key, default)
    
    def reload(self) -> None:
        """Fuerza la recarga de la configuración."""
        self._logger.info("Reloading configuration...")
        self._config_data = self._load_and_parse_config()
        self._logger.info("Configuration reloaded")
    
    def validate_required_keys(self) -> bool:
        """Valida que las claves requeridas estén presentes."""
        required_keys = ['api_key', 'base_url', 'minio_config']
        missing_keys = [key for key in required_keys if not self.data.get(key)]
        
        if missing_keys:
            self._logger.error(f"Missing required config keys: {missing_keys}")
            return False
        
        self._logger.info("All required config keys are present")
        return True
    
    def __getitem__(self, key: str) -> Any:
        """Permite acceso tipo dict: config['api_key']"""
        return self.data[key]
    
    def __contains__(self, key: str) -> bool:
        """Permite verificación: 'api_key' in config"""
        return key in self.data
