import boto3
import logging

from botocore.exceptions import ClientError
from typing import (List,
                    Optional)


logger = logging.getLogger(__name__)


class MinIOClient:
    """
    Cliente para operaciones con MinIO (S3-compatible).
    
    Maneja conexiones, autenticación y operaciones comunes.
    """
    
    def __init__(self, config: dict):
        self.config = config
        self.minio_config = config['minio_config']
        self._s3_client = None
        self._is_connected = False
        
    @property
    def s3_client(self):
        """Property que devuelve el cliente S3 (con lazy initialization)."""
        if self._s3_client is None:
            self._s3_client = self._create_s3_client()
        return self._s3_client
    
    def _create_s3_client(self):
        """Crea y retorna el cliente S3 para MinIO."""
        return boto3.client(
            's3',
            endpoint_url=self.minio_config["endpoint_url"],
            aws_access_key_id=self.minio_config["access_key"],
            aws_secret_access_key=self.minio_config["secret_key"],
            region_name=self.minio_config["region"]
        )
    
    def test_connection(self) -> bool:
        """
        Prueba la conexión a MinIO y valida credenciales.
        
        Returns:
            bool: True si la conexión es exitosa, False en caso contrario
        """
        logger.info("Probando conexión a MinIO")
        logger.info(f"Conectando a: {self.minio_config['endpoint_url']}")

        try:
            response = self.s3_client.list_buckets()
            buckets = [b['Name'] for b in response['Buckets']]

            self._is_connected = True
            logger.info("Conexión a MinIO exitosa")
            logger.info(f"Número de buckets: {len(buckets)}")
            logger.info(f"Buckets disponibles: {buckets}")
            return True

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'InvalidAccessKeyId':
                logger.error("Credenciales de acceso inválidas")
            elif error_code == 'SignatureDoesNotMatch':
                logger.error("Firma de autenticación incorrecta")
            else:
                logger.error(f"Error de cliente S3: {error_code} - {e}")
            return False
            
        except Exception as e:
            logger.error(f"Error conectando a MinIO: {e}")
            logger.error("Verificar: URL, credenciales, y que MinIO no esté caído")
            return False
    
    def list_buckets(self) -> List[str]:
        """Lista todos los buckets disponibles."""
        try:
            response = self.s3_client.list_buckets()
            return [b['Name'] for b in response['Buckets']]
        except Exception as e:
            logger.error(f"Error listando buckets: {e}")
            return []
    
    def bucket_exists(self, bucket_name: str) -> bool:
        """Verifica si un bucket existe."""
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError:
            return False
        except Exception as e:
            logger.error(f"Error verificando bucket {bucket_name}: {e}")
            return False
    
    def create_bucket(self, bucket_name: str) -> bool:
        """Crea un nuevo bucket."""
        try:
            self.s3_client.create_bucket(Bucket=bucket_name)
            logger.info(f"Bucket '{bucket_name}' creado exitosamente")
            return True
        except Exception as e:
            logger.error(f"Error creando bucket '{bucket_name}': {e}")
            return False
    
    def get_storage_options(self) -> dict:
        """Retorna las opciones de storage para Delta Lake."""
        return {
            "AWS_ENDPOINT_URL": self.minio_config["endpoint_url"],
            "AWS_ACCESS_KEY_ID": self.minio_config["access_key"],
            "AWS_SECRET_ACCESS_KEY": self.minio_config["secret_key"],
            "AWS_REGION": self.minio_config["region"],
            "AWS_ALLOW_HTTP": "true"
        }
    
    @property
    def is_connected(self) -> bool:
        """Indica si el cliente está conectado a MinIO."""
        return self._is_connected
    
    def reconnect(self) -> bool:
        """Fuerza la reconexión del cliente."""
        self._s3_client = None
        self._is_connected = False
        return self.test_connection()


# Funciones legacy para compatibilidad
def get_s3_client(config):
    """Función legacy - usar MinIOClient en nuevo código."""
    return MinIOClient(config).s3_client

def test_minio_connection(config):
    """Función legacy - usar MinIOClient en nuevo código."""
    return MinIOClient(config).test_connection()
