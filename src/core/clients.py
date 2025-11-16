import boto3
from botocore.exceptions import ClientError

def get_s3_client(config):
    """Get S3 client for MinIO"""
    return boto3.client(
        's3',
        endpoint_url=config['minio_config']["endpoint_url"],
        aws_access_key_id=config['minio_config']["access_key"],
        aws_secret_access_key=config['minio_config']["secret_key"],
        region_name=config['minio_config']["region"]
    )

def test_minio_connection(config):
    """
    Prueba la conexión a MinIO
    """
    import logging
    logger = logging.getLogger(__name__)
    logger.info("Probando conexión a MinIO")

    try:
        logger.info(f"Conectando a: {config['minio_config']['endpoint_url']}")

        s3_client = get_s3_client(config)
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
