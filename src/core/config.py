import configparser

from pathlib import Path


def load_config(config_file="pipeline.conf"):
    """Carga configuración desde el archivo pipeline.conf"""
    if not Path(config_file).exists():
        raise FileNotFoundError(f"Configuration file '{config_file}' not found")

    config = configparser.ConfigParser()
    
    # UTF-8 explícito
    try:
        config.read(config_file, encoding='utf-8')
    except UnicodeDecodeError:
        # Si falla UTF-8, intentar con latin-1
        config.read(config_file, encoding='latin-1')

    minio = {k: config.get('minio', k) for k in
             ['endpoint_url', 'access_key', 'secret_key', 'region', 'bucket_name']}

    base = f"s3://{minio['bucket_name']}/{config.get('data_lake', 'base_path')}"

    return {
        'api_key': config.get('api', 'api_key'),
        'base_url': config.get('api', 'base_url'),
        'api_timeout': config.getint('api', 'api_timeout'),
        'max_retries': config.getint('api', 'max_retries'),
        'cities': [c.strip() for c in config.get('api', 'cities').split(',')],
        'minio_config': minio,
        'storage_options': {
            "AWS_ENDPOINT_URL": minio["endpoint_url"],
            "AWS_ACCESS_KEY_ID": minio["access_key"],
            "AWS_SECRET_ACCESS_KEY": minio["secret_key"],
            "AWS_REGION": minio["region"],
            "AWS_ALLOW_HTTP": "true"
        },
        'data_lake_base': base,
        'temporal_data_path': f"{base}/{config.get('data_lake', 'temporal_path')}",
        'static_data_path': f"{base}/{config.get('data_lake', 'static_path')}",
        'processed_data_path': f"{base}/{config.get('data_lake', 'processed_path', fallback='processed_data')}",
        'processed_detailed_path': f"{base}/{config.get('data_lake', 'processed_detailed_path', fallback='processed_detailed')}"
    }
