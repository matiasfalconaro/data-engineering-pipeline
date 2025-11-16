## OpenWeatherMap Datalakehouse pipeline
Pipeline ETL para datos climaticos:
- Extrae de OpenWeather API
- Procesa mediante arquitectura Medallion de 3 capas
- Almacena en bucket S3 MinIO como Delta Lake

# Flujo de datos
```bash
Bronze: Datos crudos de la API → MinIO (incremental, Intervalo: 10min)
Silver: Limpieza + Funcionalidades temporales + Categorizacion del clima
Gold: Agregados diarios por ciudad
```

## Quick Start
```
1. Configure
cp pipeline.conf.example pipeline.conf

2. Editar pipeline.conf con credenciales

3. Configurar parametros de flujo en main.main()

4. Ejecutar el pipeline
python main.py
```

## Arquitectura
```bash
src/
├── core/           # Config y clientes
├── layers/         # Procesamiento
│   ├── bronze/     # Extraccion cruda y estructura
│   ├── silver/     # Limpieza y enrriquecimiento
│   └── gold/       # Agregados
├── utils/          # Operaciones de archivos y calidad
└── orchestration/  # Flujos
```

## Requerimientos
```
Python 3.8+
MinIO count
OpenWeather API key
See requirements.txt for dependencies
```