# TP2: Pipeline Datalakehouse para API de clima
Data Engineering - CEL UTN

Modulo 2: Procesamiento de datos

Fecha limite de entrega: Domingo, 9 de Noviembre de 2025, 23:59

Alumno: Matias Falconaro

## Oportunidades de mejora aplicadas al TP1
Mejoras propuestas en TP1 sobre `Almacenamiento de datos`

Mejoras en TP1 sobre `Extracción full e incremental`

## Repositorio GitHub
[Codigo modularizado en un artefacto Python](https://github.com/matiasfalconaro/data-engineering-pipeline)

## Definición del Alcance para el dominio de datos

Criterio de Selección Poblacional: 5 ciudades argentinas más pobladas.

[Ciudades más pobladas de Argentina - Wikipedia](https://en.wikipedia.org/wiki/List_of_cities_in_Argentina_by_population)

Cobertura estratégica de los centros urbanos con mayor densidad poblacional para pronósticos climáticos que impacten a la mayor cantidad de habitantes.

## Arquitectura

![Arquitectura del Pipeline](https://drive.google.com/uc?export=view&id=1rxQImMYwympOK95-Y5dKXiYBXTpOo_N6)

## Decisiones de desarrollo

| Categoría | Modulo | Decisión | Justificación |
|-----------|--------|----------|---------------|
|**Investigacion APIs**| TP1 |Sub-foros de data engineering|[StackOverflow](https://stackoverflow.com/questions/29913271/weather-api-for-providing-weather-forecast-based-upon-location)<br> [Reddit](https://www.reddit.com/r/dataengineering/comments/14lcyxr/resources_for_weathergeospatial_data/)<br> [Medium](https://medium.com/@ajeet214/9-free-weather-apis-for-ai-data-projects-6bfc66022e46)|
| **API** | TP1 | OpenWeatherMap | Mencion recurrente en diferentes foros<br> Plan gratuito disponible<br> Documentación detallada<br> Tiempo de actividad confiable<br> Endpoints claros para clima actual y metadatos |
| **Extracción** | TP1 | **Incremental** (datos temporales)<br>**Full** (datos estáticos) | Clima actual se actualiza cada 10 minutos<br> Append para preservar histórico<br> Metadatos cambian raramente |
| **Checkpoint Incremental** | TP1 | Delta Lake Transaction Log | Elimina dependencia de archivos externos<br>Aprovecha metadata nativa de Delta<br>Más robusto que timestamps manuales |
| **Particionamiento** | TP1 | **Por fecha/hora** (temporales)<br>**Sin particionamiento** (estáticos) | Optimiza consultas temporales<br> Organiza grandes volúmenes de datos<br> Dataset estático es pequeño |
| **Verificación de Infraestructura** | TP1 | boto3 para validación de bucket MinIO | Valida conectividad S3 antes de ejecutar pipeline<br> Detecta errores de configuración tempranamente<br> Evita fallos silenciosos durante escritura Delta Lake<br> Compatibilidad estándar con cualquier S3-compatible storage<br> Permite realizar verificacion de forma programatica sobre el bucket<br>[StackOverflow](https://stackoverflow.com/questions/51104230/how-to-automate-permissions-for-aws-s3-bucket-objects)|
| **Elecciones Técnicas** | TP1 | Zona horaria UTC<br> Estructura modular | Evita problemas de zonas horarias<br> Código reutilizable y mantenible |
| **Manejo de Errores** | TP1 | Validación de respuestas API<br> Sistema de logging<br> Manejo datos faltantes<br> Verificación de directorios | Depuración y trazabilidad |
| **Performance** | TP1 | Extracción paralelizable<br> Particionamiento temporal<br> Procesamiento por lotes<br> Reintentos automáticos | Optimización consultas<br> Manejo eficiente de memoria<br> Resiliencia a fallos de red |
| **Documentación** | TP1 | Type hints<br> Docstring minimizados| Reduce la cantidad de lineas de código<br> Mejora la legibilidad de las funciones<br> Evita explicaciones sobre parametros y salidas en forma de string dentro del docstring|
| **Versionamiento Delta Lake** | TP1 | Archivos históricos preservados | MERGE crea nuevos archivos en lugar de modificar existentes<br> Archivos antiguos marcados como "removed" en transaction log<br> Permite time travel y auditoría de cambios<br> No indica duplicación de datos<br>|
| **Diseño de función de guardado** | TP1 | Función genérica `save_to_delta_lake()`<br> 3 modos: `append`, `overwrite`, `merge` | Evita duplicación de código entre datos temporales y estáticos<br> Código validado (0 duplicados, constraints activos)<br>|
| **Arquitectura**| TP2 | **3-layer Medallion Architecture**:<br> Boronze: Integracion cruda<br> Silver: Filtrado, Limpieza y Enrriquecimiento<br> Gold: Agregados a nivel de nogocio | Evolucion natural de datos<br> Progresion de datos con calidad<br> Acceso controlado por capas<br> [Data Engineering Wiki](https://dataengineering.wiki/Concepts/Data+Architecture/Medallion+Architecture)<br> [DataBricks](https://www.databricks.com/glossary/medallion-architecture)
| **Procesamiento** | TP2 | **Pipeline 4-etapas**:<br> →Limpieza <br> →Enriquecimiento Temporal<br> →Categorización Climática<br> →Agregación Diaria | Preserva datos crudos + genera datos enriquecidos<br> Transformaciones específicas para análisis climático<br> Optimizado para reporting y dashboards<br> Mantiene trazabilidad completa del procesamiento |
| **Transformaciones de Datos** | TP2 | **Específicas para contexto argentino**:<br> Rangos temperatura -20°C a +50°C<br> Estaciones hemisferio sur<br> Categorías percepción local<br> Agregados para toma de decisiones urbanas | Datos técnicos → información estratégica<br> Adaptado a realidad geográfica argentina<br> Optimizado para ciudades pobladas (~35% población nacional)<br> Habilita planificación de servicios públicos y alertas tempranas |