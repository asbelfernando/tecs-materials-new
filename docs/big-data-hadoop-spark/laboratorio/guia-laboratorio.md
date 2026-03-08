# Guía de laboratorio: Desarrollo de Aplicaciones de Big Data con Spark en Databricks

## 1. Propósito
En este laboratorio desarrollarás una pequeña aplicación analítica usando PySpark para procesar datos de transacciones de retail.

## 2. Contexto de negocio
Una empresa desea responder preguntas como:
- ¿qué categorías venden más?
- ¿qué ciudades generan mayores ingresos?
- ¿qué productos tienen mayor salida?
- ¿cuál es el ticket promedio?
- ¿cómo se distribuyen las ventas por fecha?

## 3. Duración estimada
2 horas

## 4. Competencias a desarrollar
- uso de DataFrames en Spark
- carga estructurada de datos
- limpieza de registros
- transformación con columnas derivadas
- agregaciones
- exportación de resultados

## 5. Preparación
1. Inicia sesión en Databricks
2. Crea un clúster o usa uno disponible
3. Crea un notebook nuevo en Python
4. Carga el archivo CSV de ejemplo a FileStore o DBFS

## 6. Actividades

### Actividad 1. Carga del dataset
- importar librerías
- definir esquema
- leer el archivo CSV
- visualizar los primeros registros

### Actividad 2. Exploración
- revisar el esquema
- contar filas
- describir columnas numéricas
- validar valores nulos

### Actividad 3. Limpieza
- eliminar registros críticos incompletos
- quitar espacios innecesarios
- convertir fecha al tipo correcto

### Actividad 4. Transformación
- crear la columna `total_amount = quantity * unit_price`
- generar tablas agregadas por categoría y ciudad

### Actividad 5. Análisis
Responder:
- ¿qué categoría tiene mayor venta total?
- ¿qué ciudad registra más ingresos?
- ¿qué producto tiene más unidades vendidas?
- ¿cuál es el ticket promedio?

### Actividad 6. Persistencia
- guardar resultados agregados en parquet
- validar que los archivos hayan sido escritos correctamente

## 7. Ejercicios adicionales
1. Obtener el top 5 de productos por ventas totales
2. Calcular ventas mensuales
3. Filtrar transacciones superiores a un umbral
4. Comparar ventas por categoría y ciudad
5. Crear una vista temporal y consultar con SQL

## 8. Evidencias a entregar
- notebook ejecutado
- capturas o tablas de resultados
- breve conclusión sobre los hallazgos

## 9. Conclusión esperada
El estudiante debe demostrar que entiende el flujo básico de trabajo con Spark:
**leer -> transformar -> analizar -> guardar**