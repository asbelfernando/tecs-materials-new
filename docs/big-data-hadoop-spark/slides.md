# Presentación: Aplicaciones de Big Data con Hadoop y Spark

## Diapositiva 1. Portada
**Aplicaciones de Big Data con Hadoop y Spark**

Subtítulo:
De la teoría al laboratorio en Databricks

Docente:
[Completar]

Curso:
[Completar]

---

## Diapositiva 2. Objetivos de aprendizaje
Al finalizar esta sesión, podrás:

- comprender qué es Big Data y por qué surge
- describir la arquitectura básica de Hadoop
- explicar cómo funciona Spark a nivel conceptual
- comparar MapReduce y Spark
- identificar aplicaciones reales en la industria
- prepararte para desarrollar un laboratorio en Databricks

---

## Diapositiva 3. Ruta de la sesión
1. Big Data: contexto y conceptos
2. Hadoop y procesamiento distribuido
3. Apache Spark
4. Aplicaciones empresariales
5. Buenas prácticas
6. Puente hacia laboratorio

---

## Diapositiva 4. Motivación
Las organizaciones generan grandes cantidades de datos desde:
- transacciones
- sensores
- aplicaciones móviles
- redes sociales
- plataformas web
- dispositivos IoT

Pregunta detonadora:
**¿Qué ocurre cuando una base de datos tradicional ya no es suficiente?**

---

## Diapositiva 5. ¿Qué es Big Data?
Big Data se refiere a datos que, por su tamaño, velocidad y complejidad, requieren tecnologías distintas a las tradicionales para su almacenamiento, procesamiento y análisis.

Idea central:
**No se trata solo de mucho volumen, sino de la necesidad de nuevas arquitecturas.**

---

## Diapositiva 6. Las 5V de Big Data
- **Volumen**
- **Velocidad**
- **Variedad**
- **Veracidad**
- **Valor**

Mensaje clave:
Los proyectos de Big Data no tienen éxito por almacenar más, sino por convertir datos en decisiones.

---

## Diapositiva 7. Ejemplos reales
- Netflix: recomendación de contenido
- Amazon: sugerencia de productos
- Bancos: detección de fraude
- Salud: monitoreo y análisis predictivo
- Transporte: optimización de rutas
- Retail: análisis de comportamiento de compra

---

## Diapositiva 8. ¿Por qué no basta con sistemas tradicionales?
Limitaciones comunes:
- poca escalabilidad vertical
- altos costos al crecer
- bajo rendimiento con grandes volúmenes
- dificultad con datos semiestructurados o no estructurados
- tiempos de procesamiento largos

---

## Diapositiva 9. Computación distribuida
La computación distribuida divide el trabajo entre varios nodos para:
- almacenar grandes volúmenes
- procesar en paralelo
- tolerar fallos
- reducir tiempos de ejecución

Concepto clave:
**Escalabilidad horizontal**

---

## Diapositiva 10. Introducción a Hadoop
Hadoop es un ecosistema orientado al almacenamiento y procesamiento distribuido de datos a gran escala.

Pilares:
- HDFS
- MapReduce
- YARN

---

## Diapositiva 11. HDFS
HDFS significa **Hadoop Distributed File System**.

Características:
- divide archivos grandes en bloques
- distribuye bloques en múltiples nodos
- replica datos para tolerancia a fallos
- opera sobre clústeres de hardware commodity

---

## Diapositiva 12. Componentes de HDFS
- **NameNode:** administra metadatos
- **DataNodes:** almacenan bloques
- **Client:** interactúa con el sistema

Explicación docente:
El NameNode sabe dónde está cada bloque; los DataNodes guardan el contenido.

---

## Diapositiva 13. ¿Cómo se almacena un archivo?
1. Un archivo grande se divide en bloques
2. Los bloques se reparten entre nodos
3. Cada bloque se replica
4. Si un nodo falla, otro conserva la réplica

---

## Diapositiva 14. Beneficios de HDFS
- alta disponibilidad
- tolerancia a fallos
- escalabilidad
- procesamiento cercano a los datos
- soporte para volúmenes masivos

---

## Diapositiva 15. MapReduce
MapReduce es un modelo de programación distribuida usado para procesar grandes conjuntos de datos.

Fases:
- Map
- Shuffle/Sort
- Reduce

---

## Diapositiva 16. Lógica de Map
La fase **Map**:
- recibe datos de entrada
- transforma registros
- emite pares clave-valor

Ejemplo:
`("mouse", 1)`

---

## Diapositiva 17. Lógica de Reduce
La fase **Reduce**:
- agrupa por clave
- consolida resultados
- calcula agregados

Ejemplo:
`("mouse", 1250)`

---

## Diapositiva 18. Ejemplo clásico
**Conteo de palabras**

Entrada:
documentos de texto

Map:
genera `(palabra, 1)`

Reduce:
suma repeticiones por palabra

---

## Diapositiva 19. Limitaciones de MapReduce
- fuerte dependencia de disco
- mayor latencia
- menos flexible para procesos iterativos
- desarrollo más complejo
- menor productividad para analítica moderna

---

## Diapositiva 20. Nace Apache Spark
Spark surge para ofrecer:
- mayor velocidad
- procesamiento en memoria
- APIs más amigables
- soporte para múltiples cargas analíticas

---

## Diapositiva 21. ¿Qué es Spark?
Apache Spark es un motor de procesamiento distribuido diseñado para análisis de datos a gran escala con foco en rendimiento y simplicidad de desarrollo.

---

## Diapositiva 22. Componentes de Spark
- Spark Core
- Spark SQL
- Structured Streaming
- MLlib
- GraphX

Mensaje docente:
Spark no es solo procesamiento batch; también cubre SQL, streaming y machine learning.

---

## Diapositiva 23. Arquitectura básica de Spark
- **Driver**
- **Executors**
- **Cluster Manager**

Explicación:
- el driver coordina
- los executors ejecutan tareas
- el cluster manager asigna recursos

---

## Diapositiva 24. RDD, DataFrame y Dataset
### RDD
- más bajo nivel
- más control

### DataFrame
- tabular
- optimizado
- más productivo

### Dataset
- más usado en Scala/Java

En Databricks y PySpark normalmente trabajaremos mucho con **DataFrames**.

---

## Diapositiva 25. Lazy Evaluation
Spark no ejecuta inmediatamente cada transformación.

Primero construye un plan lógico y luego lo ejecuta cuando encuentra una acción como:
- `show()`
- `count()`
- `write()`

Ventaja:
mejor optimización.

---

## Diapositiva 26. Operaciones en Spark
### Transformaciones
- `select`
- `filter`
- `withColumn`
- `groupBy`

### Acciones
- `show`
- `count`
- `collect`
- `write`

---

## Diapositiva 27. Hadoop vs Spark
### Hadoop MapReduce
- más orientado a disco
- robusto para ciertos lotes grandes
- arquitectura histórica del ecosistema

### Spark
- generalmente más rápido
- apto para análisis iterativo
- ideal para notebooks y ciencia de datos

---

## Diapositiva 28. ¿Son excluyentes?
No.

Escenario común:
- Hadoop/HDFS como almacenamiento
- Spark como motor de procesamiento

Esto permite combinar capacidades en una misma arquitectura.

---

## Diapositiva 29. Casos de uso empresariales
- ETL masivo
- data lake analytics
- detección de anomalías
- análisis de logs
- analítica de clientes
- entrenamiento de modelos
- reporting distribuido

---

## Diapositiva 30. Caso aplicado: retail
Problema:
Una empresa quiere analizar millones de transacciones para saber:
- categorías top
- ciudades con más ventas
- productos más vendidos
- ticket promedio

Solución:
procesamiento distribuido con Spark.

---

## Diapositiva 31. Caso aplicado: banca
- detección de fraude
- monitoreo transaccional
- alertas por comportamiento anómalo
- modelos de riesgo

---

## Diapositiva 32. Caso aplicado: IoT
- millones de eventos por sensores
- procesamiento de series temporales
- mantenimiento predictivo
- observabilidad operativa

---

## Diapositiva 33. Databricks en este contexto
Databricks facilita:
- notebooks colaborativos
- ejecución de PySpark
- administración de clústeres
- integración con almacenamiento
- visualización rápida

---

## Diapositiva 34. Buenas prácticas
- definir esquema en lugar de depender siempre de inferencia
- controlar nulos y calidad del dato
- usar formatos eficientes
- evitar transformaciones innecesarias
- monitorear particiones y rendimiento
- documentar notebooks

---

## Diapositiva 35. Riesgos frecuentes
- datos duplicados
- skew de datos
- costos por mala configuración
- pipelines difíciles de mantener
- dependencia excesiva de notebooks no documentados

---

## Diapositiva 36. Pregunta al grupo
**Si tuvieras 500 millones de registros de ventas, qué parte resolverías con almacenamiento distribuido y qué parte con procesamiento distribuido?**

---

## Diapositiva 37. Puente al laboratorio
En el laboratorio haremos:
1. carga de un CSV en Databricks
2. limpieza básica
3. creación de columnas derivadas
4. agregaciones
5. guardado de resultados

---

## Diapositiva 38. Resumen final
- Big Data responde a un problema de escala y complejidad
- Hadoop fue clave en el almacenamiento y procesamiento distribuido
- Spark aceleró y simplificó el análisis
- Databricks facilita la práctica aplicada

---

## Diapositiva 39. Preguntas
Espacio para dudas, discusión y preparación del laboratorio.

---

## Diapositiva 40. Cierre
**Gracias**
Siguiente bloque:
**Laboratorio en Databricks con PySpark**