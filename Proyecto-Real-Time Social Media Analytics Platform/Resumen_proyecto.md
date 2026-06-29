# Real-Time Social Media Analytics Platform

**Objectivo:** Construir una plataforma de analítica en tiempo real que ingiera, procese, enriquezca, almacene y analice flujos continuos de interacciones a alta velocidad. [cite: 16]

### 🔍 Retos
* **Gestión de velocidad:** Gestionar datos en streaming a alta velocidad superando los 1.000 eventos por minuto de forma continua. 
* **Modelado de tráfico:** Simular el comportamiento orgánico de los usuarios junto con picos repentinos e impredecibles de tráfico viral. 
* **Garantía de calidad:** Garantizar la limpieza y validación de datos en tiempo real para evitar que registros malformados corrompan las métricas analíticas finales. 
* **Optimización de recursos:** Optimizar agregaciones complejas en ventanas de tiempo y cálculos analíticos sin sobrecargar los recursos de cómputo. 

### 🛠️ Proceso de desarrollo
* **Ingesta de datos:** Arquitecturé un simulador de eventos de alta concurrencia en Python para enrutar payloads JSON estrictamente estructurados hacia topics de Apache Kafka.
* **Procesamiento de streams:** Desarrollé un pipeline distribuido con PySpark Structured Streaming para consumir micro-batches cada 5 segundos, filtrar registros inválidos y separar las dimensiones temporales para un uso analítico limpio. 
* **Modelado y almacenamiento de datos:** Estructuré una arquitectura Medallion multicapa dentro de Snowflake (RAW, CURATED y ANALYTICS) utilizando Dynamic Tables para automatizar el enriquecimiento y la transformación de datos nativos en la nube.
* **Ingeniería analítica:** Diseñé modelos de puntuación en tiempo real y lógicas automatizadas para calcular hashtags en tendencia en ventanas móviles de 1, 5 y 15 minutos, capturar umbrales de contenido viral y agregar perfiles de sentimiento en los comentarios. 
* **Visualización de datos:** Creé un panel interactivo de engagement en Power BI para entregar insights de negocio inmediatos, rankings de influencers y alertas de contenido viral.

### 📊 Conclusiones
* **Pipeline End-to-End:** Desarrollé un pipeline end-to-end altamente escalable capaz de procesar streaming de Big Data, desde la generación de la interacción hasta el consumo visual ejecutivo final sin interrupciones.
* **Eficiencia e Innovación Cloud:** Demostré aplicaciones prácticas de ingeniería con características avanzadas de la nube como las Dynamic Tables de Snowflake, reduciendo la latencia en la transformación de datos y los costes de infraestructura. 
 **Resiliencia Demostrada:** Probé la resiliencia del sistema frente a picos de tráfico, asegurando que los cálculos basados en ventanas complejas y las alertas se activaran instantáneamente durante las fases de simulación viral activa. 
* **Skills:** Procesamiento de Streams, Arquitectura de Big Data, Ingeniería Analítica, Calidad de Datos. 
* **Tech Stack:** Python, Apache Kafka, PySpark, Snowflake, Power BI, Docker.