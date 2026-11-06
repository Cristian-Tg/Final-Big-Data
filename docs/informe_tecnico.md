# LifeMap: informe técnico de la Entrega 1

Cristian Troncoso Guerra. Área: Filadelfia (`state = PA`). Familia: Restaurants, Food.

## 1. Arquitectura

Un solo `docker-compose.yml` levanta MongoDB, Dask (scheduler y dos workers), Spark (maestro y un worker), la API Flask y Jenkins. Los trabajos por lotes (descarga, ingesta, sentimiento y tasas) corren con `docker compose run`. Los servicios se comunican por nombre en la red de Compose y los datos viven en volúmenes nombrados.

El flujo es: Kaggle → Dask (limpieza, Parquet y carga a MongoDB) → Spark (modelo y tasas) → API. `/predict` no necesita una JVM: los coeficientes de la regresión se exportan a un archivo NumPy y la API reproduce el mismo preprocesamiento. En 300 reseñas de muestra coincidió con Spark en las 300 etiquetas, con diferencia de probabilidad 0,0.

Jenkins revisa `main` con pollSCM cada 2 minutos. El pipeline construye imágenes, corre pruebas unitarias, levanta servicios, corre pruebas contra la API y despliega comprobando que `/health` informe el commit. Se reutilizaron la base de Docker, Jenkins y utilidades geoespaciales del trabajo geoespacial.

## 2. Limpieza

Dask lee el archivo por tramos de 32 MB. Cada tarea filtra mientras lee: la primera versión retenía todo el texto y los workers superaban su límite de memoria, lo que obligaba a repetir trabajo (más de 80 minutos sin terminar). Con el filtro, la ingesta tarda 10 min 46 s y ningún worker se reinicia.

| Negocios | Filas | Regla |
|---|---:|---|
| Leídos | 150.346 | Archivo completo |
| En el área | 34.039 | Conserva el estado definido |
| En la familia | 15.427 | Categoría Restaurants o Food |
| Coordenadas válidas | 15.427 | Requisito de GeoJSON |
| Sin duplicados | 15.427 | Un documento por `business_id` |
| Con reseñas | 15.427 | Sin reseñas no hay vida calculable |

| Reseñas | Filas | Regla |
|---|---:|---|
| Leídas | 6.990.280 | Se procesó el archivo completo |
| De negocios válidos | 1.186.343 | Evita reseñas huérfanas |
| Con texto | 1.186.343 | El modelo necesita texto |
| Fecha y estrellas válidas | 1.186.343 | Requeridas para vida y etiqueta |
| Sin duplicados | 1.186.343 | `review_id` único |

Las reglas posteriores no eliminaron filas, pero quedan activas para detectar datos defectuosos. El filtro usa el estado y no la ciudad, de modo que incluye municipios vecinos de Filadelfia.

## 3. Vida de los negocios

La vida va desde la primera hasta la última reseña; se cuenta al menos 1 día. El evento es `is_open = 0`; los negocios abiertos quedan censurados. Fecha de corte: 2022-01-19, la última reseña del área.

Resultado: 15.427 negocios, 5.266 cierres, 10.161 censurados y 39.315.270 días-negocio.

**Limitaciones.** El dataset no trae fechas reales de apertura ni cierre. La primera reseña ocurre después de abrir y la última antes de cerrar, por lo que la vida se subestima y las tasas tienden a sobreestimarse. `is_open` tampoco dice cuándo cerró un negocio. La fuente son negocios con reseñas en Yelp, no todo el comercio del área.

## 4. Modelo de sentimiento

Etiquetas: 1-2 estrellas negativo, 3 neutral, 4-5 positivo. Distribución: 791.005 positivas, 245.998 negativas y 149.340 neutrales. Se usó RegexTokenizer, StopWordsRemover, HashingTF (2^16) e IDF.

El repositorio SentimentStream no estaba disponible, por lo que su pipeline descrito **se reimplementó**. La partición es por negocio (0 negocios en ambos lados). Por memoria, el entrenamiento usa una muestra del 35 %: 330.450 reseñas de entrenamiento y 242.871 de prueba.

| Modelo | Accuracy | F1 macro |
|---|---:|---:|
| Naive Bayes | 0,762 | 0,666 |
| Regresión logística (guardado) | 0,822 | 0,682 |

| Clase | NB precision | NB recall | NB F1 | RL precision | RL recall | RL F1 |
|---|---:|---:|---:|---:|---:|---:|
| Positivo | 0,920 | 0,823 | 0,869 | 0,876 | 0,941 | 0,907 |
| Negativo | 0,718 | 0,719 | 0,718 | 0,793 | 0,774 | 0,783 |
| Neutral | 0,340 | 0,522 | 0,412 | 0,446 | 0,295 | 0,355 |

Matriz de confusión de regresión logística (filas reales; columnas predichas):

| Real | Positivo | Negativo | Neutral |
|---|---:|---:|---:|
| Positivo | 150.261 | 3.203 | 6.219 |
| Negativo | 6.475 | 40.131 | 5.246 |
| Neutral | 14.797 | 7.298 | 9.241 |

Matriz de confusión de Naive Bayes:

| Real | Positivo | Negativo | Neutral |
|---|---:|---:|---:|
| Positivo | 131.412 | 8.206 | 20.065 |
| Negativo | 2.896 | 37.268 | 11.688 |
| Neutral | 8.518 | 6.451 | 16.367 |

Se guardó la regresión por su mejor F1 macro. Su debilidad es la clase neutral: solo recupera el 29 %, porque es minoritaria y su texto mezcla elogios y quejas. Naive Bayes la recupera mejor a costa de precisión.

## 5. Tasas de cierre

`tasa = cierres / (días-negocio / 365) × 100`. Los totales salen de `reduceByKey` sobre `(clave, (cierre, días_negocio))` y el trabajo falla si no coinciden con un `groupBy`.

El mínimo es 36.500 días-negocio (100 años-negocio): por debajo, uno o dos cierres mueven la tasa varios puntos. Lo cumplen 113 de 355 categorías y 150 de 248 geohash. La tasa global es 4,89 %.

| Categoría | Cierres | Días-negocio | Tasa |
|---|---:|---:|---:|
| Buffets | 35 | 94.225 | 13,56 % |
| Indian | 79 | 235.970 | 12,22 % |
| Cajun/Creole | 18 | 55.753 | 11,78 % |
| Middle Eastern | 27 | 101.274 | 9,73 % |
| Bubble Tea | 16 | 61.542 | 9,49 % |

| Geohash | Cierres | Días-negocio | Tasa | % reseñas negativas |
|---|---:|---:|---:|---:|
| dr4kr | 14 | 58.310 | 8,76 % | 30,0 |
| dr4k5 | 10 | 44.299 | 8,24 % | 33,2 |
| dr4sw | 8 | 37.261 | 7,84 % | 34,6 |

Son asociaciones descriptivas, no causas. Con pocos cierres, como los 8 de `dr4sw`, las diferencias pequeñas no deben sobreinterpretarse.

## 6. Balanceo de carga

El MapReduce por categoría se midió con 3 repeticiones sobre 15.427 negocios, en caché:

| Particiones | Rep. 1 (s) | Rep. 2 (s) | Rep. 3 (s) | Media (s) | Desv. (s) |
|---:|---:|---:|---:|---:|---:|
| 2 | 0,500 | 0,430 | 0,489 | 0,473 | 0,038 |
| 8 | 1,359 | 1,489 | 1,408 | 1,419 | 0,066 |
| 32 | 4,446 | 4,076 | 3,831 | 4,118 | 0,310 |
| 200 | 24,100 | 22,761 | 22,273 | 23,045 | 0,946 |

El tiempo crece con las particiones porque el volumen es pequeño y el costo de coordinar tareas domina. En un solo worker de 3 núcleos, 200 particiones no aportan paralelismo.

El sesgo se midió con las reseñas en 8 particiones:

| Reparto | Mínimo | Máximo | Máximo / media |
|---|---:|---:|---:|
| Por ciudad | 35.418 | 796.896 | 5,37 |
| Por categoría | 34.907 | 239.889 | 1,62 |
| Sin clave | 148.291 | 148.294 | 1,00 |

Con clave, las filas de un valor van a la misma partición: Filadelfia concentra el 67 % en una sola y esa tarea marcaría el tiempo total. Sin clave el reparto es parejo, pero no sirve para agrupar sin un shuffle.

## 7. Estado del despliegue

La API está publicada en https://others-immunology-construct-christopher.trycloudflare.com mediante un túnel de Cloudflare. Los seis endpoints y sus validaciones se probaron por esa URL, y `/health` informa cada servicio y el commit desplegado.

**Incumplimientos conocidos:** el servidor es el mismo portátil de desarrollo, porque no se dispuso de otro equipo ni de una nube sin costo, y la URL no estuvo activa las 48 horas previas a la entrega. Además, un clon limpio se validó construyendo las imágenes y corriendo las pruebas unitarias, pero la reconstrucción completa no se ejecutó en otro equipo.

Jenkins detecta cambios con pollSCM. En el build 9, una prueba rota a propósito detuvo el pipeline y la versión anterior siguió en línea, con su commit correcto en `/health`.
