# LifeMap

Reputación, ubicación y supervivencia de negocios con el Yelp Open Dataset.

**Área:** Filadelfia (filtro `state = PA`). **Familia de categorías:** Restaurants, Food.

**URL pública:** https://cfr-progressive-cadillac-pose.trycloudflare.com

Es un túnel rápido de Cloudflare hacia el puerto 5000: cambia si se reinicia. Prueba: `https://cfr-progressive-cadillac-pose.trycloudflare.com/health`.

| Servicio | Papel | Puerto |
|---|---|---|
| MongoDB | Negocios, reseñas y tasas | interno |
| Dask (scheduler y 2 workers) | Ingesta y limpieza | interno |
| Spark (maestro y worker) | Sentimiento y MapReduce | interno |
| API Flask + Gunicorn | Consultas y predicción | 5000 |
| Jenkins | Integración y despliegue | 8088 |

## Levantar el sistema desde cero

Requisitos: Docker con Compose y unos 8 GB de RAM asignables.

```bash
git clone https://github.com/Cristian-Tg/Final-Big-Data.git
cd Final-Big-Data
cp .env.example .env     # definir MONGO_ROOT_PASSWORD, KAGGLE_USERNAME y KAGGLE_KEY
docker compose build
docker compose up -d --wait mongo dask-scheduler dask-worker
docker compose run --rm download
docker compose run --rm ingestion
docker compose stop dask-worker        # libera memoria para Spark
docker compose up -d --wait spark-master spark-worker
docker compose run --rm sentiment
docker compose run --rm closure
docker compose up -d --wait mongo dask-scheduler dask-worker spark-master spark-worker api jenkins
```

Las credenciales nunca se versionan: `.env` y `secrets/` están ignorados por git.

## Jenkins

El job `lifemap` usa este repositorio y el `Jenkinsfile`. Necesita dos credenciales:

- `kaggle-json` (archivo): token de Kaggle.
- `lifemap-mongo-password` (texto): contraseña de MongoDB.

Revisa `main` con pollSCM cada 2 minutos, porque Jenkins no es alcanzable desde GitHub. Si una
prueba falla, la versión no se promueve y sigue en línea la anterior. El parámetro `RUN_DATA`
ejecuta descarga, ingesta, modelo y tasas en el primer build de un servidor nuevo.

## Exponer la API

```bash
cloudflared tunnel --url http://localhost:5000
```

## Mapa

`/mapa` (o la raíz `/`) muestra un mapa de Filadelfia con las zonas coloreadas por tasa de cierre. Al hacer clic se ven los negocios a 800 m, abiertos y cerrados, junto con el resumen de la zona; incluye una caja para clasificar el sentimiento de un texto. Usa los mismos endpoints de la API.

## API

| Método | Ruta | Uso |
|---|---|---|
| GET | `/health` | Estado de los servicios y commit desplegado |
| GET | `/businesses/near?lat&lon&radius_m&category&limit` | Negocios cercanos (`$near`) |
| GET, POST | `/businesses/within` | Negocios en un polígono GeoJSON (`$geoWithin`) |
| GET | `/zones/summary?lat&lon&radius_m&category` | Resumen de zona (`$geoNear`) |
| GET | `/stats/closure?by=category&min_dias&limit` | Tasas de cierre (`by`: category o geohash) |
| POST | `/predict` con `{"text": "..."}` | Sentimiento de un texto |

## Pruebas

```bash
docker compose run --rm --no-deps tests pytest -m "not integration"
docker compose run --rm tests pytest -m integration
```

El informe técnico está en [`docs/informe_tecnico.md`](docs/informe_tecnico.md).
