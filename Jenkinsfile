// Checkout -> imagenes -> pytest -> servicios -> pruebas contra la API -> despliegue.
// Si una prueba falla no se promueve la version y sigue en linea la anterior.
pipeline {
  agent any

  options {
    timestamps()
    timeout(time: 180, unit: 'MINUTES')
    disableConcurrentBuilds()
    buildDiscarder(logRotator(numToKeepStr: '20'))
  }

  // Jenkins no siempre es alcanzable desde GitHub: pollSCM cada 2 minutos
  triggers {
    githubPush()
    pollSCM('H/2 * * * *')
  }

  parameters {
    booleanParam(name: 'RUN_DATA', defaultValue: false,
                 description: 'Descarga, ingesta, modelo y tasas. Activar en el primer build de un servidor')
  }

  environment {
    COMPOSE_PROJECT_NAME = 'lifemap'
    SERVICIOS = 'mongo dask-scheduler dask-worker spark-master spark-worker api'
  }

  stages {
    stage('Checkout') {
      steps {
        checkout scm
        script { env.GIT_COMMIT_HASH = sh(script: 'git rev-parse HEAD', returnStdout: true).trim() }
        sh '''
          if git ls-files | grep -Ei '(^|/)(kaggle\\.json|\\.env)$'; then
            echo "Hay secretos versionados"; exit 1
          fi
        '''
      }
    }

    stage('Preparar entorno') {
      steps {
        withCredentials([string(credentialsId: 'mongo-root-password', variable: 'MONGO_PWD')]) {
          sh '''
            set +x
            printf 'MONGO_ROOT_USER=lifemap\\nMONGO_ROOT_PASSWORD=%s\\nGIT_COMMIT=%s\\n' \
              "$MONGO_PWD" "$GIT_COMMIT_HASH" > .env
          '''
        }
      }
    }

    stage('Construir imagenes') {
      steps {
        sh 'docker compose build dask-scheduler spark-master api tests'
      }
    }

    stage('Pruebas unitarias') {
      steps {
        sh 'docker compose run --rm --no-deps tests pytest -m "not integration" -v'
      }
    }

    stage('Levantar servicios') {
      steps {
        sh 'docker compose up -d --wait --wait-timeout 300 $SERVICIOS'
      }
    }

    stage('Datos y modelos') {
      when { expression { params.RUN_DATA } }
      steps {
        withCredentials([file(credentialsId: 'kaggle-json', variable: 'KAGGLE_JSON_FILE')]) {
          sh '''
            set +x
            KAGGLE_USERNAME=$(jq -r .username "$KAGGLE_JSON_FILE") \
            KAGGLE_KEY=$(jq -r .key "$KAGGLE_JSON_FILE") \
            docker compose run --rm download
          '''
        }
        sh '''
          docker compose run --rm ingestion
          docker compose run --rm sentiment
          docker compose run --rm closure
        '''
      }
    }

    stage('Pruebas contra la API') {
      steps {
        sh 'docker compose run --rm tests pytest -m integration -v'
      }
    }

    stage('Desplegar') {
      steps {
        sh '''
          for img in dask spark api; do docker tag lifemap/$img:latest lifemap/$img:stable; done
          docker compose up -d --wait --wait-timeout 300 $SERVICIOS
          docker compose exec -T api curl -fsS http://localhost:5000/health | tee health.json
          grep -q "$GIT_COMMIT_HASH" health.json
        '''
      }
    }
  }

  post {
    failure {
      sh '''
        if docker image inspect lifemap/api:stable >/dev/null 2>&1; then
          for img in dask spark api; do docker tag lifemap/$img:stable lifemap/$img:latest; done
          docker compose up -d $SERVICIOS || true
        fi
      '''
    }
    cleanup {
      sh 'rm -f .env health.json'
    }
  }
}
