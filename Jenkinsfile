// Pipeline de CI/CD: checkout -> validar -> build -> pruebas unitarias -> stack efimero + pruebas de humo -> despliegue.
// Cada etapa depende de la anterior: si una prueba falla, el pipeline se detiene y NO se despliega nada.
// La imagen que se despliega es exactamente la que paso las pruebas (se promociona bigdata-api:ci a :latest).
//
// Nota sobre Docker: Jenkins usa el daemon del host (socket montado). Por eso las pruebas no usan volumenes con
// rutas del workspace (el daemon no las veria): los archivos viajan con `docker cp`.
pipeline {
    agent any

    options {
        timestamps()
        disableConcurrentBuilds()
        timeout(time: 30, unit: 'MINUTES')
    }

    environment {
        CI_COMPOSE = 'docker-compose.ci.yml'
        SMOKE_NAME = "smoke-${env.BUILD_NUMBER}"
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
                sh 'git log -1 --pretty="Commit: %h %s"'
            }
        }

        stage('Validar compose') {
            environment {
                MONGO_USER = 'validacion'
                MONGO_PASSWORD = 'validacion'
            }
            steps {
                sh 'docker compose config --quiet'
                sh 'docker compose -f "$CI_COMPOSE" config --quiet'
            }
        }

        stage('Build imagenes') {
            steps {
                sh 'docker build -t bigdata-api:ci api'
                sh 'docker build -t bigdata-ingest:ci ingest'
            }
        }

        stage('Pruebas unitarias API') {
            steps {
                sh 'docker run --rm bigdata-api:ci python -m pytest tests -q -p no:cacheprovider'
            }
        }

        stage('Pruebas unitarias ingesta') {
            steps {
                sh '''
                    cid=$(docker create bigdata-ingest:ci python -m pytest tests/unit -q -p no:cacheprovider)
                    docker cp tests "$cid":/app/tests
                    rc=0
                    docker start -a "$cid" || rc=$?
                    docker rm "$cid" > /dev/null
                    exit $rc
                '''
            }
        }

        stage('Stack efimero + pruebas de humo') {
            steps {
                sh '''
                    docker compose -f "$CI_COMPOSE" down -v --remove-orphans || true
                    docker compose -f "$CI_COMPOSE" up -d --wait
                    cid=$(docker create --name "$SMOKE_NAME" --network bigdata-ci_default \
                        -e API_URL=http://api:5000 \
                        -e MONGO_URI="mongodb://ci:ci-only@mongo:27017/?authSource=admin" \
                        -e MONGO_DB=pokemon_ci \
                        bigdata-api:ci python /smoke/smoke_api.py)
                    docker cp tests/smoke "$cid":/smoke
                    docker start -a "$cid"
                '''
            }
        }

        stage('Deploy') {
            // Solo el job de main despliega; el job con parametro BRANCH (pokemon-geo-bigdata-rama) solo prueba.
            when { expression { params.BRANCH == null } }
            steps {
                // Reutiliza las credenciales del MongoDB que ya corre (no se guardan en el repo ni en Jenkins).
                sh '''
                    env_mongo() { docker inspect mongo --format '{{range .Config.Env}}{{println .}}{{end}}' | sed -n "s/^$1=//p"; }
                    export MONGO_USER="$(env_mongo MONGO_INITDB_ROOT_USERNAME)"
                    export MONGO_PASSWORD="$(env_mongo MONGO_INITDB_ROOT_PASSWORD)"
                    [ -n "$MONGO_USER" ] || { echo "No hay un contenedor mongo en ejecucion: levanta el stack primero (docker compose up -d)"; exit 1; }
                    docker tag bigdata-api:ci bigdata-api:latest
                    docker compose -p bigdata-geo up -d --no-deps --no-build --wait api
                    docker exec api python -c "import urllib.request as u; print(u.urlopen('http://localhost:5000/health', timeout=5).read().decode())"
                '''
            }
        }
    }

    post {
        always {
            sh '''
                docker rm -f "$SMOKE_NAME" > /dev/null 2>&1 || true
                docker compose -f "$CI_COMPOSE" down -v --remove-orphans > /dev/null 2>&1 || true
            '''
        }
        success {
            echo 'Pipeline correcto: API desplegada con la imagen que paso todas las pruebas.'
        }
        failure {
            echo 'Pipeline fallido: no se despliega nada (el despliegue es la ultima etapa).'
        }
    }
}
