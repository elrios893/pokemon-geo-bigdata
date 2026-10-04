// Pipeline minimo (Fase 1): checkout + build de imagenes.
// Se ampliara en la Fase 5: pytest, levantar servicios, pruebas smoke y despliegue.
pipeline {
    agent any

    options {
        timestamps()
        disableConcurrentBuilds()
    }

    environment {
        COMPOSE_PROJECT_NAME = 'bigdata-ci'
        // Credenciales de prueba para el build; las reales viven en Jenkins (Credentials).
        MONGO_USER     = 'ci'
        MONGO_PASSWORD = 'ci-only'
        MONGO_DB       = 'pokemon'
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Validar compose') {
            steps {
                sh 'docker compose config --quiet'
            }
        }

        stage('Build imagenes') {
            steps {
                sh 'docker compose build dask-scheduler api'
            }
        }
    }

    post {
        failure {
            echo 'Pipeline fallido: no se despliega nada.'
        }
    }
}
