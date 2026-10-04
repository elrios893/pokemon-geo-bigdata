FROM python:3.11-slim

WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
# Las pruebas viajan en la imagen para que el pipeline las ejecute igual en cualquier maquina:
#   docker run --rm bigdata-api:latest python -m pytest tests -q
COPY tests ./tests

EXPOSE 5000
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "2", "app:create_app()"]
