FROM python:3.12-slim

RUN useradd -m app

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY tfl_ingest.py .

USER app

ENTRYPOINT ["python", "tfl_ingest.py"]
