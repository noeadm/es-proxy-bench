FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /bench
COPY requirements.txt ./
RUN pip install -r requirements.txt
COPY es_proxy_bench.py ./
ENTRYPOINT ["python", "/bench/es_proxy_bench.py"]
