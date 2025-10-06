FROM apache/superset:latest

USER root

RUN /app/.venv/bin/python -m ensurepip --upgrade \
    && /app/.venv/bin/pip3 install --no-cache-dir --upgrade pip \
    && /app/.venv/bin/pip3 install --no-cache-dir psycopg2-binary

USER superset
