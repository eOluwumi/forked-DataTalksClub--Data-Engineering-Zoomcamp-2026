FROM python:3.12-slim

WORKDIR /code

# install runtime deps explicitly
RUN pip install --no-cache-dir \
    click \
    pandas \
    sqlalchemy \
    psycopg2-binary \
    pyarrow \
    requests \
    tqdm


COPY ingest_data.py .

ENTRYPOINT ["python", "ingest_data.py"]