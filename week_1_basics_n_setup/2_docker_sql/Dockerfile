FROM python:3.9

RUN pip install pandas pyarrow fastparquet sqlalchemy psycopg2 wget

WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]