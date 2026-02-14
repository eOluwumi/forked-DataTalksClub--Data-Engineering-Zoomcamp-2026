#!/usr/bin/env python
#!/usr/bin/env python
"""Parameterized ingestion script for NYC Yellow Taxi data.

Usage: python ingest_data.py --help

This script downloads a compressed CSV for the given year/month and writes
it into a Postgres table using SQLAlchemy. It is parameterized with Click.
"""

from typing import Dict, List

import click
import pandas as pd
from tqdm.auto import tqdm
from sqlalchemy import create_engine


DEFAULT_DTYPE: Dict[str, str] = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
}

DEFAULT_PARSE_DATES: List[str] = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]


def build_url(year: int, month: int) -> str:
    prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
    filename = f"yellow_tripdata_{year:04d}-{month:02d}.csv.gz"
    return f"{prefix}/{filename}"


def ingest(
    pg_user: str,
    pg_pass: str,
    pg_host: str,
    pg_port: str,
    pg_db: str,
    year: int,
    month: int,
    target_table: str,
    chunksize: int,
    dtype: Dict[str, str] = DEFAULT_DTYPE,
    parse_dates: List[str] = DEFAULT_PARSE_DATES,
) -> None:
    url = build_url(year, month)
    engine = create_engine(f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize,
    )

    for i, chunk in enumerate(tqdm(df_iter, desc="ingesting", unit="chunk")):
        if i == 0:
            chunk.head(0).to_sql(name=target_table, con=engine, if_exists="replace", index=False)
        chunk.to_sql(name=target_table, con=engine, if_exists="append", index=False)


@click.command()
@click.option("--pg-user", default="root", show_default=True, help="Postgres user")
@click.option("--pg-pass", default="root", show_default=True, help="Postgres password")
@click.option("--pg-host", default="localhost", show_default=True, help="Postgres host")
@click.option("--pg-port", default="5432", show_default=True, help="Postgres port")
@click.option("--pg-db", default="ny_taxi", show_default=True, help="Postgres database name")
@click.option("--year", default=2021, show_default=True, help="Data year")
@click.option("--month", default=1, show_default=True, help="Data month (1-12)")
@click.option("--target-table", default="yellow_taxi_data", show_default=True, help="Target table name")
@click.option("--chunksize", default=100000, show_default=True, help="CSV read chunksize")

def main(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table, chunksize):
    """CLI entry point for ingestion."""
    ingest(
        pg_user=pg_user,
        pg_pass=pg_pass,
        pg_host=pg_host,
        pg_port=pg_port,
        pg_db=pg_db,
        year=year,
        month=month,
        target_table=target_table,
        chunksize=chunksize,
    )


if __name__ == "__main__":
    main()