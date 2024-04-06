from ingestion.bigquery import (
    get_bigquery_client,
    get_bigquery_result,
    build_pypi_query,
)

from ingestion.models import PypiJobParameters
import fire


def main(params: PypiJobParameters):
    # project name: 'big-data-419421'
    df = get_bigquery_result(
        build_pypi_query(params), get_bigquery_client(params.gcp_project)
    )
    print(df)
    print("Hello from the pipeline")


if __name__ == "__main__":
  fire.Fire(lambda **kwargs: main(PypiJobParameters(**kwargs)))
