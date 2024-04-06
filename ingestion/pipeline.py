from ingestion.bigquery import get_bigquery_client, get_bigquery_result, build_pypi_query

def main():
  df = get_bigquery_result(build_pypi_query(), get_bigquery_client('big-data-419421'))
  print(df)
  print("Hello from the pipeline")

if __name__ == '__main__':
  main()