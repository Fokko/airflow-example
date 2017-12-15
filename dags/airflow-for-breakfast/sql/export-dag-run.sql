COPY (
  SELECT *
  FROM dag_run
  WHERE DATE(start_date) = '{{ ds }}'
)
TO '/data/{{ ds }}/dag_run.csv' (format csv, delimiter ',', header)
