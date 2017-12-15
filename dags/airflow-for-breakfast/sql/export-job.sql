COPY (
  SELECT *
  FROM job
  WHERE DATE(start_date) = '{{ ds }}'
)
TO '/data/{{ ds }}/job.csv' (format csv, delimiter ',', header)