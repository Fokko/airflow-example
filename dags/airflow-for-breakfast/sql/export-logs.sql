COPY (
  SELECT *
  FROM log
  WHERE DATE(dttm) = '{{ ds }}'
)
TO '/data/{{ ds }}/logs.csv' (format csv, delimiter ',', header)