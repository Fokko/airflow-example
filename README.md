# Airflow example

This repository contains a simple dag that shows some of the possibilities of Airflow.

## TODO

After booting up the Airflow stack using docker-compose:
```
docker-compose up
```

Or kill it:
```
docker-compose kill
```

Or remove the instances to create a clean state:
```
docker-compose rm -f
```

You need to set the connections to postgres:
```
Host: postgres
User: airflow
Pass: airflow
```

And replace the rename the master host of Spark from `yarn` to `local[*]`