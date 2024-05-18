Generate fernet token

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Initialize airflow UI

```bash
docker-compose run airflow airflow db init
```

```bash
docker-compose run airflow airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

Build

```bash
docker-compose build
```

Launch

```
docker-compose up -d
```

```
docker-compose run -d airflow airflow scheduler
```
