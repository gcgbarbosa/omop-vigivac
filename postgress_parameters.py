POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5432"  # Default PostgreSQL port
POSTGRES_DB = "omopdb"
POSTGRES_USER = "omop"
POSTGRES_PASSWORD = "ladygaga"

jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
connection_properties = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

