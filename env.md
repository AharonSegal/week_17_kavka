DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3307"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "sql_pw")
DB_NAME = os.getenv("DB_NAME", "test_17")

mongo_uri = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
mongo_db = os.getenv("MONGO_DB", "test_17_mongo")
mongo_collection = os.getenv("MONGO_COLLECTION", "records")

kafka_topic = os.getenv("KAFKA_TOPIC", "raw-records")
kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")

kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
kafka_group_id = os.getenv("KAFKA_GROUP_ID", "test-group")
