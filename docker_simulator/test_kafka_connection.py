from kafka import KafkaAdminClient
from kafka.errors import KafkaError

try:
    admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
    topics = admin.list_topics()
    print("✅ Connessione a Kafka riuscita. Topic disponibili:")
    print(topics)
except KafkaError as e:
    print("❌ Errore nella connessione a Kafka:")
    print(e)
