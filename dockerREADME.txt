docker-compose up -d postgres redis minio

docker-compose up db-sql-jobs

docker-compose up --build minio-init

docker-compose up -d zookeeper kafka

docker-compose up --build kafka-producer-foot-traffic kafka-producer-pos kafka-producer-shelf kafka-producer-near-expiry

docker-compose up --build spark-foot-traffic spark-pos spark-shelf spark-near-expiry spark-alert-orch spark-restock-wh

