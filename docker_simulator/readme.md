----------------- DESCRIZIONE CARTELLA ---------------

- docker-compose.yml
Serve a configurare (per ora) i seguenti servizi:
            - zookeeper: Coordinamento per Kafka
            - kafka: Broker di messaggi
            - kafka-ui: Interfaccia web per Kafka (http://localhost:8080) dove vengono mostrati i messaggi
            - simulator: Genera eventi di vendita/trasferimento/spoiler da file Parquet
            - consumer: Consuma e stampa gli eventi Kafka
            - mongodb: devo ancora capirlo :D 

- test_kafka_connection.py
Script di test per verificare la connessione al broker Kafka da locale o da container (deve sempre essere 
da Docker, MAI locale)


simulator/
    - simulate_from_parquet.py:
    legge entrambi i file Parquet (in-store e warehouse), simula vendite e trasferimenti, e invia eventi Kafka.
    gli eventi kafka in questione:
                - sales.events → vendita nello store
                - transfer.events → trasferimento magazzino → store
                - adjustment.events → scarti/spoiler

    - store_inventory.parquet & warehouse_inventory.parquet
    Dataset Parquet con le scorte di negozio e magazzino.

    - wait-for-kafka.sh
    Script che aspetta che Kafka sia attivo prima di eseguire lo script di simulazione (perché altrimenti l'image simulator si blocca)
    Usato nel Dockerfile.

    - Dockerfile
    Costruisce l'immagine Docker per il simulatore Kafka. Include installazione di Python, librerie e script di avvio.

consumer/
    - consume_kafka_events.py
    Consuma messaggi dai topic Kafka e li stampa. Ascolta sugli stessi eventi di simulate_from_parquet.py

    - Dockerfile
    Costruisce l'immagine Docker per il consumer.




---------------- COME AVVIARE IL PROGETTO --------------

- ogni volta  che modifichi il file docker-compose, o costrisci nuove immagini:
docker-compose build

- per avviare il container:
docker-compose up -d

- per verificare che il simulatore stia mandando eventi:
docker-compose logs -f simulator

- per verificare il consumer:
docker-compose logs -f consumer

- per aprire l'interfaccia (dal web) e vedere cosa succede nello specifico:
http://localhost:8080




---------------- DUBBI ------------------
- problema warehouse:
come inserirlo? 
Ci sta che i prodotti in-store calino al secondo, ma non può andare al secondo anche il rifornimento degli scaffali
o il prelevamento dal warehouse.
Forse dovrebbe essere impostato un orario (o 2/3...) al giorno in cui avviene il rifornimento dove serve; 
magari tranne che per gli scaffali dove viene lanciato l'allarme: lì serve qualcuno che li rifornisca subito.

Poi, come funziona invece il rifornimento del warehouse? bisogna creare sistema di allarme anche lì. Però
tenere a mente che non è possibile che si rifornisca immediatamente, in quanto vanno "ordinati" i prodotti.

- come tenere conto dei prodotti che hanno data di scadenza?



