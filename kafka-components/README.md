Producers → Kafka (solo!)

Consumers → MinIO (raw) e/o PostgreSQL (strutturato) e/o altre destinazioni

Il pattern “Kafka → MinIO raw” è perfetto per backup, audit e batch processing.
Il pattern “Kafka → PostgreSQL” è perfetto per dashboard, API, ML serving.

1. Eventi in tempo reale:
Foot traffic
Producer: genera evento ingresso cliente → foot_traffic topic Kafka

Consumers:

Consumer 1: Kafka → MinIO (raw, append JSON, audit/replay/analytics)

Consumer 2 (opzionale): Kafka → PostgreSQL (foot_traffic_events) se vuoi servire insight real-time/dash

(Spark): Può consumare direttamente dal topic per analisi real-time su foot traffic, oppure leggere periodicamente dal raw in MinIO per batch analytics

Shelf sensors
Producer: evento weight change/pickup/replace → shelf_sensors topic Kafka

Consumers:

Consumer 1: Kafka → MinIO (raw)

Consumer 2: Kafka → PostgreSQL (shelf_events)

Business logic:

La logica di decremento stock/aggiornamenti avviene periodicamente in PostgreSQL tramite gli script SQL già visti, non nel consumer (meglio per flessibilità e audit).

Spark:

Puoi usare Spark Structured Streaming per analisi aggregate in tempo reale (tassi di prelievo, anomalie, previsioni, alert “spike”)

Spark può anche leggere dal raw MinIO e produrre dataset cleansed/aggregati (cleansed layer)

POS transactions
Producer: evento transazione cassa → pos_transactions topic Kafka

Consumers:

Consumer 1: Kafka → MinIO (raw)

Consumer 2: Kafka → PostgreSQL (tabella di staging, poi update su prodotti e sales_history via SQL periodici)

Business logic:

Gli script SQL periodici aggiornano stock e storicizzano le vendite

Spark:

Spark può fare feature engineering, predizione domanda, batch analytics sulle vendite (es. trend settimanali, heatmap orarie, ecc.)

2. Pipeline completa (step-by-step)
Apertura negozio / primi eventi
Producer foot traffic → Kafka →

Consumer MinIO (append in /raw/foot_traffic/…)

(opzionale: consumer PostgreSQL → per insight serving/dash)

Producer shelf sensors → Kafka →

Consumer MinIO (append /raw/shelf_sensors/…)

Consumer PostgreSQL (shelf_events)

Nessun decremento stock automatico qui:
lo fa il job SQL periodico se un pickup resta pending > 1 min

Producer POS → Kafka →

Consumer MinIO (append /raw/pos_transactions/…)

Consumer PostgreSQL (staging, poi update stock con job SQL)

Job periodici
SQL update jobs (come già configurato, eseguiti ogni minuto o X sec):

Gestiscono decremento stock scaffale, refill, sales, refill warehouse, alert

Spark entra in azione:
Cleansing/Aggregation layer:

Spark batch/streaming consuma da MinIO raw

Elabora, aggrega, pulisce i dati

Scrive in MinIO cleansed/aggregated (/cleansed/…) e/o aggiorna PostgreSQL se vuoi dataset “serving ready” (ad es. per Streamlit)

Può anche generare previsioni di domanda, insight periodici (ML jobs)

Advanced Analytics:

Spark può scrivere feature set, training set, forecast direttamente in MinIO (per ML) o su PostgreSQL (per dash/API)

ML pipeline:

Spark può triggerare retrain/forecast in batch, scrivere output in MinIO/MLflow

Recap: schema pipeline produttivo
Data Generation (producers) → Kafka topics

Raw consumers

Kafka → MinIO (per ogni stream)

Serving consumers

Kafka → PostgreSQL (per ogni stream)

Job SQL periodici

Solo in PostgreSQL, nessuna logica business “hardcoded” nei consumer

Spark jobs

Consuma raw/cleansed da MinIO

Produce aggregati, feature, forecast, batch insights (sul cleansed layer MinIO/postgre)

ML/serving layer

Spark/MLflow su MinIO/ML models, FastAPI/Streamlit su PostgreSQL/MinIO