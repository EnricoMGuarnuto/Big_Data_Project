## dubbi 

1. nella tabella receipt, total_net, total_tax e total_gross devono essere comunicanti con la tabella dei prezzi (quindi prima fare prezzi!!!)
2. nella tabella line_receipt:
line_discount     numeric(12,2) not null default 0,        -- sconto riga (+ = sconto)
come si acquisisce questo line_discount? dalla tabella di enrico???????
3. register_id: immaginiamo di avere 5 casse, per esempio. forse il register id va messo già nelle pos_transaction producer.

## Spiegazione nuove tabelle batches:

1. staging_batches
Scopo: area di atterraggio temporanea per i CSV (store e warehouse).
Perché è utile: isoli il formato “grezzo” dei file dal modello dati “buono”. Qui importi rapidamente con COPY, poi normalizzi.
2. items (e stessa cosa location)
Scopo: anagrafica unica degli articoli (SKU).
Perché è utile: centralizzi il dizionario articoli e puoi agganciare a un item più lotti nel tempo.
3. batches
Scopo: master dei lotti. Qui assegni un batch interno (PK batch_id) a ogni combinazione di articolo + batch_code.
Importante: non referenziare qui la tabella di staging. batches è la verità “pulita” e duratura; la staging la svuoti.
4. batch_inventory
Scopo: quantità corrente per ogni (lotto, location).
Perché è utile: è la foto aggiornata dello stock, granulare a livello di lotto, essenziale per logistica, picking FIFO/FEFO, recall, e alert scorte.
Chiave: PK composta (batch_id, location_id).
Uso: la popoli/aggiorni aggregando i dati di staging oppure, in sistemi più evoluti, derivandola dai movimenti (entrate/uscite/trasferimenti/scarti).

Come interagiscono
Import: carichi entrambi i CSV in staging_batches.

Normalize articoli: inserisci in items tutti i shelf_id nuovi.

Master lotti: per ogni (shelf_id, batch_code) crei/aggiorni una riga in batches (col relativo item_id).

Inventario: sommi le quantità per (batch, location) e aggiorni batch_inventory.

Alert:

Scadenza: query su batches.expiry_date (es. entro 30 giorni) + join a batch_inventory per quantità residue.

Scorte basse: sum quantità per articolo/location su batch_inventory con soglie.

