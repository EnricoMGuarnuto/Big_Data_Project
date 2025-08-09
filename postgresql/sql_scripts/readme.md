## dubbi 

1. nella tabella receipt, total_net, total_tax e total_gross devono essere comunicanti con la tabella dei prezzi (quindi prima fare prezzi!!!)
2. nella tabella line_receipt:
line_discount     numeric(12,2) not null default 0,        -- sconto riga (+ = sconto)
come si acquisisce questo line_discount? dalla tabella di enrico???????
3. register_id: immaginiamo di avere 5 casse, per esempio. forse il register id va messo già nelle pos_transaction producer.