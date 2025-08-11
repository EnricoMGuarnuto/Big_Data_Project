-- Popolazione delle tabelle

-- 1) items
insert into items (shelf_id)
select distinct shelf_id
from staging_batches
where shelf_id is not null
on conflict (shelf_id) do nothing;

-- 2) locations
insert into locations (location)
select distinct location
from staging_batches
where location is not null
on conflict (location) do nothing;

-- 3) batches (master)
insert into batches (item_id, batch_code, received_date, expiry_date)
select
  i.item_id,
  s.batch_code,
  min(s.received_date) as received_date,
  max(s.expiry_date)   as expiry_date
from staging_batches s
join items i on i.shelf_id = s.shelf_id
group by i.item_id, s.batch_code
on conflict (item_id, batch_code) do update
set received_date = least(batches.received_date, excluded.received_date),
    expiry_date   = coalesce(excluded.expiry_date, batches.expiry_date);

-- 4) batch_inventory (stato per location)
insert into batch_inventory (batch_id, location_id, quantity)
select
  b.batch_id,
  l.location_id,
  sum(s.quantity) as qty
from staging_batches s
join items i on i.shelf_id = s.shelf_id
join batches b on b.item_id = i.item_id and b.batch_code = s.batch_code
join locations l on l.location = s.location
group by b.batch_id, l.location_id
on conflict (batch_id, location_id) do update
set quantity = batch_inventory.quantity + excluded.quantity;

--Movimenti A→B (es. warehouse→instore) in un’unica transazione:

with moved as (
  update batch_inventory
  set quantity = quantity - :qty
  where batch_id = :batch_id and location_id = :loc_from and quantity >= :qty
  returning *
)
insert into batch_inventory (batch_id, location_id, quantity)
values (:batch_id, :loc_to, :qty)
on conflict (batch_id, location_id) do update
set quantity = batch_inventory.quantity + excluded.quantity;

--Vendite (consumo da instore):

update batch_inventory
set quantity = greatest(quantity - :sold_qty, 0)
where batch_id = :batch_id and location_id = :instore_loc;