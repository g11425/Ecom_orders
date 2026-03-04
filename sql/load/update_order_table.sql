MERGE INTO dev.public.orders
USING (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY order_id
                   ORDER BY timestamp DESC
               ) AS rn
        FROM dev.public.order_events
    ) t
    WHERE rn = 1
) s
ON orders.order_id = s.order_id

WHEN MATCHED THEN
UPDATE SET
    latest_event_id =
        CASE WHEN s.timestamp > orders.latest_event_time
             THEN s.event_id ELSE orders.latest_event_id END,

    order_status =
        CASE WHEN s.timestamp > orders.latest_event_time
             THEN s.event_type ELSE orders.order_status END,

    latest_event_time =
        CASE WHEN s.timestamp > orders.latest_event_time
             THEN s.timestamp ELSE orders.latest_event_time END,

    updated_at =
        CASE WHEN s.timestamp > orders.latest_event_time
             THEN GETDATE() ELSE orders.updated_at END

WHEN NOT MATCHED THEN
INSERT (
    order_id,
    latest_event_id,
    order_status,
    latest_event_time,
    customer_id,
    product_id,
    quantity,
    price
)
VALUES (
    s.order_id,
    s.event_id,
    s.event_type,
    s.timestamp,
    s.customer_id,
    s.product_id,
    s.quantity,
    s.price
);