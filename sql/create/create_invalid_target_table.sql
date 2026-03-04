CREATE TABLE IF NOT EXISTS dev.public.invalid_order_events (
    event_id      VARCHAR(255),
    order_id      VARCHAR(255),
    event_type    VARCHAR(100),
    timestamp     TIMESTAMP,
    customer_id   VARCHAR(255),
    product_id    VARCHAR(255),
    quantity      INTEGER,
    price         FLOAT,
    partition_key  VARCHAR(255)
);