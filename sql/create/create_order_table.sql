CREATE TABLE IF NOT EXISTS dev.public.orders (
    order_id            VARCHAR(36)   NOT NULL,
    latest_event_id     VARCHAR(36),
    order_status        VARCHAR(50),
    latest_event_time   TIMESTAMP,

    customer_id         VARCHAR(36),
    product_id          VARCHAR(36),
    quantity            INTEGER,
    price               DECIMAL(10,2),

    updated_at          TIMESTAMP DEFAULT GETDATE(),

    PRIMARY KEY(order_id)
)
DISTKEY(order_id)
SORTKEY(order_id, latest_event_time);