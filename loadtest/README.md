# Locust Stress Test

## 1) Start the stack

```bash
docker compose up -d --build
```

## 2) Install load-test dependencies

```bash
python -m pip install -r loadtest/requirements.txt
```

## 3) Seed large test data

```bash
python loadtest/init_orders.py
```

This seeds:
- 100,000 users
- 100,000 items
- 100,000 orders

All endpoints are read from `loadtest/urls.json`.

## 4) Run Locust

```bash
locust -f loadtest/locustfile.py
```

Then open `http://localhost:8089`.

Suggested starting profile:
- Users: `200`
- Spawn rate: `20`
- Duration: `5m`

## Notes

- The test targets random pre-created orders via `POST /orders/checkout/{order_id}`.
- It marks `4xx` as failures and treats other responses as success.
