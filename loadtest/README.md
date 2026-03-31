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

`loadtest/locustfile.py` now tries to infer the number of partitions by counting running `order-service-*` containers. If that detection does not work in your environment, you can still set `KAFKA_PARTITIONS` manually.

If you want to set it explicitly for one of the topology variants, use the matching partition count:

- `docker-compose.small.yml` -> `KAFKA_PARTITIONS=1`
- `docker-compose.medium.yml` -> `KAFKA_PARTITIONS=12`
- `docker-compose.large.yml` -> `KAFKA_PARTITIONS=24`

```bash
locust -f loadtest/locustfile.py
```

Examples:

```bash
KAFKA_PARTITIONS=1 locust -f loadtest/locustfile.py
KAFKA_PARTITIONS=12 locust -f loadtest/locustfile.py
KAFKA_PARTITIONS=24 locust -f loadtest/locustfile.py
```

Then open `http://localhost:8089`.

Suggested starting profile:
- Users: `200`
- Spawn rate: `20`
- Duration: `5m`

## Notes

- The test targets random pre-created orders via `POST /orders/checkout/{order_id}`.
- It marks `4xx` as failures and treats other responses as success.

## 5) Run our own consistency benchmarks


This is adapted from the course benchmark, but made more project-friendly:
- it uses one unique user per order so the expected result is easier to reason about
- it verifies consistency from API state instead of scraping temporary log files
- it works with both Saga and 2PC order representations
- it prints a human-readable summary and exits non-zero if any consistency check fails
- `--json` also prints the raw JSON report

Default scenario:
- `1000` users
- `1` shared item
- shared item stock `100`
- item price `1`
- user credit `1`
- `1000` concurrent checkout attempts


```bash
python loadtest/run_consistency_benchmark.py --users 300 --stock 50 --credit 5 --price-list 1,2,3
```

Chaos version:

```bash
python loadtest/run_chaos_consistency_benchmark.py --users 300 --stock 50 --credit 5 --price-list 1,2,3 --kill-services order-service-2 --kill-delay 1.0 --checkout-spread 2.0
```

Useful flags:
- `--users`
- `--stock`
- `--price`
- `--price-list`
- `--credit`
- `--quantity`
- `--parallelism`

The chaos benchmark runs the same consistency scenario, but kills one or more app containers during live checkout traffic and then checks whether the final stock and credit are still consistent after recovery.
