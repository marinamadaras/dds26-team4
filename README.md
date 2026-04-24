# For Review 
main -> default implementation with both saga and 2pc, no orchestration
inschallah -> Orchestrator: After the interview where we ran into troubles with larger docker compose files, we made this
inschallah-cpu -> Orchestrator: same as above with cpu limits (reccommended)

Branches we used during interview but had troubles with regarding large docker compose
orchestrator-test -> orchestrator implemented, with added consistency test (in loadtest there is a readme file that explains how to run this)
orchestrator-> same as orchestrator test, without our own testing files

As a reminder from our email, to run orchestrator the easiest way would be:

checkout inchallah
bash scripts/stack_up.sh saga (or 2pc)
python loadtest/init_orders.py
locust -f loadtest/locustfile.py
Make sure the host field in the locust ui is http://127.0.0.1:8000



# Distributed Data Systems Project Template

Basic project structure with Python's Flask and Redis. 
**You are free to use any web framework in any language and any database you like for this project.**

### Project structure

* `env`
    Folder containing the Redis env variables for the docker-compose deployment
    
* `helm-config` 
   Helm chart values for Redis and ingress-nginx
        
* `k8s`
    Folder containing the kubernetes deployments, apps and services for the ingress, order, payment and stock services.
    
* `order`
    Folder containing the order application logic and dockerfile. 
    
* `payment`
    Folder containing the payment application logic and dockerfile. 

* `stock`
    Folder containing the stock application logic and dockerfile. 

* `test`
    Folder containing some basic correctness tests for the entire system. (Feel free to enhance them)

### Deployment types:

#### docker-compose (local development)

After coding the REST endpoint logic run `docker-compose up --build` in the base folder to test if your logic is correct
(you can use the provided tests in the `\test` folder and change them as you wish). 

To switch all services between implementation variants (`2pc` vs `saga`), use a root `.env` file:

1. `cp .env.example .env`
2. Edit these variables in `.env`:
   - `ORDER_APP_MODULE`
   - `PAYMENT_APP_MODULE`
   - `STOCK_APP_MODULE`
3. Start the stack: `docker compose up --build`

Set all three to `app` for Saga mode, or keep `order2pcApp` / `payment2pcApp` / `stock2pcApp` for 2PC mode.

Additional pre-sized compose variants are available for a 96-core host:

- `docker-compose.small.yml`: 1 partition, 1 instance per app/database, 7.5 CPUs total
- `docker-compose.medium.yml`: 12 partitions, 50 CPUs total, leaves 46 CPUs for Locust
- `docker-compose.large.yml`: 24 partitions, 90 CPUs total, leaves 6 CPUs for Locust

Example:

```bash
docker compose -f docker-compose.medium.yml up -d --build
```

To regenerate these files after changing the topology template, run:

```bash
python3 scripts/generate_compose_variants.py
```

***Requirements:*** You need to have docker and docker-compose installed on your machine. 

K8s is also possible, but we do not require it as part of your submission. 

#### minikube (local k8s cluster)

This setup is for local k8s testing to see if your k8s config works before deploying to the cloud. 
First deploy your database using helm by running the `deploy-charts-minicube.sh` file (in this example the DB is Redis 
but you can find any database you want in https://artifacthub.io/ and adapt the script). Then adapt the k8s configuration files in the
`\k8s` folder to mach your system and then run `kubectl apply -f .` in the k8s folder. 

***Requirements:*** You need to have minikube (with ingress enabled) and helm installed on your machine.

#### kubernetes cluster (managed k8s cluster in the cloud)

Similarly to the `minikube` deployment but run the `deploy-charts-cluster.sh` in the helm step to also install an ingress to the cluster. 

***Requirements:*** You need to have access to kubectl of a k8s cluster.
