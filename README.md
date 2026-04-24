# es-proxy-bench: Envoy vs HAProxy dla Elasticsearch

Benchmark uruchamiany w Dockerze. Porównuje dwa endpointy HTTP prowadzące do tego samego klastra Elasticsearch:

- `--envoy http://adres:port`
- `--haproxy http://adres:port`

Workload miesza operacje typowe dla aplikacji i dashboardów: `_search` z filtrami i agregacją, `_msearch`, `GET _doc`, `_bulk` oraz pojedyncze indeksowanie dokumentów.

## Uruchomienie

```bash
mkdir -p results
ENVOY_URL=http://10.0.0.10:10000 \
HAPROXY_URL=http://10.0.0.11:10001 \
DURATION=180 \
WARMUP=30 \
CONCURRENCY=128 \
SEED_DOCS=100000 \
docker compose run --rm es-proxy-bench
```

Albo bez Compose:

```bash
docker build -t es-proxy-bench:local .
docker run --rm --network host -v "$PWD/results:/bench/results" es-proxy-bench:local \
  --envoy http://10.0.0.10:10000 \
  --haproxy http://10.0.0.11:10001 \
  --duration 180 \
  --warmup 30 \
  --concurrency 128 \
  --seed-docs 100000
```

## Elasticsearch z autoryzacją

```bash
ES_BASIC_AUTH='elastic:haslo' docker compose run --rm es-proxy-bench
```

Dla tokena:

```bash
ES_BEARER_TOKEN='...' docker compose run --rm es-proxy-bench
```

Dla HTTPS z self-signed:

```bash
docker compose run --rm es-proxy-bench --tls-insecure \
  --envoy https://10.0.0.10:10000 \
  --haproxy https://10.0.0.11:10001
```

## Wyniki

Wyniki zapisują się w `./results` jako:

- `es-proxy-bench-<run_id>.json`
- `es-proxy-bench-<run_id>.md`

Raport zawiera: requesty, błędy, RPS, success RPS, avg, p50, p95 i p99 globalnie oraz per operacja.

## Uwagi do rzetelnego testu

1. Oba proxy powinny prowadzić do tego samego klastra i najlepiej tych samych backendów.
2. Puść test minimum dwa razy, raz normalnie i raz z `--reverse-order`, żeby ograniczyć efekt cache/warmup.
3. Testuj z maszyny niezależnej od proxy i Elasticsearch, bo generator ruchu też zużywa CPU.
4. Na czas porównania wyłącz zbędne logowanie access logów albo ustaw je identycznie w obu proxy.
5. Dla większych testów zwiększ `--seed-docs`, `--duration` i `--concurrency`.
