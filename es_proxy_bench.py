#!/usr/bin/env python3
import argparse
import asyncio
import base64
import dataclasses
import hashlib
import os
import random
import statistics
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any

import aiohttp
import numpy as np
import orjson


@dataclasses.dataclass
class Sample:
    op: str
    ms: float
    status: int
    ok: bool
    bytes_in: int = 0
    bytes_out: int = 0


def jdumps(obj: Any) -> bytes:
    return orjson.dumps(obj)


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    return float(np.percentile(np.array(values, dtype=float), p))


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_headers(args) -> dict[str, str]:
    headers = {"Accept": "application/json"}
    if args.basic_auth or os.getenv("ES_BASIC_AUTH"):
        raw = args.basic_auth or os.getenv("ES_BASIC_AUTH")
        token = base64.b64encode(raw.encode()).decode()
        headers["Authorization"] = f"Basic {token}"
    if args.bearer_token or os.getenv("ES_BEARER_TOKEN"):
        token = args.bearer_token or os.getenv("ES_BEARER_TOKEN")
        headers["Authorization"] = f"Bearer {token}"
    return headers


def gen_doc(i: int, run_id: str) -> dict[str, Any]:
    services = ["checkout", "catalog", "payment", "search", "auth", "orders", "frontend"]
    levels = ["INFO", "INFO", "INFO", "WARN", "ERROR"]
    regions = ["pl", "de", "fr", "uk", "us"]
    service = services[i % len(services)]
    level = levels[(i * 7) % len(levels)]
    region = regions[(i * 13) % len(regions)]
    latency = 5 + ((i * 17) % 500)
    user_id = f"u-{(i * 2654435761) % 250000:06d}"
    trace = hashlib.sha1(f"{run_id}-{i}".encode()).hexdigest()
    return {
        "@timestamp": datetime.fromtimestamp(1700000000 + (i % 2_500_000), tz=timezone.utc).isoformat(),
        "run_id": run_id,
        "service": service,
        "level": level,
        "region": region,
        "user_id": user_id,
        "trace_id": trace,
        "message": f"{level} {service} request completed in {latency} ms for {user_id}",
        "http": {"method": "GET" if i % 4 else "POST", "status_code": 200 if i % 19 else 500},
        "metrics": {"latency_ms": latency, "bytes": 300 + (i * 31) % 15000},
        "tags": [service, region, level.lower()],
    }


async def request(session: aiohttp.ClientSession, method: str, url: str, **kwargs) -> tuple[int, bytes, float]:
    start = time.perf_counter()
    async with session.request(method, url, **kwargs) as resp:
        body = await resp.read()
        elapsed_ms = (time.perf_counter() - start) * 1000
        return resp.status, body, elapsed_ms


async def ensure_index(session, base_url: str, index: str, args) -> None:
    mapping = {
        "settings": {"number_of_shards": args.shards, "number_of_replicas": args.replicas, "refresh_interval": "30s"},
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date"},
                "run_id": {"type": "keyword"},
                "service": {"type": "keyword"},
                "level": {"type": "keyword"},
                "region": {"type": "keyword"},
                "user_id": {"type": "keyword"},
                "trace_id": {"type": "keyword"},
                "message": {"type": "text"},
                "http.status_code": {"type": "integer"},
                "metrics.latency_ms": {"type": "float"},
                "metrics.bytes": {"type": "long"},
                "tags": {"type": "keyword"},
            }
        },
    }
    status, body, _ = await request(session, "PUT", f"{base_url}/{index}", data=jdumps(mapping), headers={"Content-Type": "application/json"})
    if status not in (200, 201):
        # OK when index already exists.
        if b"resource_already_exists_exception" not in body:
            raise RuntimeError(f"Cannot create index {index}: HTTP {status}: {body[:500]!r}")


async def seed_data(session, base_url: str, index: str, args, run_id: str) -> None:
    if args.seed_docs <= 0:
        return
    print(f"[{now_iso()}] Seeding {args.seed_docs} docs into {base_url}/{index} ...", flush=True)
    sent = 0
    while sent < args.seed_docs:
        n = min(args.bulk_size, args.seed_docs - sent)
        lines = []
        for j in range(n):
            doc_id = sent + j
            lines.append(jdumps({"index": {"_index": index, "_id": f"seed-{doc_id}"}}))
            lines.append(jdumps(gen_doc(doc_id, run_id)))
        payload = b"\n".join(lines) + b"\n"
        status, body, ms = await request(
            session,
            "POST",
            f"{base_url}/_bulk",
            data=payload,
            headers={"Content-Type": "application/x-ndjson"},
        )
        if status >= 300:
            raise RuntimeError(f"Bulk seed failed HTTP {status}: {body[:500]!r}")
        sent += n
        if sent % max(args.bulk_size * 10, 5000) == 0:
            print(f"  seeded={sent}/{args.seed_docs}, last_bulk_ms={ms:.1f}", flush=True)
    await request(session, "POST", f"{base_url}/{index}/_refresh")


class EsWorkload:
    def __init__(self, base_url: str, index: str, args, run_id: str):
        self.base_url = base_url.rstrip("/")
        self.index = index
        self.args = args
        self.run_id = run_id
        self.services = ["checkout", "catalog", "payment", "search", "auth", "orders", "frontend"]
        self.levels = ["INFO", "WARN", "ERROR"]
        self.regions = ["pl", "de", "fr", "uk", "us"]

    async def search_filter_agg(self, session, rnd: random.Random) -> Sample:
        service = rnd.choice(self.services)
        level = rnd.choice(self.levels)
        body = {
            "size": 10,
            "query": {"bool": {"filter": [{"term": {"service": service}}, {"term": {"level": level}}]}},
            "aggs": {"by_region": {"terms": {"field": "region", "size": 5}}},
            "sort": [{"@timestamp": "desc"}],
        }
        return await self._json(session, "POST", f"/{self.index}/_search", body, "search_filter_agg")

    async def search_text(self, session, rnd: random.Random) -> Sample:
        term = rnd.choice(["request", "completed", "ERROR", "checkout", "payment", "catalog"])
        body = {"size": 10, "query": {"match": {"message": term}}, "highlight": {"fields": {"message": {}}}}
        return await self._json(session, "POST", f"/{self.index}/_search", body, "search_text")

    async def msearch_dashboard(self, session, rnd: random.Random) -> Sample:
        service = rnd.choice(self.services)
        region = rnd.choice(self.regions)
        parts = []
        for query in [
            {"size": 0, "query": {"term": {"service": service}}, "aggs": {"levels": {"terms": {"field": "level"}}}},
            {"size": 5, "query": {"term": {"region": region}}, "sort": [{"@timestamp": "desc"}]},
            {"size": 0, "query": {"range": {"metrics.latency_ms": {"gte": rnd.randint(50, 400)}}}},
        ]:
            parts.append(jdumps({"index": self.index}))
            parts.append(jdumps(query))
        payload = b"\n".join(parts) + b"\n"
        return await self._raw(session, "POST", "/_msearch", payload, "msearch_dashboard", "application/x-ndjson")

    async def get_doc(self, session, rnd: random.Random) -> Sample:
        doc_id = rnd.randint(0, max(1, self.args.seed_docs - 1))
        return await self._raw(session, "GET", f"/{self.index}/_doc/seed-{doc_id}", None, "get_doc")

    async def bulk_ingest(self, session, rnd: random.Random) -> Sample:
        n = max(1, self.args.bulk_size // 4)
        base = rnd.randint(10_000_000, 99_999_999)
        lines = []
        for i in range(n):
            doc_id = f"live-{base}-{i}"
            lines.append(jdumps({"index": {"_index": self.index, "_id": doc_id}}))
            lines.append(jdumps(gen_doc(base + i, self.run_id)))
        payload = b"\n".join(lines) + b"\n"
        return await self._raw(session, "POST", "/_bulk", payload, "bulk_ingest", "application/x-ndjson")

    async def index_single(self, session, rnd: random.Random) -> Sample:
        doc_id = f"single-{rnd.randint(1, 5_000_000)}"
        body = gen_doc(rnd.randint(1, 10_000_000), self.run_id)
        return await self._json(session, "PUT", f"/{self.index}/_doc/{doc_id}", body, "index_single")

    async def _json(self, session, method, path, body, op) -> Sample:
        payload = jdumps(body)
        return await self._raw(session, method, path, payload, op, "application/json")

    async def _raw(self, session, method, path, payload, op, content_type=None) -> Sample:
        headers = {}
        if content_type:
            headers["Content-Type"] = content_type
        status, body, ms = await request(session, method, f"{self.base_url}{path}", data=payload, headers=headers)
        return Sample(op=op, ms=ms, status=status, ok=(200 <= status < 300), bytes_in=len(body), bytes_out=len(payload or b""))

    def pick(self, rnd: random.Random):
        # Wagi celowo przypominają typowy mix: dużo search/msearch, trochę GET, trochę zapisu.
        ops = [
            ("search_filter_agg", 34, self.search_filter_agg),
            ("msearch_dashboard", 24, self.msearch_dashboard),
            ("search_text", 16, self.search_text),
            ("get_doc", 12, self.get_doc),
            ("bulk_ingest", 10, self.bulk_ingest),
            ("index_single", 4, self.index_single),
        ]
        total = sum(w for _, w, _ in ops)
        x = rnd.randint(1, total)
        acc = 0
        for _, weight, fn in ops:
            acc += weight
            if x <= acc:
                return fn
        return ops[-1][2]


async def worker(worker_id: int, session, workload: EsWorkload, deadline: float, samples: list[Sample], stop_on_errors: bool):
    rnd = random.Random(42_000 + worker_id)
    while time.perf_counter() < deadline:
        fn = workload.pick(rnd)
        try:
            s = await fn(session, rnd)
        except Exception as e:
            s = Sample(op="client_exception", ms=0.0, status=0, ok=False, bytes_in=0, bytes_out=0)
            if stop_on_errors:
                raise
        samples.append(s)
        if workload.args.sleep_ms > 0:
            await asyncio.sleep(workload.args.sleep_ms / 1000)


async def run_target(name: str, base_url: str, index: str, args, headers: dict[str, str], run_id: str) -> dict[str, Any]:
    timeout = aiohttp.ClientTimeout(total=args.request_timeout)
    connector = aiohttp.TCPConnector(ssl=False if args.tls_insecure else None, limit=0, ttl_dns_cache=300)
    async with aiohttp.ClientSession(headers=headers, timeout=timeout, connector=connector) as session:
        if not args.skip_setup:
            await ensure_index(session, base_url, index, args)
            await seed_data(session, base_url, index, args, run_id)

        workload = EsWorkload(base_url, index, args, run_id)

        if args.warmup > 0:
            print(f"[{now_iso()}] Warmup {name}: {args.warmup}s", flush=True)
            warm_samples: list[Sample] = []
            deadline = time.perf_counter() + args.warmup
            await asyncio.gather(*[worker(i, session, workload, deadline, warm_samples, False) for i in range(max(1, args.concurrency // 4))])

        print(f"[{now_iso()}] Benchmark {name}: duration={args.duration}s concurrency={args.concurrency}", flush=True)
        samples: list[Sample] = []
        start = time.perf_counter()
        deadline = start + args.duration
        await asyncio.gather(*[worker(i, session, workload, deadline, samples, args.stop_on_errors) for i in range(args.concurrency)])
        elapsed = time.perf_counter() - start
        return summarize(name, base_url, elapsed, samples)


def summarize(name: str, base_url: str, elapsed: float, samples: list[Sample]) -> dict[str, Any]:
    by_op: dict[str, list[Sample]] = defaultdict(list)
    for s in samples:
        by_op[s.op].append(s)
    total = len(samples)
    ok = sum(1 for s in samples if s.ok)
    result = {
        "name": name,
        "url": base_url,
        "elapsed_s": round(elapsed, 3),
        "requests": total,
        "ok": ok,
        "errors": total - ok,
        "rps": round(total / elapsed, 2) if elapsed else 0,
        "success_rps": round(ok / elapsed, 2) if elapsed else 0,
        "latency_ms": latency_stats([s.ms for s in samples if s.ok]),
        "status_counts": dict(Counter(str(s.status) for s in samples)),
        "bytes_in": sum(s.bytes_in for s in samples),
        "bytes_out": sum(s.bytes_out for s in samples),
        "operations": {},
    }
    for op, rows in sorted(by_op.items()):
        good = [s for s in rows if s.ok]
        result["operations"][op] = {
            "requests": len(rows),
            "ok": len(good),
            "errors": len(rows) - len(good),
            "rps": round(len(rows) / elapsed, 2) if elapsed else 0,
            "latency_ms": latency_stats([s.ms for s in good]),
            "status_counts": dict(Counter(str(s.status) for s in rows)),
        }
    return result


def latency_stats(values: list[float]) -> dict[str, float]:
    if not values:
        return {"min": 0, "avg": 0, "p50": 0, "p90": 0, "p95": 0, "p99": 0, "max": 0}
    return {
        "min": round(min(values), 2),
        "avg": round(statistics.mean(values), 2),
        "p50": round(percentile(values, 50), 2),
        "p90": round(percentile(values, 90), 2),
        "p95": round(percentile(values, 95), 2),
        "p99": round(percentile(values, 99), 2),
        "max": round(max(values), 2),
    }


def md_report(payload: dict[str, Any]) -> str:
    a, b = payload["results"]
    lines = [
        f"# Elasticsearch proxy benchmark: {a['name']} vs {b['name']}",
        "",
        f"Run ID: `{payload['run_id']}`",
        f"Index: `{payload['index']}`",
        f"Started: `{payload['started_at']}`",
        "",
        "## Summary",
        "",
        "| target | requests | errors | rps | success rps | avg ms | p50 | p95 | p99 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in payload["results"]:
        lat = r["latency_ms"]
        lines.append(f"| {r['name']} | {r['requests']} | {r['errors']} | {r['rps']} | {r['success_rps']} | {lat['avg']} | {lat['p50']} | {lat['p95']} | {lat['p99']} |")
    lines += ["", "## Operations", ""]
    for r in payload["results"]:
        lines += [f"### {r['name']} `{r['url']}`", "", "| op | requests | errors | rps | avg ms | p95 | p99 |", "|---|---:|---:|---:|---:|---:|---:|"]
        for op, stats in r["operations"].items():
            lat = stats["latency_ms"]
            lines.append(f"| {op} | {stats['requests']} | {stats['errors']} | {stats['rps']} | {lat['avg']} | {lat['p95']} | {lat['p99']} |")
        lines.append("")
    return "\n".join(lines)


async def main_async(args):
    os.makedirs(args.out_dir, exist_ok=True)
    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    index = args.index or f"proxy-bench-{run_id}"
    headers = build_headers(args)
    started = now_iso()

    order = [("envoy", args.envoy.rstrip("/")), ("haproxy", args.haproxy.rstrip("/"))]
    if args.reverse_order:
        order.reverse()

    results = []
    for name, url in order:
        res = await run_target(name, url, index, args, headers, run_id)
        results.append(res)

    payload = {
        "run_id": run_id,
        "started_at": started,
        "finished_at": now_iso(),
        "index": index,
        "args": vars(args),
        "results": results,
    }
    json_path = os.path.join(args.out_dir, f"es-proxy-bench-{run_id}.json")
    md_path = os.path.join(args.out_dir, f"es-proxy-bench-{run_id}.md")
    with open(json_path, "wb") as f:
        f.write(orjson.dumps(payload, option=orjson.OPT_INDENT_2))
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md_report(payload))

    print("\n" + md_report(payload), flush=True)
    print(f"\nSaved: {json_path}\nSaved: {md_path}", flush=True)


def parse_args():
    p = argparse.ArgumentParser(description="Benchmark Envoy vs HAProxy in front of Elasticsearch using realistic HTTP workload")
    p.add_argument("--envoy", required=True, help="Base URL do Elasticsearch przez Envoy, np. http://10.0.0.10:10000")
    p.add_argument("--haproxy", required=True, help="Base URL do Elasticsearch przez HAProxy, np. http://10.0.0.11:10001")
    p.add_argument("--index", default="", help="Nazwa indeksu testowego; domyślnie proxy-bench-<run_id>")
    p.add_argument("--run-id", default="", help="Stały ID przebiegu, przydatny do porównań")
    p.add_argument("--duration", type=int, default=120, help="Czas właściwego testu per proxy w sekundach")
    p.add_argument("--warmup", type=int, default=20, help="Warmup per proxy w sekundach")
    p.add_argument("--concurrency", type=int, default=64, help="Liczba równoległych workerów")
    p.add_argument("--seed-docs", type=int, default=50000, help="Liczba dokumentów seedowanych do indeksu")
    p.add_argument("--bulk-size", type=int, default=200, help="Rozmiar paczki bulk przy seedzie; live bulk używa 1/4 tej wartości")
    p.add_argument("--sleep-ms", type=float, default=0.0, help="Opcjonalna pauza po każdym requestcie workera")
    p.add_argument("--request-timeout", type=float, default=30.0)
    p.add_argument("--basic-auth", default="", help="user:password; alternatywnie env ES_BASIC_AUTH")
    p.add_argument("--bearer-token", default="", help="Bearer token; alternatywnie env ES_BEARER_TOKEN")
    p.add_argument("--tls-insecure", action="store_true", help="Wyłącza walidację TLS dla HTTPS")
    p.add_argument("--skip-setup", action="store_true", help="Nie tworzy indeksu i nie seeduje danych")
    p.add_argument("--stop-on-errors", action="store_true", help="Przerwij na wyjątku klienta")
    p.add_argument("--reverse-order", action="store_true", help="Najpierw HAProxy, potem Envoy")
    p.add_argument("--shards", type=int, default=1)
    p.add_argument("--replicas", type=int, default=0)
    p.add_argument("--out-dir", default="/bench/results")
    return p.parse_args()


def main():
    args = parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
