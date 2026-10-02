# Locust plan (Hinglish, short)

## Kya measure karna hai (3 alag numbers, mix mat karna)
1. **/score latency + RPS**: Locust se. Ye "scoring service" ka number hai.
2. **Consumer throughput**: simulator `--rate` badhao, `consumer_stats_*.json` mein `events_per_sec`.
3. **Consume-to-decision p99** (aur Kafka-to-decision jisme queueing shamil hai): consumer stats.

Resume mein "4,500 events/sec at p99 < 45 ms" tabhi jab **ek hi test mein dono** (throughput aur p99) aaye aur metric ka naam sahi likha ho.

## Steps
1. Warm-up: 30s low load (model/JIT/connections garam ho jayein). Warm-up ke numbers mat ginna.
2. Step load: `make step-load` (10,25,50,100,200,400 users, har level 45s). Har level ka RPS + p99 dekho.
3. Knee dhundo: jis level par RPS flat ho jaye aur p99 tez badhe, wahi capacity hai. Usse ~20-30% neeche wala level "sustained" number hai.
4. Sustained run: us level par 5 min chalao (`-t 5m`). Failure = 0 hona chahiye.
5. `docker stats` parallel dekho (CPU%, mem).
6. `make summarize LABEL="<machine + cores>"`.

## Bottleneck kaise padhein
| Symptom | Matlab | Fix |
|---|---|---|
| RPS flat, scorer CPU 100% | Predict/JSON CPU-bound | `SCORER_WORKERS` badhao (cores tak), `BATCHER_MAX_WAIT_MS` 1-2 try karo, `/stats` mein avg_batch_size dekho |
| RPS flat, scorer CPU low, Locust CPU 100% | **Load generator** bottleneck | `locust --processes N` ya alag machine |
| p99 >> p50, CPU low | GC pause / noisy neighbour / single worker queueing | workers badhao, `--no-access-log` confirm |
| Consumer lag badhta hai | Consumer slow | consumers scale karo (max = partitions), `BATCH_SIZE` badhao, DynamoDB workers |
| Consumer stuck, DDB errors | Throttling (WCU) | `APPROVE_SAMPLE_RATE` kam karo ya WCU badhao (cost dekho) |
| Redis CPU 100% | Feature pipeline heavy | batch size badhao, key TTL/ZSET size check |
| Ek partition busy baaki idle | Hot key | key salting ya user-level rate limit; interview mein tradeoff batao |

## Before/after rule
Har optimization (workers, batching, batch size) ke pehle aur baad ka number `results/` mein alag file mein rakho. Resume mein wahi likho jo final config ne diya.
