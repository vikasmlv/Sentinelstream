# Interview Q&A (Hinglish, short answers; tech terms English)

## M1: Data + Model
**Q1. Time-based split kyun, random kyun nahi?**
Fraud time-dependent hota hai. Random split se future ka info train mein leak hota hai aur numbers jhoothe achhe aate hain. Production mein hum hamesha past par train karke future score karte hain, split wahi simulate karta hai.

**Q2. Recall at fixed precision kyun, accuracy kyun nahi?**
Fraud rate ~3%, toh "sab approve" 97% accuracy deta hai. Business ka constraint hota hai: review team sirf itne alerts handle kar sakti hai (precision). Us precision par kitna fraud pakda (recall) wahi real metric hai.

**Q3. Threshold test par tune kiya?**
Nahi. Val par threshold freeze kiya, test sirf ek baar. Warna test leak hota hai. (Limitation: early stopping bhi val use karta hai.)

**Q4. Rule baseline fair hai?**
Rule thresholds bhi val par grid-search se tune hue same precision constraint ke saath. Weak baseline banake improvement dikhana dishonest hota.

**Q5. `scale_pos_weight` ka score par kya asar?**
Probabilities calibrated nahi rehti, isliye raw 0.5 threshold nahi, val-based threshold use karte hain. Calibration chahiye toh isotonic/Platt.

## M2: Streaming
**Q1. Exactly-once vs at-least-once?**
Hum at-least-once hain: offset DynamoDB write ke BAAD commit. Crash par message dobara aata hai. Effects idempotent hain (Redis ZADD by event_id, DynamoDB `attribute_not_exists`), toh result effectively-once. True exactly-once Kafka transactions chahta hai, aur external sinks (DynamoDB) ke saath phir bhi idempotency lagti hai.

**Q2. Redis done-marker pehle kyun nahi set karte?**
Pehle set karke crash hua toh redelivered message skip ho jayega aur decision kabhi nahi likha jayega (data loss). Isliye marker durable write ke baad. Marker sirf optimization hai, truth DynamoDB hai.

**Q3. Poison message aur transient failure mein farq?**
Poison (bad JSON) kabhi theek nahi hoga, retry bekaar, seedha DLQ taaki partition block na ho. Transient (timeout, 5xx, throttle) retry with exponential backoff + jitter. Retries khatam toh DLQ, aur `replay_dlq` se dobara.

**Q4. Hot key / hot partition?**
key=user_id se ek bahut active user ek partition load karta hai. Options: key salting (par ordering toot-ti hai, features galat), rate-limit per user, ya us user ko alag topic. Tradeoff: ordering vs balance.

**Q5. Consumers kaise scale karein?**
Consumer group mein consumers badhao, max = partition count. Partitions pehle se zyada rakho (repartition ordering todta hai). Lag metric dekh kar autoscale.

## M3: Scoring service
**Q1. Model service alag kyun, consumer ke andar kyun nahi?**
Independent scaling, aur alag deploy/rollback, aur `/score` ko Locust se seedha measure kar sakte hain. Cost: ek network hop; isliye keep-alive + batch endpoint.

**Q2. Feature skew kaise roka?**
Ek hi `features.py` definitions, window semantics document, aur test jo offline pandas features ko Redis online features se bit-by-bit compare karta hai (timestamp ties ke saath). Model meta mein feature list, service load par code ki list se match check karta hai.

**Q3. Micro-batching ka design?**
`max_wait=0`: latency add nahi karta, jab predict chal raha hota hai to naye requests queue mein jama hote hain aur agla predict unhe ek saath karta hai. Timed batching throughput badha sakta hai par p99 badhata hai. Measure karke decide karo.

**Q4. Redis crash ho jaye toh?**
Features gayab (cold start). Kafka replay se rebuild ho sakte hain (state deterministic hai event ts se). Tab tak counts kam aayenge, isliye model conservative behave kar sakta hai; production mein replication/AOF ya warm-up phase.

**Q5. DynamoDB item design aur consistency?**
PK=event_id, conditional put idempotent. By-user audit ke liye optional GSI (`--with-gsi`), par GSI free-tier WCU share karta hai. Reads by event_id strongly consistent maang sakte hain.

## M4: Load test
**Q1. p99 kaise interpret karte ho?**
Step load se knee dhundta hun: jahan RPS flat aur p99 tez badhe. Sustained number knee se 20-30% neeche. Locust CPU aur service CPU dono dekhta hun taaki load generator bottleneck na ho.

**Q2. Coordinated omission?**
Closed-loop users (wait=0) slow response par kam requests bhejte hain, toh tail latency under-report ho sakti hai. Isliye p99 ko RPS ke saath padho, aur open-loop (fixed arrival rate, simulator) test alag karo.

**Q3. Tumhara bottleneck kya nikla?**
Clean local benchmarks mein DynamoDB write concurrency bottleneck dikha. Same 20,401-message workload par `DDB_WORKERS=16` se 309 events/s aur p99 593 ms mila. Workers ko 32 karne par throughput 99 events/s tak gir gaya aur p99 1,943 ms ho gaya. Isliye higher write concurrency automatically better nahi thi; local DynamoDB/write contention ke baad 16 workers retain kiye. Ye local Docker result hai, AWS DynamoDB capacity benchmark nahi.**

**Q4. Resume number aur measured number alag aaye toh?**
Resume update karta hun. Environment (local/AWS, cores) aur metric ka naam bullet mein sahi rakhta hun.

## M5: AWS / ops
**Q1. Free tier par 4,500/sec kyun nahi?**
CPU/RAM aur DynamoDB 25 WCU limit. Isliye approve decisions ka deterministic sample likhta hun aur limitation README mein hai.

**Q2. Production mein kya badalte?**
MSK/Kinesis, ElastiCache with replication, DynamoDB on-demand + TTL, model registry + canary, metrics (Prometheus/CloudWatch), schema registry, consumer autoscaling on lag.

