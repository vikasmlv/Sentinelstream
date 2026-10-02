# AWS deploy (Hinglish, short)

**Pehle free-tier terms check karo** (AWS ne policies badli hain; console mein "Free tier" page dekho). Ye doc assume karta hai ek chhota EC2 + real DynamoDB table.

1. EC2 launch: Ubuntu, sabse chhota jo free-tier mein aaye. 1 GB RAM par Kafka+Redis+scorer+consumer tight hai. 2 GB swap add karo, ya paid t3.small/medium par test karo aur **instance type resume/README mein likho**.
2. IAM role attach karo: `dynamodb:PutItem, GetItem, DescribeTable, CreateTable` (us table par).
3. Docker install, repo clone, laptop se `models/` copy karo (`scp -r models ubuntu@host:sentinelstream/`).
4. `.env` mein `AWS_REGION`, `DDB_WCU=25`, `DDB_RCU=25`, `APPROVE_SAMPLE_RATE=0.01`.
5. `docker compose -f docker-compose.yml -f docker-compose.aws.yml up -d --build`
6. Simulator EC2 par hi chalao (`--rate` chhota se shuru) ya alag instance se `KAFKA_BOOTSTRAP` set karke.
7. Locust alag machine se (warna load generator EC2 ka CPU kha jayega).
8. Numbers `results/summary.md` mein `--label "EC2 <type>"` ke saath.

## Kya "on AWS" bol sakte ho
- Sirf tab jab pipeline actually EC2 par chala aur numbers wahin se aaye.
- Agar sirf DynamoDB real tha aur baaki local: "DynamoDB on AWS, rest local" likho.
- SQS is design mein use nahi hua (DLQ Kafka topic hai). Resume mein SQS mat likho jab tak use na karo.

## DynamoDB free tier math
25 WCU ~ 25 writes/sec (1KB item tak). 4,500 events/sec sab likhna impossible. Isliye `APPROVE_SAMPLE_RATE` (deterministic sample by event_id) ya review/block-only. Ye tradeoff README mein sach-sach likho.
