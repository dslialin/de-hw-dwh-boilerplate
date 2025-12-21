import json
import os
import random
import time
from datetime import datetime, timedelta, timezone

import boto3

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "adminadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "lake")
PREFIX = os.getenv("PREFIX", "incoming")
BATCHES = int(os.getenv("BATCHES", "3"))
SLEEP_SEC = int(os.getenv("SLEEP_SEC", "3"))

random.seed(42)

session = boto3.session.Session()
s3 = session.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

def ensure_bucket(bucket: str, retries: int = 60, sleep_sec: float = 1.0) -> None:
    for i in range(retries):
        try:
            s3.head_bucket(Bucket=bucket)
            return
        except Exception:
            time.sleep(sleep_sec)
    # last attempt to fail loudly
    s3.head_bucket(Bucket=bucket)


def put_jsonl(key: str, rows: list[dict]) -> None:
    body = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows).encode("utf-8")
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/json")


def iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def main():
    # базовые сущности
    merchants = [f"m_{i:03d}" for i in range(1, 6)]
    users = [f"u_{i:04d}" for i in range(1, 51)]
    currencies = ["USD", "EUR"]

    base = datetime.now(timezone.utc) - timedelta(days=1)
    ensure_bucket(S3_BUCKET)

    # первый merchants snapshot
    merch_rows = []
    for m in merchants:
        merch_rows.append({
            "merchant_id": m,
            "event_time": iso(base),
            "mcc": random.choice(["5411", "5812", "5999"]),
            "country": random.choice(["LT", "PL", "DE"]),
            "is_active": True,
            "op": "I"
        })
    put_jsonl(f"{PREFIX}/merchants/batch0.jsonl", merch_rows)

    tx_state: dict[str, dict] = {}

    for b in range(1, BATCHES + 1):
        ts = base + timedelta(minutes=5 * b)

        # генерим вставки
        new_rows = []
        for i in range(30):
            tx_id = f"tx_{b:02d}_{i:04d}"
            row = {
                "tx_id": tx_id,
                "event_time": iso(ts),
                "merchant_id": random.choice(merchants),
                "user_id": random.choice(users),
                "amount": str(round(random.uniform(1, 120), 2)),
                "currency": random.choice(currencies),
                "status": random.choice(["APPROVED", "DECLINED"]),
                "op": "I",
            }
            tx_state[tx_id] = row
            new_rows.append(row)

        # делаем апдейты и удаления из прошлого батча
        upd_rows = []
        del_rows = []
        prev_keys = list(tx_state.keys())
        random.shuffle(prev_keys)

        for tx_id in prev_keys[:5]:
            old = tx_state[tx_id]
            upd = dict(old)
            upd["event_time"] = iso(ts)
            upd["amount"] = str(round(float(old["amount"]) * random.uniform(0.8, 1.2), 2))
            upd["status"] = "APPROVED"
            upd["op"] = "U"
            tx_state[tx_id] = upd
            upd_rows.append(upd)

        for tx_id in prev_keys[5:8]:
            old = tx_state[tx_id]
            tomb = {
                "tx_id": tx_id,
                "event_time": iso(ts),
                "merchant_id": old["merchant_id"],
                "user_id": old["user_id"],
                "amount": old["amount"],
                "currency": old["currency"],
                "status": old["status"],
                "op": "D",
            }
            del_rows.append(tomb)

        put_jsonl(f"{PREFIX}/transactions/batch{b}_new.jsonl", new_rows)
        put_jsonl(f"{PREFIX}/transactions/batch{b}_upd.jsonl", upd_rows)
        put_jsonl(f"{PREFIX}/transactions/batch{b}_del.jsonl", del_rows)

        # merchants: иногда обновим страну/active
        m_updates = []
        for m in random.sample(merchants, k=2):
            m_updates.append({
                "merchant_id": m,
                "event_time": iso(ts),
                "mcc": random.choice(["5411", "5812", "5999"]),
                "country": random.choice(["LT", "PL", "DE"]),
                "is_active": random.choice([True, True, True, False]),
                "op": "U",
            })
        put_jsonl(f"{PREFIX}/merchants/batch{b}.jsonl", m_updates)

        print(f"Published batch {b}: tx new={len(new_rows)} upd={len(upd_rows)} del={len(del_rows)}; merchants upd={len(m_updates)}")
        time.sleep(SLEEP_SEC)

if __name__ == "__main__":
    main()
