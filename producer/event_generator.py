from kafka import KafkaProducer
import json
import random
import time
import uuid
from datetime import datetime, timezone
from faker import Faker

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

MERCHANT_CATEGORIES = ["grocery", "electronics", "gas", "restaurant", "travel", "online_retail"]
ACCOUNTS = [f"acct_{i:04d}" for i in range(50)]


def make_transaction(account_id, burst=False):
    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": account_id,
        "amount": round(random.uniform(500, 2000), 2) if burst else round(random.uniform(5, 500), 2),
        "merchant": fake.company(),
        "category": random.choice(MERCHANT_CATEGORIES),
        "city": fake.city(),
        "card_present": random.random() > 0.3,
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def run(n_events=500, delay=0.2):
    print("Producing transactions... (Ctrl+C to stop)")
    for i in range(n_events):
        if random.random() < 0.05:
            burst_account = random.choice(ACCOUNTS)
            for _ in range(random.randint(3, 6)):
                txn = make_transaction(burst_account, burst=True)
                producer.send("transactions", key=txn["account_id"], value=txn)
                print(f"[BURST] {txn['account_id']} ${txn['amount']}")
                time.sleep(0.05)
        else:
            account_id = random.choice(ACCOUNTS)
            txn = make_transaction(account_id)
            producer.send("transactions", key=txn["account_id"], value=txn)
            print(f"sent {txn['account_id']} ${txn['amount']}")
        time.sleep(delay)

    producer.flush()
    print("done")


if __name__ == "__main__":
    run()