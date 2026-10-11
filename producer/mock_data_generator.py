import random
import uuid
from datetime import datetime, timezone

CATEGORIES = ["Electronics", "Clothing", "Home", "Books", "Toys"]
CUSTOMERS = [f"C{i}" for i in range(101, 115)]
STATUSES = ["SUCCESS", "SUCCESS", "SUCCESS", "CANCELLED", "REFUNDED"]

def generate_transaction():
    return {
        "transaction_id": f"T{uuid.uuid4().hex[:8].upper()}",
        "customer_id": random.choice(CUSTOMERS),
        "category": random.choice(CATEGORIES),
        "amount": round(random.uniform(15.0, 950.0), 2),
        "status": random.choice(STATUSES),
        "event_time": datetime.now(timezone.utc).isoformat()
    }