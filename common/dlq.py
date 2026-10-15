"""Dead letter queue helper.

Malformed or out-of-range messages are wrapped with their validation errors
and republished to a ``<topic>-dlq`` topic, so bad data is quarantined for
inspection rather than silently dropped or allowed to crash the consumer.
"""

import json
from datetime import datetime, timezone


def send_to_dlq(producer, dlq_topic, raw_value, errors):
    envelope = {
        "original_message": raw_value,
        "validation_errors": errors,
        "failed_at": datetime.now(timezone.utc).isoformat(),
    }
    producer.produce(dlq_topic, value=json.dumps(envelope))
    producer.flush()
