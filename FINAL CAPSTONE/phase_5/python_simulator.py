import json
import time
import random
from datetime import datetime
import numpy as np
from faker import Faker
from kafka import KafkaProducer

fake = Faker()
random.seed(42)
np.random.seed(42)

# Configuration
BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'social_media_events'
NUM_USERS = 100
NUM_POSTS = 500
TARGET_EVENTS_PER_MIN = 1000
SLEEP_INTERVAL = 60.0 / TARGET_EVENTS_PER_MIN 

# Sample Data Pools
HASHTAGS_POOL = ['#dataengineering', '#pyspark', '#snowflake', '#viral', '#tech2026', '#ai', '#opensource', '#trending']
COMMENTS_POOL = [
    "Wow, this is amazing!", "Honestly, I disagree with this.", "Incredible work!", 
    "Can you share more details?", "This changed my perspective.", "Love it!"
]

# Generate Static Mock dimensions to map targets
user_ids = [f"USR_{i:04d}" for i in range(1, NUM_USERS + 1)]
posts_pool = []
for i in range(1, NUM_POSTS + 1):
    posts_pool.append({
        "post_id": f"PST_{i:05d}",
        "user_id": random.choice(user_ids)
    })

# Initialize Kafka Connection
try:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks=1
    )
    KAFKA_ONLINE = True
    print(f"Connected to Kafka. Routing structured events to topic: '{TOPIC_NAME}'...")
except Exception as e:
    KAFKA_ONLINE = False
    print(f"Kafka connection offline ({e}). Running in DRY-RUN console mode...")

# Fixed Event Distribution
event_types = ['POST_CREATED', 'LIKE', 'COMMENT', 'SHARE', 'FOLLOW', 'VIDEO_VIEW', 'PROFILE_VISIT']
event_weights = [0.05, 0.45, 0.15, 0.05, 0.05, 0.15, 0.10]

post_probabilities = np.random.pareto(2.5, NUM_POSTS)
post_probabilities /= post_probabilities.sum()

def generate_unified_event(viral_post=None, viral_active=False):
    """Generates a strictly structured record where every column exists regardless of event_type."""
    current_time = datetime.utcnow().isoformat() + "Z"
    actor_user_id = random.choice(user_ids)
    
    # Determine Event Type
    if viral_active and random.random() < 0.85:
        event_type = random.choice(['LIKE', 'COMMENT', 'SHARE'])
    else:
        event_type = random.choices(event_types, weights=event_weights)[0]

    # Target Selection Logic
    target_post_id = None
    target_author_id = None
    target_user_id = None
    comment_text = None
    hashtags_used = []

    if event_type in ['LIKE', 'COMMENT', 'SHARE', 'VIDEO_VIEW']:
        target_post = viral_post if viral_active and random.random() < 0.85 else np.random.choice(posts_pool, p=post_probabilities)
        target_post_id = target_post["post_id"]
        target_author_id = target_post["user_id"]
        
        if event_type == 'COMMENT':
            comment_text = random.choice(COMMENTS_POOL)
            if random.random() < 0.4:  # 40% chance of adding a hashtag to a comment
                hashtags_used.append(random.choice(HASHTAGS_POOL))

    elif event_type in ['FOLLOW', 'PROFILE_VISIT']:
        target_user_id = random.choice(user_ids)
        while target_user_id == actor_user_id:
            target_user_id = random.choice(user_ids)

    elif event_type == 'POST_CREATED':
        # Simulate creating a brand new post out-of-band
        target_post_id = f"PST_{random.randint(10000, 99999)}"
        target_author_id = actor_user_id
        if random.random() < 0.7:  # 70% chance new posts have tags
            hashtags_used = random.sample(HASHTAGS_POOL, k=random.randint(1, 3))

    # UNIFIED SCHEMA PAYLOAD: Flat & consistent layout
    payload = {
        "event_id": fake.uuid4(),
        "event_type": event_type,
        "actor_user_id": actor_user_id,
        "target_user_id": target_user_id,
        "target_post_id": target_post_id,
        "target_author_id": target_author_id,
        "comment_text": comment_text,
        "hashtags": hashtags_used,
        "timestamp": current_time
    }
    return payload

# Main Loop with Throughput Maintenance
print("Beginning real-time social event stream. Press Ctrl+C to terminate.")
event_count = 0
start_time = time.time()

viral_post_target = random.choice(posts_pool)
is_viral_burst = False

try:
    while True:
        # Step 2 Spike Test Capability: Trigger viral spikes every 600 records
        if event_count % 600 == 0 and event_count > 0:
            is_viral_burst = not is_viral_burst
            if is_viral_burst:
                viral_post_target = random.choice(posts_pool)
                print(f"\n⚡ [VIRAL SPIKE ACTIVE] Target post: {viral_post_target['post_id']} by {viral_post_target['user_id']} ⚡\n")
            else:
                print(f"\n🍃 [VIRAL SPIKE COOLED] Returning to organic distribution traffic patterns. 🍃\n")

        # Generate structural event
        event_payload = generate_unified_event(viral_post=viral_post_target, viral_active=is_viral_burst)

        # Ship Event
        if KAFKA_ONLINE:
            producer.send(TOPIC_NAME, value=event_payload)
        else:
            print(json.dumps(event_payload))

        event_count += 1

        # Metrics Log for Verification (Target: >1000 events/min)
        if event_count % 200 == 0:
            elapsed_seconds = time.time() - start_time
            calculated_rpm = (event_count / elapsed_seconds) * 60
            print(f"[STREAM MONITOR] Total Packets: {event_count} | Up Time: {elapsed_seconds:.1f}s | Flow Rate: {calculated_rpm:.0f} RPM")

        time.sleep(SLEEP_INTERVAL)

except KeyboardInterrupt:
    print("\nShutting down event generation process.")
finally:
    if KAFKA_ONLINE:
        producer.flush()
        producer.close()