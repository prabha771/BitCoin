import os
import json
import re
import string
from utils.extract import extract_addresses

# Define your paths
path_threads = "BitcoinTalkThreads"
path_detected_threads = "utils/threads_w_addresses_or_ids.json"

# Load detected threads if already present
if os.path.exists(path_detected_threads):
    with open(path_detected_threads, "r") as f:
        threads_detected = json.load(f)
else:
    threads_detected = {}

# Define must_contain keywords for "withdraw2" task
must_contain = {"withdraw", "withdrawal", "withdrew", "withdrawn","deposit", "deposited", "transfer", "transferred","hot", "cold"}

# Start analysis
print("\n=== THREAD PROCESSING DIAGNOSTIC ===\n")
for filename in sorted(os.listdir(path_threads)):
    if not filename.endswith(".json"):
        continue

    thread_path = os.path.join(path_threads, filename)
    with open(thread_path, "r") as f:
        thread = json.load(f)

    thread_number = str(thread["thread_number"])
    was_detected = thread_number in threads_detected
    total_messages = 0
    selected_messages = 0

    print(f"🧵 Thread {thread_number} — Detected: {was_detected}")

    for msg_idx in [k for k in thread if k.isdigit()]:
        message = thread[msg_idx]["message"]
        message_lc = message.lower()

        # Split and clean words to match must_contain keywords
        words = set(w.strip(string.punctuation) for w in message_lc.split())
        has_keyword = len(words.intersection(must_contain)) > 0

        addresses = extract_addresses(message)
        has_address = len(addresses) > 0

        status = "✅ Included" if has_address and has_keyword else "❌ Skipped"
        print(f"  • Message {msg_idx}: {status} | keyword: {has_keyword}, address: {has_address}")

        total_messages += 1
        if has_keyword and has_address:
            selected_messages += 1

    print(f"  → {selected_messages}/{total_messages} messages matched conditions\n")

print("✅ Diagnostic complete.")