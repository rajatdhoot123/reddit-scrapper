#!/usr/bin/env python3

from database_integration import find_comments_file_by_reddit_id
import logging

# Set logging to WARNING level to reduce noise
logging.basicConfig(level=logging.WARNING)

# Test with known Reddit IDs
test_ids = ['1j9bd4e', '1kwf0a5', '1ky44lo']

print("Testing fixed comment finding function:")
print("=" * 50)

for reddit_id in test_ids:
    result = find_comments_file_by_reddit_id(reddit_id)
    if result:
        print(f"✅ {reddit_id}: {result.name}")
    else:
        print(f"❌ {reddit_id}: Not found")

print("=" * 50)
print("Test complete!") 