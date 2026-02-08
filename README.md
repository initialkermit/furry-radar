# Furry Fandom Network Mapper

Maps the furry fandom social network on Bluesky using a two-phase BFS algorithm.

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Get Bluesky App Password

1. Log into Bluesky
2. Go to Settings → Privacy and Security → App Passwords
3. Click "Add App Password"
4. Name it something like "furry network mapper"
5. Copy the generated password (format: `xxxx-xxxx-xxxx-xxxx`)

### 3. Configure Credentials

Edit `main.py` and replace these lines:

```python
BLUESKY_HANDLE = "your-handle.bsky.social"  # Your Bluesky handle
BLUESKY_APP_PASSWORD = "xxxx-xxxx-xxxx-xxxx"  # Your app password
```

## Usage

Run the script:

```bash
python main.py
```

The script will:
1. **Phase 1**: Start from your account, find all your mutuals, then recursively find their mutuals
2. **Phase 2**: Expand the graph by adding users connected to 3+ existing members

All data is saved to `furry_network.db` (SQLite database).

## Configuration

You can adjust these settings in `main.py`:

- `MIN_CONNECTIONS = 3` - In Phase 2, users need this many connections to be added
- `seed_account` - Change which account to start from
- Uncomment `seed_from_furrylist()` to pre-populate from the furryList bot

## Optional: Seed from furryList

The furryList bot tracks ~63K furries. To use it as a starting point, uncomment these lines in `main.py`:

```python
# Uncomment these lines:
seed_from_furrylist(api, db)
```

This will add all 63K users to your database before Phase 1, giving you a much larger initial graph.

## Output

Data is stored in `furry_network.db` with two tables:

- `users` - User profiles (DID, handle, follower counts, etc.)
- `follows` - Follow relationships (who follows who, mutual status)

You can query this database with any SQLite tool or Python's sqlite3 library.

## Analyzing the Data

Once you have the data, you can analyze it with tools like:
- NetworkX (Python graph analysis library)
- Gephi (visualization software)
- Raw SQL queries on the database

Example to export for Gephi:

```python
import sqlite3
import csv

conn = sqlite3.connect('furry_network.db')

# Export nodes
with open('nodes.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['Id', 'Label', 'FollowerCount'])
    cursor = conn.execute('SELECT did, handle, followers_count FROM users')
    writer.writerows(cursor.fetchall())

# Export edges
with open('edges.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['Source', 'Target', 'Type'])
    cursor = conn.execute('SELECT follower_did, following_did, "Directed" FROM follows')
    writer.writerows(cursor.fetchall())

print("Exported to nodes.csv and edges.csv")
```

## Rate Limiting

The script includes small delays (0.5-1 second) between API calls to be polite. For large crawls, this means:
- ~7,200 users per hour maximum
- A full crawl could take several hours to days

## Notes

- The script saves progress to the database continuously
- You can stop and restart it - it will skip already-crawled users
- DIDs (not handles) are used as unique identifiers since handles can change
- All data is public information available through Bluesky's API
