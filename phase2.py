"""
Re-run Phase 2 with different N values to experiment

This script lets you re-run Phase 2 without re-doing Phase 1.
Useful for experimenting with different min_connections thresholds.

Before running this:
1. Complete Phase 1 first with main.py
2. Optionally, reset all non-mutual-core users' crawled status
"""

from bluesky_api import BlueskyAPI
from storage import FurryNetworkDB
from main import phase2_expand_graph
import sys

# Configuration
BLUESKY_HANDLE = "deltaspire.pawbea.nz"  # Replace with your handle
BLUESKY_APP_PASSWORD = "dzie-3t37-drqw-ajro"  # Replace with your app password

def reset_phase2_users(db):
    """Reset crawled status for all non-mutual-core users"""
    print("Resetting Phase 2 users (keeping mutual core intact)...")
    db.conn.execute('''
        UPDATE users 
        SET crawled = 0 
        WHERE is_mutual_core = 0
    ''')
    db.conn.commit()
    
    cursor = db.conn.execute('SELECT COUNT(*) FROM users WHERE crawled = 0')
    count = cursor.fetchone()[0]
    print(f"Reset {count} users to uncrawled status")

def main():
    # Get min_connections from command line or use default
    if len(sys.argv) > 1:
        min_connections = int(sys.argv[1])
    else:
        min_connections = int(input("Enter minimum connections to mutual core (default 3): ") or "3")
    
    # Optional: reset Phase 2 users
    reset = input("Reset Phase 2 users first? (y/n, default n): ").lower()
    
    # Connect
    print("\nConnecting to Bluesky...")
    api = BlueskyAPI(BLUESKY_HANDLE, BLUESKY_APP_PASSWORD)
    
    print("Opening database...")
    db = FurryNetworkDB()
    
    # Show current stats
    stats = db.get_stats()
    print(f"\nCurrent database stats:")
    print(f"  Total users: {stats['total_users']}")
    print(f"  Crawled users: {stats['crawled_users']}")
    
    cursor = db.conn.execute('SELECT COUNT(*) FROM users WHERE is_mutual_core = 1')
    mutual_core_count = cursor.fetchone()[0]
    print(f"  Mutual core members: {mutual_core_count}")
    
    # Reset if requested
    if reset == 'y':
        reset_phase2_users(db)
    
    # Run Phase 2
    print(f"\nRunning Phase 2 with min_connections = {min_connections}")
    phase2_expand_graph(api, db, min_connections=min_connections, max_users=None)
    
    # Final stats
    print("\n" + "=" * 50)
    print("FINAL RESULTS")
    print("=" * 50)
    stats = db.get_stats()
    print(f"Total users in graph: {stats['total_users']}")
    print(f"Users fully crawled: {stats['crawled_users']}")
    print(f"Follow relationships: {stats['total_follows']}")
    print(f"Mutual relationships: {stats['mutual_follows']}")
    
    cursor = db.conn.execute('SELECT COUNT(*) FROM users WHERE is_mutual_core = 1')
    mutual_core_count = cursor.fetchone()[0]
    print(f"Mutual core members: {mutual_core_count}")
    print(f"Phase 2 additions: {stats['crawled_users'] - mutual_core_count}")
    
    db.close()

if __name__ == "__main__":
    main()