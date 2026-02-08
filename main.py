from bluesky_api import BlueskyAPI
from storage import FurryNetworkDB
import time, os

# Configuration
BLUESKY_HANDLE = os.getenv("BLUESKY_HANDLE")
BLUESKY_APP_PASSWORD = os.getenv("BLUESKY_APP_PASSWORD")  # Replace with your app password

# Phase 2 settings
MIN_CONNECTIONS = 3  # Minimum connections to existing graph members to be added

def phase1_mutuals_graph(api, db, seed_account, max_users=None):
    """
    Phase 1: Build the core mutual network
    
    Starting from seed_account, recursively find mutuals.
    For each mutual:
    - Crawl them (get full profile + all connections)
    - Add ALL their connections (follows + followers) to database
    - Only add mutuals to the to_process queue
    - Mark them as is_mutual_core = 1
    
    Args:
        max_users: Maximum number of users to crawl (None for unlimited)
    """
    print("\n=== PHASE 1: Building Mutual Core Network ===")
    if max_users:
        print(f"Limit: {max_users} users")
    
    # Queue of users to process (BFS) - only mutuals get added here
    to_process = [seed_account]
    processed = set()
    
    while to_process:
        # Check if we've hit the limit
        if max_users and len(processed) >= max_users:
            print(f"\n⚠️  Reached Phase 1 limit of {max_users} users")
            break
        
        current_did = to_process.pop(0)
        
        # Skip if already processed
        if current_did in processed or db.is_crawled(current_did):
            continue
        
        print(f"\nProcessing: {current_did}")
        
        # Get profile
        profile = api.get_profile(current_did)
        if not profile:
            print(f"  Could not fetch profile, skipping")
            continue
        
        # Get counts
        followers_count = getattr(profile, 'followers_count', 0)
        follows_count = getattr(profile, 'follows_count', 0)
        
        # Add user to database
        db.add_user(
            did=profile.did,
            handle=profile.handle,
            display_name=getattr(profile, 'display_name', None),
            followers_count=followers_count,
            follows_count=follows_count,
            description=getattr(profile, 'description', '')
        )
        
        print(f"  {profile.display_name or profile.handle}")
        print(f"  Followers: {followers_count}, Following: {follows_count}")
        
        # Find mutuals
        result = api.find_mutuals(current_did)
        print(f"  Found {len(result['mutuals'])} mutuals")
        print(f"  Debug: {len(result['follows'])} follows, {len(result['followers'])} followers")
        
        # Add ALL connections to database (follows + followers)
        # This includes non-furries, which is fine - we'll filter in Phase 2
        all_connections = {}  # did -> handle mapping
        
        # Add people this user follows
        for follow in result['follows']:
            all_connections[follow.did] = follow.handle
            db.add_user(
                did=follow.did,
                handle=follow.handle,
                display_name=getattr(follow, 'display_name', None)
            )
            # Record follow relationship
            db.add_follow(
                follower_did=current_did,
                follower_handle=profile.handle,
                following_did=follow.did,
                following_handle=follow.handle,
                is_mutual=(follow.did in result['mutuals'])
            )
        
        # Add people who follow this user
        follows_dids = {f.did for f in result['follows']}  # More efficient lookup
        for follower in result['followers']:
            all_connections[follower.did] = follower.handle
            db.add_user(
                did=follower.did,
                handle=follower.handle,
                display_name=getattr(follower, 'display_name', None)
            )
            # Record follow relationship (if not already added above as a mutual)
            if follower.did not in follows_dids:
                db.add_follow(
                    follower_did=follower.did,
                    follower_handle=follower.handle,
                    following_did=current_did,
                    following_handle=profile.handle,
                    is_mutual=False  # Can't be mutual if not in follows
                )
        
        print(f"  Added {len(all_connections)} connections to database")
        
        # Only add MUTUALS to the processing queue
        for mutual_did in result['mutuals']:
            if mutual_did not in processed and not db.is_crawled(mutual_did):
                to_process.append(mutual_did)
        
        # Mark as crawled and as part of mutual core
        db.mark_as_crawled(current_did)
        db.mark_as_mutual_core(current_did)
        processed.add(current_did)
        
        # Print progress
        stats = db.get_stats()
        print(f"  Progress: {stats['crawled_users']} mutual core crawled, {len(to_process)} mutuals in queue, {stats['total_users']} accounts total in DB")
        
        # Small delay to avoid hammering the API (adjust or remove if needed)
        time.sleep(0.01)
    
    print(f"\n✓ Phase 1 complete: {len(processed)} mutual core members crawled")

def phase2_expand_graph(api, db, min_connections=3, max_users=None):
    """
    Phase 2: Expand beyond mutual core by finding connected community members
    
    1. Find all uncrawled users with ≥N connections to mutual core
    2. BFS through them, crawling users who meet the connection threshold
    3. For each crawled user, check their connections for more qualified users
    
    Args:
        min_connections: Minimum connections to mutual core to be included
        max_users: Maximum number of users to crawl in this phase (None for unlimited)
    """
    print(f"\n=== PHASE 2: Expanding Beyond Mutual Core ===")
    print(f"Minimum connections to mutual core: {min_connections}")
    if max_users:
        print(f"Limit: {max_users} users")
    
    # Step 1: Find initial candidates from uncrawled users in database
    print("\nScanning uncrawled users for connections to mutual core...")
    uncrawled = db.get_uncrawled_users()
    print(f"Found {len(uncrawled)} uncrawled users in database")
    
    # Debug: Check mutual core count
    cursor = db.conn.execute('SELECT COUNT(*) FROM users WHERE is_mutual_core = 1')
    mutual_core_count = cursor.fetchone()[0]
    print(f"Debug: {mutual_core_count} users in mutual core")
    
    # Build initial queue of users with sufficient connections
    to_process = []
    checked_count = 0
    for did, handle in uncrawled:
        connection_count = db.get_mutual_core_connection_count(did)
        checked_count += 1
        if checked_count <= 10:  # Show first 10 for debugging
            print(f"  Debug: {handle}: {connection_count} connections to mutual core")
        if connection_count >= min_connections:
            to_process.append(did)
            if checked_count > 10:  # Only print qualified ones after first 10
                print(f"  {handle}: {connection_count} connections - QUEUED")
    
    print(f"\nInitial queue: {len(to_process)} users with ≥{min_connections} connections")
    
    # Step 2: BFS through qualified users
    processed = set()
    crawled_count = 0
    
    while to_process:
        # Check if we've hit the limit
        if max_users and crawled_count >= max_users:
            print(f"\n⚠️  Reached Phase 2 limit of {max_users} users")
            break
        
        current_did = to_process.pop(0)
        
        # Skip if already processed
        if current_did in processed or db.is_crawled(current_did):
            continue
        
        # Get profile
        profile = api.get_profile(current_did)
        if not profile:
            print(f"  Could not fetch profile, skipping")
            processed.add(current_did)
            continue
        
        print(f"\nCrawling: {profile.display_name or profile.handle} (@{profile.handle})")
        
        # Update user info in database
        db.add_user(
            did=profile.did,
            handle=profile.handle,
            display_name=getattr(profile, 'display_name', None),
            followers_count=getattr(profile, 'followers_count', 0),
            follows_count=getattr(profile, 'follows_count', 0),
            description=getattr(profile, 'description', '')
        )
        
        # Get their connections (follows + followers)
        follows = api.get_all_follows(current_did)
        followers = api.get_all_followers(current_did)
        
        print(f"  Found {len(follows)} follows, {len(followers)} followers")
        
        # Process follows
        temp_connections = {}  # did -> handle
        for f in follows:
            temp_connections[f.did] = f.handle
            # Add user to DB if not exists
            if not db.user_exists(f.did):
                db.add_user(did=f.did, handle=f.handle, display_name=getattr(f, 'display_name', None))
            # Record follow relationship
            db.add_follow(
                follower_did=current_did,
                follower_handle=profile.handle,
                following_did=f.did,
                following_handle=f.handle,
                is_mutual=False
            )
        
        # Process followers
        for f in followers:
            temp_connections[f.did] = f.handle
            # Add user to DB if not exists
            if not db.user_exists(f.did):
                db.add_user(did=f.did, handle=f.handle, display_name=getattr(f, 'display_name', None))
            # Record follow relationship
            db.add_follow(
                follower_did=f.did,
                follower_handle=f.handle,
                following_did=current_did,
                following_handle=profile.handle,
                is_mutual=False
            )
        
        print(f"  Checking {len(temp_connections)} connections for candidates...")
        
        # Scan connections for qualified users
        new_candidates = 0
        skipped_already_processed = 0
        skipped_already_crawled = 0
        skipped_in_queue = 0
        skipped_insufficient_connections = 0
        
        for conn_did, conn_handle in temp_connections.items():
            # Skip if already processed or in queue
            if conn_did in processed:
                skipped_already_processed += 1
                continue
            if conn_did in to_process:
                skipped_in_queue += 1
                continue
            if db.is_crawled(conn_did):
                skipped_already_crawled += 1
                continue
            
            # Check connections to mutual core (now they should have recorded relationships!)
            conn_count = db.get_mutual_core_connection_count(conn_did)
            if conn_count >= min_connections:
                to_process.append(conn_did)
                new_candidates += 1
                print(f"    ✓ {conn_handle}: {conn_count} connections to mutual core - QUEUED")
            else:
                skipped_insufficient_connections += 1
                # Show first few examples
                if skipped_insufficient_connections <= 3:
                    print(f"    ✗ {conn_handle}: only {conn_count} connections (need {min_connections})")
        
        print(f"  Results:")
        print(f"    New candidates: {new_candidates}")
        print(f"    Skipped - already processed: {skipped_already_processed}")
        print(f"    Skipped - already in queue: {skipped_in_queue}")
        print(f"    Skipped - already crawled: {skipped_already_crawled}")
        print(f"    Skipped - insufficient connections: {skipped_insufficient_connections}")
        
        # Mark as crawled
        db.mark_as_crawled(current_did)
        processed.add(current_did)
        crawled_count += 1
        
        # Print progress
        print(f"  Progress: {crawled_count} crawled in Phase 2, {len(to_process)} in queue")
        
        time.sleep(0.01)
    
    print(f"\n✓ Phase 2 complete: {crawled_count} additional users crawled")

def main():
    # Initialize
    print("Furry Fandom Network Mapper")
    print("=" * 50)
    
    # Connect to Bluesky
    api = BlueskyAPI(BLUESKY_HANDLE, BLUESKY_APP_PASSWORD)
    
    # Connect to database
    db = FurryNetworkDB()
    
    # Print initial stats
    stats = db.get_stats()
    print(f"\nDatabase stats:")
    print(f"  Total users: {stats['total_users']}")
    print(f"  Crawled users: {stats['crawled_users']}")
    print(f"  Follow relationships: {stats['total_follows']}")
    
    # Starting point - your account
    seed_account = api.me.did
    
    # Run Phase 1: Build mutuals graph (includes furryList automatically!)
    # TEST LIMITS - Remove max_users parameter for full run
    phase1_mutuals_graph(api, db, seed_account, max_users=50)
    
    # Run Phase 2: Expand based on connections to mutual core
    # TEST LIMITS - Remove max_users parameter for full run
    phase2_expand_graph(api, db, min_connections=MIN_CONNECTIONS, max_users=100)
    
    # Final stats
    print("\n" + "=" * 50)
    print("FINAL RESULTS")
    print("=" * 50)
    stats = db.get_stats()
    print(f"Total users in graph: {stats['total_users']}")
    print(f"Users fully crawled: {stats['crawled_users']}")
    print(f"Follow relationships: {stats['total_follows']}")
    print(f"Mutual relationships: {stats['mutual_follows']}")
    print(f"\nDatabase saved to: furry_network.db")
    
    # Close connections
    db.close()

if __name__ == "__main__":
    main()