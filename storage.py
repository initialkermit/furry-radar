import sqlite3
from datetime import datetime

class FurryNetworkDB:
    def __init__(self, db_path='furry_network.db'):
        self.conn = sqlite3.connect(db_path)
        self.create_tables()
    
    def create_tables(self):
        """Create tables if they don't exist"""
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                did TEXT PRIMARY KEY,
                handle TEXT,
                display_name TEXT,
                followers_count INTEGER,
                follows_count INTEGER,
                description TEXT,
                crawled BOOLEAN DEFAULT 0,
                is_mutual_core BOOLEAN DEFAULT 0,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS follows (
                follower_did TEXT,
                follower_handle TEXT,
                following_did TEXT,
                following_handle TEXT,
                is_mutual BOOLEAN DEFAULT 0,
                PRIMARY KEY (follower_did, following_did),
                FOREIGN KEY (follower_did) REFERENCES users(did),
                FOREIGN KEY (following_did) REFERENCES users(did)
            )
        ''')
        
        # Create indexes for faster lookups
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_crawled ON users(crawled)
        ''')
        
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_follower ON follows(follower_did)
        ''')
        
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_following ON follows(following_did)
        ''')
        
        self.conn.commit()
    
    def add_user(self, did, handle, display_name=None, followers_count=0, follows_count=0, description=None):
        """Add or update a user in the database (only update if we have better data)"""
        # Check if user exists
        cursor = self.conn.execute('''
            SELECT followers_count, follows_count, description, display_name, crawled, is_mutual_core
            FROM users WHERE did = ?
        ''', (did,))
        existing = cursor.fetchone()
        
        crawled = 0  # Default for new users
        is_mutual_core = 0  # Default for new users
        if existing:
            # User exists - only update if new data is more complete
            existing_followers, existing_follows, existing_desc, existing_display, existing_crawled, existing_mutual_core = existing
            crawled = existing_crawled  # Preserve crawled status
            is_mutual_core = existing_mutual_core  # Preserve mutual core status
            
            # Keep existing data if new data is incomplete (None or 0)
            if followers_count == 0 and existing_followers > 0:
                followers_count = existing_followers
            if follows_count == 0 and existing_follows > 0:
                follows_count = existing_follows
            if description is None and existing_desc is not None:
                description = existing_desc
            if display_name is None and existing_display is not None:
                display_name = existing_display
        
        # Insert or update
        self.conn.execute('''
            INSERT OR REPLACE INTO users 
            (did, handle, display_name, followers_count, follows_count, description, crawled, is_mutual_core)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (did, handle, display_name, followers_count, follows_count, description, crawled, is_mutual_core))
        self.conn.commit()
    
    def add_follow(self, follower_did, follower_handle, following_did, following_handle, is_mutual=False):
        """Add a follow relationship"""
        self.conn.execute('''
            INSERT OR IGNORE INTO follows 
            (follower_did, follower_handle, following_did, following_handle, is_mutual)
            VALUES (?, ?, ?, ?, ?)
        ''', (follower_did, follower_handle, following_did, following_handle, is_mutual))
        self.conn.commit()
    
    def mark_as_crawled(self, did):
        """Mark a user as having been crawled"""
        self.conn.execute('''
            UPDATE users SET crawled = 1 WHERE did = ?
        ''', (did,))
        self.conn.commit()
    
    def mark_as_mutual_core(self, did):
        """Mark a user as part of the Phase 1 mutual core"""
        self.conn.execute('''
            UPDATE users SET is_mutual_core = 1 WHERE did = ?
        ''', (did,))
        self.conn.commit()
    
    def is_crawled(self, did):
        """Check if a user has already been crawled"""
        cursor = self.conn.execute('''
            SELECT crawled FROM users WHERE did = ?
        ''', (did,))
        result = cursor.fetchone()
        return result and result[0] == 1
    
    def get_uncrawled_users(self, limit=None):
        """Get list of users that haven't been crawled yet"""
        query = 'SELECT did, handle FROM users WHERE crawled = 0'
        if limit:
            query += f' LIMIT {limit}'
        cursor = self.conn.execute(query)
        return cursor.fetchall()
    
    def get_user_followers(self, did):
        """Get all DIDs that follow this user"""
        cursor = self.conn.execute('''
            SELECT follower_did FROM follows WHERE following_did = ?
        ''', (did,))
        return [row[0] for row in cursor.fetchall()]
    
    def get_user_following(self, did):
        """Get all DIDs this user follows"""
        cursor = self.conn.execute('''
            SELECT following_did FROM follows WHERE follower_did = ?
        ''', (did,))
        return [row[0] for row in cursor.fetchall()]
    
    def get_connection_count(self, did):
        """Get number of connections (followers + following) a user has in our graph"""
        cursor = self.conn.execute('''
            SELECT COUNT(*) FROM (
                SELECT follower_did FROM follows WHERE following_did = ?
                UNION
                SELECT following_did FROM follows WHERE follower_did = ?
            )
        ''', (did, did))
        return cursor.fetchone()[0]
    
    def get_mutual_core_connection_count(self, did):
        """Get number of connections to mutual core members specifically"""
        cursor = self.conn.execute('''
            SELECT COUNT(DISTINCT connected_did) FROM (
                SELECT f.follower_did as connected_did
                FROM follows f
                JOIN users u ON f.follower_did = u.did
                WHERE f.following_did = ? AND u.is_mutual_core = 1
                UNION
                SELECT f.following_did as connected_did
                FROM follows f
                JOIN users u ON f.following_did = u.did
                WHERE f.follower_did = ? AND u.is_mutual_core = 1
            )
        ''', (did, did))
        return cursor.fetchone()[0]
    
    def user_exists(self, did):
        """Check if user exists in database"""
        cursor = self.conn.execute('SELECT 1 FROM users WHERE did = ?', (did,))
        return cursor.fetchone() is not None
    
    def get_stats(self):
        """Get database statistics"""
        cursor = self.conn.execute('SELECT COUNT(*) FROM users')
        total_users = cursor.fetchone()[0]
        
        cursor = self.conn.execute('SELECT COUNT(*) FROM users WHERE crawled = 1')
        crawled_users = cursor.fetchone()[0]
        
        cursor = self.conn.execute('SELECT COUNT(*) FROM follows')
        total_follows = cursor.fetchone()[0]
        
        cursor = self.conn.execute('SELECT COUNT(*) FROM follows WHERE is_mutual = 1')
        mutual_follows = cursor.fetchone()[0]
        
        return {
            'total_users': total_users,
            'crawled_users': crawled_users,
            'uncrawled_users': total_users - crawled_users,
            'total_follows': total_follows,
            'mutual_follows': mutual_follows
        }
    
    def close(self):
        """Close database connection"""
        self.conn.close()