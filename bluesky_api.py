from atproto import Client
import time

class BlueskyAPI:
    def __init__(self, handle, app_password):
        """Initialize and login to Bluesky"""
        self.client = Client()
        self.client.login(handle, app_password)
        self.me = self.client.me
        print(f"Logged in as {self.me.handle} (DID: {self.me.did})")
    
    def get_profile(self, actor):
        """Get a user's profile information"""
        try:
            profile = self.client.app.bsky.actor.get_profile({'actor': actor})
            return profile
        except Exception as e:
            print(f"Error getting profile for {actor}: {e}")
            return None
    
    def get_all_follows(self, actor):
        """Get all accounts that this actor follows (with pagination)"""
        follows = []
        cursor = None
        
        while True:
            try:
                params = {
                    'actor': actor,
                    'limit': 100
                }
                if cursor:
                    params['cursor'] = cursor
                
                resp = self.client.app.bsky.graph.get_follows(params)
                follows.extend(resp.follows)
                
                cursor = resp.cursor
                if not cursor:
                    break
                
                # Be polite to the API
                time.sleep(0.05)
                
            except Exception as e:
                print(f"Error getting follows for {actor}: {e}")
                break
        
        return follows
    
    def get_all_followers(self, actor):
        """Get all accounts that follow this actor (with pagination)"""
        followers = []
        cursor = None
        
        while True:
            try:
                params = {
                    'actor': actor,
                    'limit': 100
                }
                if cursor:
                    params['cursor'] = cursor
                
                resp = self.client.app.bsky.graph.get_followers(params)
                followers.extend(resp.followers)
                
                cursor = resp.cursor
                if not cursor:
                    break
                
                # Be polite to the API
                time.sleep(0.5)
                
            except Exception as e:
                print(f"Error getting followers for {actor}: {e}")
                break
        
        return followers
    
    def find_mutuals(self, actor):
        """Find mutual follows for an actor"""
        follows = self.get_all_follows(actor)
        followers = self.get_all_followers(actor)
        
        follows_set = {f.did for f in follows}
        followers_set = {f.did for f in followers}
        
        # Get intersection (mutuals)
        mutuals = follows_set & followers_set
        
        return {
            'follows': follows,
            'followers': followers,
            'mutuals': mutuals
        }
    
    def get_profiles_batch(self, actors):
        """Get multiple profiles at once (up to 25)"""
        try:
            # API only allows 25 at a time
            if len(actors) > 25:
                actors = actors[:25]
            
            resp = self.client.app.bsky.actor.get_profiles({'actors': actors})
            return resp.profiles
        except Exception as e:
            print(f"Error getting batch profiles: {e}")
            return []
