import sqlite3
import networkx as nx
import matplotlib.pyplot as plt
from collections import Counter

def load_graph_from_db(db_path='furry_network.db'):
    """Load the network from SQLite into a NetworkX graph"""
    conn = sqlite3.connect(db_path)
    
    # Create directed graph
    G = nx.DiGraph()
    
    # Add nodes (users)
    print("Loading users...")
    cursor = conn.execute('''
        SELECT did, handle, display_name, followers_count, follows_count 
        FROM users
    ''')
    
    for did, handle, display_name, followers_count, follows_count in cursor:
        G.add_node(did, 
                   handle=handle,
                   display_name=display_name or handle,
                   followers=followers_count or 0,
                   following=follows_count or 0)
    
    print(f"Loaded {G.number_of_nodes()} users")
    
    # Add edges (follows)
    print("Loading follows...")
    cursor = conn.execute('''
        SELECT follower_did, following_did, is_mutual 
        FROM follows
    ''')
    
    for follower, following, is_mutual in cursor:
        G.add_edge(follower, following, mutual=bool(is_mutual))
    
    print(f"Loaded {G.number_of_edges()} follow relationships")
    
    conn.close()
    return G

def analyze_graph(G):
    """Compute basic statistics about the graph"""
    print("\n" + "="*50)
    print("GRAPH STATISTICS")
    print("="*50)
    
    print(f"Nodes (users): {G.number_of_nodes()}")
    print(f"Edges (follows): {G.number_of_edges()}")
    
    # Mutual follows
    mutual_count = sum(1 for u, v, d in G.edges(data=True) if d.get('mutual', False))
    print(f"Mutual follows: {mutual_count}")
    
    # Density
    density = nx.density(G)
    print(f"Graph density: {density:.4f}")
    
    # Degree statistics
    in_degrees = [d for n, d in G.in_degree()]
    out_degrees = [d for n, d in G.out_degree()]
    
    print(f"\nDegree statistics:")
    print(f"  Average in-degree (followers): {sum(in_degrees)/len(in_degrees):.2f}")
    print(f"  Average out-degree (following): {sum(out_degrees)/len(out_degrees):.2f}")
    print(f"  Max in-degree: {max(in_degrees)}")
    print(f"  Max out-degree: {max(out_degrees)}")
    
    # Most followed users
    print(f"\nTop 10 most followed users in graph:")
    top_followed = sorted(G.in_degree(), key=lambda x: x[1], reverse=True)[:10]
    for did, degree in top_followed:
        handle = G.nodes[did].get('handle', 'unknown')
        display = G.nodes[did].get('display_name', handle)
        print(f"  {display} (@{handle}): {degree} followers")
    
    # Connectivity
    if G.number_of_nodes() > 0:
        # For directed graph, check weak connectivity
        num_components = nx.number_weakly_connected_components(G)
        print(f"\nWeakly connected components: {num_components}")
        
        if num_components > 1:
            largest = max(nx.weakly_connected_components(G), key=len)
            print(f"  Largest component size: {len(largest)} ({100*len(largest)/G.number_of_nodes():.1f}%)")

def visualize_graph(G, output_file='graph_visualization.png', max_nodes=500):
    """Create a simple visualization of the graph"""
    print(f"\nCreating visualization...")
    
    # If graph is too large, sample it
    if G.number_of_nodes() > max_nodes:
        print(f"Graph has {G.number_of_nodes()} nodes, sampling {max_nodes} most connected...")
        # Get top N most connected nodes
        degree_dict = dict(G.degree())
        top_nodes = sorted(degree_dict.items(), key=lambda x: x[1], reverse=True)[:max_nodes]
        top_node_ids = [node for node, degree in top_nodes]
        G_vis = G.subgraph(top_node_ids).copy()
    else:
        G_vis = G
    
    # Create figure
    plt.figure(figsize=(20, 20))
    
    # Layout - spring layout works well for social networks
    print("Computing layout (this may take a minute)...")
    pos = nx.spring_layout(G_vis, k=0.5, iterations=50)
    
    # Node sizes based on degree
    node_sizes = [G_vis.degree(node) * 10 for node in G_vis.nodes()]
    
    # Draw
    nx.draw_networkx_nodes(G_vis, pos, 
                          node_size=node_sizes,
                          node_color='lightblue',
                          alpha=0.6)
    
    nx.draw_networkx_edges(G_vis, pos, 
                          alpha=0.2,
                          arrows=True,
                          arrowsize=5,
                          width=0.5)
    
    # Labels for high-degree nodes only
    degree_dict = dict(G_vis.degree())
    high_degree_threshold = sorted(degree_dict.values(), reverse=True)[min(20, len(degree_dict)-1)]
    labels = {node: G_vis.nodes[node].get('handle', '')[:15] 
              for node in G_vis.nodes() 
              if degree_dict[node] >= high_degree_threshold}
    
    nx.draw_networkx_labels(G_vis, pos, labels, font_size=8)
    
    plt.title(f"Furry Fandom Network Graph ({G_vis.number_of_nodes()} nodes)", fontsize=16)
    plt.axis('off')
    plt.tight_layout()
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"Saved visualization to {output_file}")

def export_for_gephi(G, nodes_file='nodes.csv', edges_file='edges.csv'):
    """Export graph data for Gephi visualization"""
    import csv
    
    print(f"\nExporting for Gephi...")
    
    # Export nodes
    with open(nodes_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Id', 'Label', 'Handle', 'Followers', 'Following'])
        for node in G.nodes():
            writer.writerow([
                node,
                G.nodes[node].get('display_name', 'Unknown'),
                G.nodes[node].get('handle', 'unknown'),
                G.nodes[node].get('followers', 0),
                G.nodes[node].get('following', 0)
            ])
    
    # Export edges
    with open(edges_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Source', 'Target', 'Type', 'Mutual'])
        for u, v, data in G.edges(data=True):
            writer.writerow([
                u,
                v,
                'Directed',
                1 if data.get('mutual', False) else 0
            ])
    
    print(f"Exported to {nodes_file} and {edges_file}")
    print("Import these into Gephi for advanced visualization!")

def find_communities(G):
    """Find communities in the network"""
    print("\nFinding communities...")
    
    # Convert to undirected for community detection
    G_undirected = G.to_undirected()
    
    # Use Louvain community detection
    try:
        import community.community_louvain as community_louvain
        communities = community_louvain.best_partition(G_undirected)
        
        # Count communities
        community_counts = Counter(communities.values())
        print(f"Found {len(community_counts)} communities")
        print(f"Largest community: {max(community_counts.values())} users")
        
        # Add community info to nodes
        for node in G.nodes():
            G.nodes[node]['community'] = communities.get(node, -1)
        
        return communities
    except ImportError:
        print("Install python-louvain for community detection: pip install python-louvain")
        return None

def main():
    # Load graph
    G = load_graph_from_db()
    
    # Analyze
    analyze_graph(G)
    
    # Find communities (optional)
    find_communities(G)
    
    # Visualize
    visualize_graph(G)
    
    # Export for Gephi
    export_for_gephi(G)
    
    print("\n" + "="*50)
    print("Analysis complete!")
    print("="*50)

if __name__ == "__main__":
    main()