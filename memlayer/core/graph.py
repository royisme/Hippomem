import os
import json
import logging
from typing import Optional, List, Dict, Any, Tuple
try:
    from falkordb import FalkorDB
except ImportError:
    FalkorDB = None

class GraphAccelerator:
    def __init__(self, host: str = "localhost", port: int = 6379):
        self.enabled = False
        self.client = None
        self.graph = None
        
        if not FalkorDB:
            return

        try:
            self.client = FalkorDB(host=host, port=port)
            self.graph = self.client.select_graph("memlayer")
            # fast ping to check connection
            self.client.connection.ping()
            self.enabled = True
        except Exception as e:
            # Connection failed, accelerator disabled
            self.enabled = False
            # We might want to log this debug but not crash
            logging.debug(f"FalkorDB accelerator disabled: {e}")

    def upsert_node(self, id: str, type: str, title: str, tags: List[str], confidence: float) -> bool:
        """Projects an L2 node into FalkorDB."""
        if not self.enabled:
            return False
            
        try:
            # Cypher query to merge node
            # Using MERGE to handle updates.
            # We store minimal properties for traversal and filtering.
            query = """
            MERGE (n:L2Node {id: $id})
            SET n.type = $type,
                n.title = $title,
                n.tags = $tags,
                n.confidence = $confidence
            """
            params = {
                "id": id,
                "type": type,
                "title": title,
                "tags": tags,
                "confidence": confidence
            }
            self.graph.query(query, params)
            return True
        except Exception as e:
            logging.error(f"FalkorDB upsert_node error: {e}")
            return False

    def upsert_edge(self, from_id: str, to_id: str, rel: str, weight: float = 1.0) -> bool:
        """Projects an L2 edge into FalkorDB."""
        if not self.enabled:
            return False
            
        try:
            # Cypher query to match nodes and merge edge
            # Dynamic relationship type is tricky in parameterized Cypher if not supported directly.
            # FalkorDB usually supports it or we perform string formatting for Rel Type (safe if validated).
            # rel comes from our code/enum, fairly safe, but let's sanitize.
            safe_rel = "".join(x for x in rel if x.isalnum() or x == "_")
            
            query = f"""
            MATCH (a:L2Node {{id: $from_id}}), (b:L2Node {{id: $to_id}})
            MERGE (a)-[r:{safe_rel}]->(b)
            SET r.weight = $weight
            """
            params = {
                "from_id": from_id,
                "to_id": to_id,
                "weight": weight
            }
            self.graph.query(query, params)
            return True
        except Exception as e:
            logging.error(f"FalkorDB upsert_edge error: {e}")
            return False

    def expand(self, seed_id: str, hops: int = 1) -> Optional[Dict]:
        """Traverse the graph from seed_id. Returns simplified graph structure."""
        if not self.enabled:
            return None
            
        try:
            # Cypher traversal
            # 1 hop: MATCH (n {id: $id})-[r]->(m) RETURN m, r
            # 2 hops: MATCH (n {id: $id})-[r1]->(m)-[r2]->(o) ...
            # Variable length path: MATCH (n {id: $id})-[r*1..hops]->(m)
            
            # We want nodes and relationships.
            # This query gets all paths up to `hops` length.
            # We return paths.
            query = f"""
            MATCH p=(n:L2Node {{id: $id}})-[*1..{hops}]->(m)
            RETURN p
            """
            result = self.graph.query(query, {"id": seed_id})
            
            nodes_map = {}
            edges_list = []
            
            for record in result.result_set:
                # Path object
                # FalkorDB python client returns Path objects? Or list of nodes/rels?
                # Usually it returns a Path object if p is returned.
                # Let's inspect the structure. The result set is a list of records.
                # Record[0] is the path.
                path = record[0] 
                # path should have nodes() and relationships()
                
                # Check implementation of FalkorDB client. 
                # Assuming it behaves like Neo4j driver or standard Cypher response.
                # If not, we might need to adjust. 
                # Fallback safe approach: Return distinct nodes and relationships manually if needed.
                
                for node in path.nodes():
                     # node.properties
                     props = node.properties
                     if 'id' in props:
                         nodes_map[props['id']] = props
                
                for rel in path.relationships():
                    # rel.start_node, rel.end_node, rel.type, rel.properties
                    # Note: We need the IDs of start/end nodes.
                    # Usually relationship objects have access to start/end node IDs or objects.
                    
                    # FalkorDB Python Client:
                    # Relationship has `start_node` (GraphNode), `end_node` (GraphNode), `relation` (str), `properties` (dict)
                    
                    # We need to map internal IDs to our UUIDs.
                    # Assuming we can get properties from start_node/end_node.
                    
                    src_id = rel.start_node.properties.get('id')
                    tgt_id = rel.end_node.properties.get('id')
                    
                    if src_id and tgt_id:
                        edges_list.append({
                            "from": src_id,
                            "to": tgt_id,
                            "rel": rel.relation
                        })

            return {
                "nodes": list(nodes_map.values()),
                "edges": edges_list
            }

        except Exception as e:
            logging.error(f"FalkorDB expand error: {e}")
            return None
