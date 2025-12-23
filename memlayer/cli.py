import click
import json
import os
from memlayer.db import init_db, DB_PATH_DEFAULT
from memlayer.models import Scope, EventPayload, EpisodePayload, L2DraftPayload, ArtifactRef
from memlayer.core.ingestion import upsert_event, commit_episode, promote_to_l2, link_memories
from memlayer.core.retrieval import search_memory, expand_memory
from memlayer.core.governance import deprecate_memory, forget_memory, gc_sweep, gc_compact
import subprocess

def load_json(value):
    if value.startswith("@"):
        with open(value[1:], 'r') as f:
            return json.load(f)
    return json.loads(value)

@click.group()
def cli():
    """MemLayer CLI: Lifecycle memory management for agents."""
    pass

@cli.command()
@click.option('--db-path', default=DB_PATH_DEFAULT, help='Path to SQLite database.')
def init(db_path):
    """Initialize the MemLayer database."""
    try:
        init_db(db_path)
        click.echo(json.dumps({"status": "ok", "message": f"Initialized database at {db_path}"}))
    except Exception as e:
        click.echo(json.dumps({"status": "error", "message": str(e)}))

@cli.group()
def event():
    """Event management commands."""
    pass

@event.command('upsert')
@click.option('--scope', required=True, help='JSON string or @file')
@click.option('--payload', required=True, help='JSON string or @file')
@click.option('--idempotency-key', required=True)
@click.option('--distill', is_flag=True, help='Automatically distill to L1')
@click.option('--db-path', default=DB_PATH_DEFAULT)
def event_upsert(scope, payload, idempotency_key, distill, db_path):
    """Upsert an event (L0)."""
    try:
        scope_obj = Scope(**load_json(scope))
        payload_obj = EventPayload(**load_json(payload))
        result = upsert_event(scope_obj, payload_obj, idempotency_key, distill, db_path)
        click.echo(json.dumps({"status": "ok", "data": result}))
    except Exception as e:
        click.echo(json.dumps({"status": "error", "message": str(e)}))

@cli.command()
@click.option('--scope', required=True, help='JSON string or @file')
@click.option('--from-id', required=True, help='Source Memory ID')
@click.option('--to-id', required=True, help='Target Memory ID')
@click.option('--rel', required=True, help='Relationship type')
@click.option('--idempotency-key', required=False)
@click.option('--db-path', default=DB_PATH_DEFAULT)
def link(scope, from_id, to_id, rel, idempotency_key, db_path):
    """Link two L2 memories."""
    try:
        scope_obj = Scope(**load_json(scope))
        result = link_memories(scope_obj, from_id, to_id, rel, 1.0, idempotency_key, db_path)
        click.echo(json.dumps({"status": "ok", "data": result}))
    except Exception as e:
        click.echo(json.dumps({"status": "error", "message": str(e)}))

@cli.command()
@click.option('--scope', required=True, help='JSON string or @file')
@click.option('--draft', required=True, help='JSON string or @file')
@click.option('--artifact', required=True, help='JSON string or @file')
@click.option('--idempotency-key', required=True)
@click.option('--db-path', default=DB_PATH_DEFAULT)
def promote(scope, draft, artifact, idempotency_key, db_path):
    """Promote L1 memory to L2."""
    try:
        scope_obj = Scope(**load_json(scope))
        draft_obj = L2DraftPayload(**load_json(draft))
        artifact_obj = ArtifactRef(**load_json(artifact))
        result = promote_to_l2(scope_obj, draft_obj, artifact_obj, idempotency_key, db_path)
        
        if result.get("status") == "ok":
            click.echo(json.dumps({"status": "ok", "data": result}))
        else:
             click.echo(json.dumps(result)) # Error format is already match
    except Exception as e:
        click.echo(json.dumps({"status": "error", "message": str(e)}))

@cli.command()
@click.option('--scope', required=True, help='JSON string or @file')
@click.option('--id', required=True, help='Memory ID')
@click.option('--reason', required=True, help='Reason for deprecation')
@click.option('--superseded-by', required=False, help='ID of superseding memory')
@click.option('--db-path', default=DB_PATH_DEFAULT)
def deprecate(scope, id, reason, superseded_by, db_path):
    """Deprecate a memory."""
    try:
        scope_obj = Scope(**load_json(scope))
        result = deprecate_memory(scope_obj, id, reason, superseded_by, db_path)
        click.echo(json.dumps({"status": "ok", "data": result}))
    except Exception as e:
        click.echo(json.dumps({"status": "error", "message": str(e)}))

@cli.command()
@click.option('--scope', required=True, help='JSON string or @file')
@click.option('--selector', required=True, help='JSON string or @file')
@click.option('--db-path', default=DB_PATH_DEFAULT)
def forget(scope, selector, db_path):
    """Forget memories by selector (Tombstone)."""
    try:
        scope_obj = Scope(**load_json(scope))
        selector_obj = load_json(selector)
        result = forget_memory(scope_obj, selector_obj, db_path)
        click.echo(json.dumps({"status": "ok", "data": result}))
    except Exception as e:
        click.echo(json.dumps({"status": "error", "message": str(e)}))

@cli.group()
def gc():
    """Garbage collection commands."""
    pass

@gc.command('sweep')
@click.option('--db-path', default=DB_PATH_DEFAULT)
def gc_sweep_cmd(db_path):
    """Sweep expired L0 memories."""
    try:
        result = gc_sweep(db_path)
        click.echo(json.dumps({"status": "ok", "data": result}))
    except Exception as e:
        click.echo(json.dumps({"status": "error", "message": str(e)}))

@gc.command('compact')
@click.option('--scope', required=False, help='JSON string or @file')
@click.option('--db-path', default=DB_PATH_DEFAULT)
def gc_compact_cmd(scope, db_path):
    """Compact L1 memories."""
    try:
        scope_obj = Scope(**load_json(scope)) if scope else None
        result = gc_compact(scope_obj, db_path)
        click.echo(json.dumps({"status": "ok", "data": result}))
    except Exception as e:
        click.echo(json.dumps({"status": "error", "message": str(e)}))

@cli.command()
@click.option('--db-path', default=DB_PATH_DEFAULT)
def doctor(db_path):
    """Check system health (SQLite, FalkorDB)."""
    health = {"sqlite": "unknown", "falkordb": "unknown"}
    
    # Check SQLite
    try:
        if not os.path.exists(os.path.dirname(db_path)):
             health["sqlite"] = "missing_dir"
        else:
             init_db(db_path) # Safe check
             health["sqlite"] = "ok"
    except Exception as e:
        health["sqlite"] = f"error: {e}"
        
    # Check FalkorDB
    try:
        from memlayer.core.graph import GraphAccelerator
        acc = GraphAccelerator()
        if acc.enabled:
            health["falkordb"] = "connected"
        else:
            health["falkordb"] = "disconnected"
    except ImportError:
        health["falkordb"] = "dependency_missing"
    except Exception as e:
        health["falkordb"] = f"error: {e}"
        
    click.echo(json.dumps({"status": "ok", "data": health}))

@cli.group()
def service():
    """Manage external services (FalkorDB)."""
    pass

@service.command('start')
def service_start():
    """Start FalkorDB container."""
    try:
        # Run docker container
        cmd = "docker run -d -p 6379:6379 --name memlayer-falkor falkordb/falkordb"
        subprocess.run(cmd, shell=True, check=True)
        click.echo(json.dumps({"status": "ok", "message": "FalkorDB started"}))
    except subprocess.CalledProcessError:
        click.echo(json.dumps({"status": "error", "message": "Failed to start FalkorDB (Docker required). Check if container name 'memlayer-falkor' already exists."}))
    except Exception as e:
        click.echo(json.dumps({"status": "error", "message": str(e)}))

@service.command('stop')
def service_stop():
    """Stop FalkorDB container."""
    try:
        subprocess.run("docker stop memlayer-falkor && docker rm memlayer-falkor", shell=True, check=True)
        click.echo(json.dumps({"status": "ok", "message": "FalkorDB stopped"}))
    except Exception as e:
        click.echo(json.dumps({"status": "error", "message": str(e)}))

@cli.group()
def episode():
    """Episode management commands."""
    pass

@episode.command('commit')
@click.option('--scope', required=True, help='JSON string or @file')
@click.option('--episode', required=True, help='JSON string or @file')
@click.option('--idempotency-key', required=True)
@click.option('--db-path', default=DB_PATH_DEFAULT)
def episode_commit(scope, episode, idempotency_key, db_path):
    """Commit an episode summary (L1)."""
    try:
        scope_obj = Scope(**load_json(scope))
        payload_obj = EpisodePayload(**load_json(episode))
        result = commit_episode(scope_obj, payload_obj, idempotency_key, db_path)
        click.echo(json.dumps({"status": "ok", "data": result}))
    except Exception as e:
        click.echo(json.dumps({"status": "error", "message": str(e)}))

@cli.command()
@click.option('--scope', required=True, help='JSON string or @file')
@click.option('--query', required=True)
@click.option('--view', default='index', type=click.Choice(['index', 'detail', 'evidence']))
@click.option('--budget', default=1000, type=int)
@click.option('--top-k', default=8, type=int)
@click.option('--filters', required=False, help='JSON string or @file')
@click.option('--db-path', default=DB_PATH_DEFAULT)
def search(scope, query, view, budget, top_k, filters, db_path):
    """Search memory (L1/L2)."""
    try:
        scope_obj = Scope(**load_json(scope))
        filters_obj = load_json(filters) if filters else None
        result = search_memory(scope_obj, query, view, budget, top_k, filters_obj, db_path)
        click.echo(json.dumps({"status": "ok", "data": result}))
    except Exception as e:
        click.echo(json.dumps({"status": "error", "message": str(e)}))

@cli.command()
@click.option('--scope', required=True, help='JSON string or @file')
@click.option('--seed', required=True, help='Seed Memory ID')
@click.option('--hops', default=1, type=int)
@click.option('--view', default='detail', type=click.Choice(['index', 'detail', 'evidence']))
@click.option('--budget', default=1000, type=int)
@click.option('--db-path', default=DB_PATH_DEFAULT)
def expand(scope, seed, hops, view, budget, db_path):
    """Expand memory relations (L2)."""
    try:
        scope_obj = Scope(**load_json(scope))
        result = expand_memory(scope_obj, seed, hops, view, budget, db_path)
        click.echo(json.dumps({"status": "ok", "data": result}))
    except Exception as e:
        click.echo(json.dumps({"status": "error", "message": str(e)}))

if __name__ == '__main__':
    cli()
