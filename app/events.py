import asyncio
from typing import Dict, List, Any

_subscribers: Dict[str, List[asyncio.Queue]] = {}

def subscribe(run_id: str) -> asyncio.Queue:
    q: asyncio.Queue = asyncio.Queue()
    _subscribers.setdefault(run_id, []).append(q)
    return q

def publish(run_id: str, event: Dict[str, Any]) -> None:
    for q in _subscribers.get(run_id, []):
        q.put_nowait(event)
