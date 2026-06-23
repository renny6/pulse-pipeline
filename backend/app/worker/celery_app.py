"""
Celery Application Factory
===========================
[MANDATE — master_system_mandates.md §3 / master_solution_blueprint.md §2]
[MANDATE — zero_trust_security.md §3 — NO PICKLE]

MANDATORY CELERY CONFIGURATION (ALL REQUIRED BY ARCHITECTURE DOCS)
-------------------------------------------------------------------

  [C1] task_serializer='json' / accept_content=['json']
       NEVER use pickle. Pickle allows arbitrary Python object deserialization
       which enables Remote Code Execution (RCE) if an attacker injects a
       crafted Kafka message. JSON-only is the zero-trust serialization mandate.

  [C2] worker_max_tasks_per_child=500
       After 500 tasks, the worker process is gracefully killed and respawned.
       This flushes all memory fragmentation, object allocation creep, and
       stale file descriptor leaks that accumulate in long-lived Python workers.
       [MANDATE — Challenge 9 / master_solution_blueprint.md §2]

  [C3] task_soft_time_limit=25 / task_time_limit=30
       Every task has a hard execution ceiling. A task stalled on a hung DB
       write or network call receives a SoftTimeLimitExceeded exception at 25s
       (allowing graceful cleanup), then a SIGKILL at 30s.
       [MANDATE — Challenge 10 / master_solution_blueprint.md §2]

  [C4] task_acks_late=True + task_reject_on_worker_lost=True
       The Celery task ack is sent AFTER execution, not before. If the worker
       crashes mid-task, the message is re-queued (not silently lost).
       Combined with ON CONFLICT DO NOTHING in the DB, this is safe.

  [C5] broker_transport_options['visibility_timeout'] > task_time_limit
       Prevents Celery from re-queuing a task that's still running (false
       positive due to slow execution). Set to 60s > 30s task_time_limit.

  [C6] worker_prefetch_multiplier=4
       Each worker pre-fetches 4 tasks at a time (balanced between throughput
       and memory overhead). Lower this during high-memory DB write spikes.

  [C7] Pre-fork pool disposal signal
       worker_process_init is connected to dispose_engine() which drops all
       SQLAlchemy connections inherited from the master process before the
       forked child opens its own clean pool.
       [MANDATE — Challenge 8 / master_solution_blueprint.md §3]
"""
from __future__ import annotations

import asyncio
import logging

from celery import Celery
from celery.signals import worker_process_init

from app.config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Celery App Factory
# ---------------------------------------------------------------------------

def create_celery_app() -> Celery:
    """
    Build and configure the Celery application with all mandate-required settings.
    """
    celery_app = Celery(
        "pulse_worker",
        broker=settings.redis_url,   # Redis as the broker
        backend=settings.redis_url,  # Redis as the result backend
    )

    celery_app.conf.update(
        # ------------------------------------------------------------------
        # [C1] Zero-trust serialization — NO PICKLE
        # ------------------------------------------------------------------
        task_serializer="json",
        result_serializer="json",
        accept_content=["json"],
        event_serializer="json",

        # ------------------------------------------------------------------
        # [C2] Worker process recycling — memory leak prevention
        # ------------------------------------------------------------------
        worker_max_tasks_per_child=500,

        # ------------------------------------------------------------------
        # [C3] Hard execution ceilings — zombie task elimination
        # SoftTimeLimitExceeded raised at 25s → cleanup code runs.
        # SIGKILL sent at 30s → guarantees the process is dead.
        # ------------------------------------------------------------------
        task_soft_time_limit=25,
        task_time_limit=30,

        # ------------------------------------------------------------------
        # [C4] Late acknowledgement — no silent message loss on worker crash
        # ------------------------------------------------------------------
        task_acks_late=True,
        task_reject_on_worker_lost=True,

        # ------------------------------------------------------------------
        # [C5] Visibility timeout > task_time_limit to avoid false re-queues
        # ------------------------------------------------------------------
        broker_transport_options={"visibility_timeout": 60},

        # ------------------------------------------------------------------
        # [C6] Prefetch — balanced between throughput and memory
        # ------------------------------------------------------------------
        worker_prefetch_multiplier=4,

        # ------------------------------------------------------------------
        # Timezone & misc
        # ------------------------------------------------------------------
        timezone="UTC",
        enable_utc=True,
        task_track_started=True,

        # Auto-discover tasks in app.worker.tasks
        imports=["app.worker.tasks"],
    )

    return celery_app


# Module-level singleton — imported by tasks.py and the CLI entrypoint
celery_app: Celery = create_celery_app()


# ---------------------------------------------------------------------------
# [C7] Pre-fork DB connection pool teardown
# [MANDATE — master_system_mandates.md §4 / Challenge 8]
#
# When Celery forks a new worker process (prefork pool), the child inherits
# all open file descriptors from the parent, including SQLAlchemy engine
# connection pool sockets. If these are not disposed BEFORE the child opens
# its own connections, the database sees double the expected connections,
# quickly hitting max_connections and rejecting all new session requests.
#
# This signal handler fires INSIDE each newly-forked child process,
# disposing the inherited engine before any task touches the DB.
# ---------------------------------------------------------------------------

@worker_process_init.connect
def _on_worker_process_init(sender=None, **kwargs) -> None:  # noqa: ANN001
    """
    Drop all inherited DB connections AND reset the event loop in the
    forked child process.

    [MANDATE — master_solution_blueprint.md §3]
    'Attach teardown events to celery.signals.worker_process_init to tear
    down and cycle inherited engine pools cleanly.'

    EVENT LOOP RESET
    ----------------
    The forked child inherits the parent's _worker_loop object from tasks.py.
    That loop object belongs to the parent process and must NOT be used in
    the child. reset_worker_loop() discards it so the first task in the child
    creates a fresh, process-local event loop via _get_worker_loop().
    """
    logger.info(
        "[Worker init] Disposing inherited engine connections and resetting "
        "event loop in forked child."
    )

    # Reset the persistent loop BEFORE dispose_engine, because dispose_engine
    # is async and needs a clean loop to run in.
    from app.worker.tasks import reset_worker_loop
    reset_worker_loop()

    from app.db.engine import dispose_engine

    try:
        # Use the freshly reset loop (created by _get_worker_loop inside asyncio.run)
        asyncio.run(dispose_engine())
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "[Worker init] Engine dispose raised (may be None on first fork): %s", exc
        )
