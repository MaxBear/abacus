"""The clock the supervisor runs on: six durations and the one rule between them.

Its own module rather than a block inside `worker/supervisor.py`, because of who
builds it. `worker/__main__.py` constructs one from `Settings` before there is a
supervisor to hand it to, and every test in this phase constructs one to collapse
the production timings into milliseconds — neither of those wants the process
machinery, the object store, or an event loop to come along with the import.

The durations are `timedelta` rather than seconds-as-floats, so a caller cannot
pass 30 where 30.0 minutes was meant and discover it at the moment a lease is
lost. `Settings` holds the deployed numbers as plain floats, which is what an
operator can set in an environment variable; `from_settings` is the one place
that conversion happens.
"""

from dataclasses import dataclass
from datetime import timedelta

from core.config import Settings


@dataclass(frozen=True, slots=True)
class WorkerConfig:
    """The clock the supervisor runs on. Every value is a duration.

    Defaults are the testable ones rather than the deployed ones — a suite that
    had to configure four timers to assert anything would configure them
    inconsistently. `from_settings` carries the production numbers, which live
    in `Settings` where an operator can reach them.
    """

    lease: timedelta = timedelta(seconds=60)
    extend_every: timedelta = timedelta(seconds=20)
    solve_timeout: timedelta = timedelta(minutes=10)
    grace: timedelta = timedelta(seconds=5)
    reserve_wait: timedelta = timedelta(seconds=5)
    retry_in: timedelta = timedelta(seconds=30)

    # How long the run loop pauses after an error it could not attribute to a
    # job — a broker that is down, most plausibly. Without it a `reserve` that
    # raises immediately becomes a hot loop against a service already in trouble.
    error_pause: timedelta = timedelta(seconds=1)

    def __post_init__(self) -> None:
        # Checked rather than documented because the failure it prevents is
        # silent: a heartbeat at or past the lease renews a claim that has
        # already lapsed, which works right up until another worker is quick
        # enough to take it, and then loses a solve for reasons that look like
        # a broker fault.
        if self.extend_every >= self.lease:
            raise ValueError(
                f"extend_every ({self.extend_every}) must be inside lease ({self.lease}): "
                f"a heartbeat that only fires when the lease is already spent defends nothing"
            )
        if self.grace <= timedelta() or self.solve_timeout <= timedelta():
            raise ValueError("grace and solve_timeout must be positive")

    @classmethod
    def from_settings(cls, settings: Settings) -> "WorkerConfig":
        return cls(
            lease=timedelta(seconds=settings.worker_lease_seconds),
            extend_every=timedelta(seconds=settings.worker_extend_interval_seconds),
            solve_timeout=timedelta(seconds=settings.worker_solve_timeout_seconds),
            grace=timedelta(seconds=settings.worker_grace_seconds),
            reserve_wait=timedelta(seconds=settings.worker_reserve_wait_seconds),
            retry_in=timedelta(seconds=settings.worker_retry_backoff_seconds),
        )
