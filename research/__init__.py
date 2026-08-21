"""Research-only experiments that live alongside — but never inside — production.

Nothing under ``research/`` is imported by ``src/``.  The dependency arrow
points one way only: research reads production primitives, production never
reads research.  Keeping the packages separate is what makes it safe to run
an experiment against the live analytics database without any possibility of
altering production behavior.
"""
