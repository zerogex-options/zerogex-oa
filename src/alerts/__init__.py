"""Member alerts.

``wall_detector`` turns the published Call/Put Walls and the 1-minute tape
into wall events (hit / broke / held) with the chop filtered out;
``wall_store`` records them and fans them out to members' delivery channels;
``wall_alert_engine`` is the service loop that drives both. See
``docs/design/wall-alerts.md``.
"""
