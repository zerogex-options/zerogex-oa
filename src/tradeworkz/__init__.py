"""TradeWorkz™ — multi-bot signaled-trading engine.

Each bot in ``src.tradeworkz.bots`` is a self-contained strategy: it decides
when to enter, how big to size, when to add to or cut its position, and when
to close. Bots share nothing but the market context and their own capital
sleeve — they compete for a spot on the leaderboard at ``/trading-signals``.

Public entrypoints:

* :func:`src.tradeworkz.engine.tick` — one reconcile cycle for the fleet.
* :func:`src.tradeworkz.registry.get_bot` — resolve a bot instance by id.
* :func:`src.tradeworkz.registry.provision_defaults` — seed the default
  roster into ``tw_bots`` + ``tw_bot_capital`` on first boot.
"""
