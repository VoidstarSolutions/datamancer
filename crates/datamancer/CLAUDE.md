# datamancer

Subscription and replay layer for market data. Normalizes provider messages into canonical `MarketEvent`s and presents them through a multiplexed client-session stream.

- All provider-specific concerns are confined here. Once an event leaves `datamancer`, it must be source-agnostic.
- Ordering and determinism is **per `(instrument, kind)` pair**: each pair's substream is a total order (source-stamped `seq`) and reproducible from inputs; one instrument's trades and quotes are not ordered against each other, and across pairs the multiplex is arrival-order only (no global order). If a provider emits out-of-order or duplicate messages, normalize at the edge.
