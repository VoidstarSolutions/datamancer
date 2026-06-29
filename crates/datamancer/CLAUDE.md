# datamancer

Subscription and replay layer for market data. Normalizes provider messages into canonical `MarketEvent`s and presents them through a multiplexed client-session stream.

- All provider-specific concerns are confined here. Once an event leaves `datamancer`, it must be source-agnostic.
- Ordering and determinism is **per symbol**: each instrument's substream is a within-instrument total order (source-stamped `seq`) and reproducible from inputs; across instruments the multiplex is arrival-order only (no cross-instrument/global order). If a provider emits out-of-order or duplicate messages, normalize at the edge.
