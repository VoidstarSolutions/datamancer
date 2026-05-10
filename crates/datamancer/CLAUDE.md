# datamancer

Subscription and replay layer for market data. Normalizes provider messages into canonical `MarketEvent`s and produces a single ordered event stream.

- All provider-specific concerns are confined here. Once an event leaves `datamancer`, it must be source-agnostic.
- Ordering and determinism: the merged stream must be totally ordered and reproducible from inputs. If a provider emits out-of-order or duplicate messages, normalize at the edge.
