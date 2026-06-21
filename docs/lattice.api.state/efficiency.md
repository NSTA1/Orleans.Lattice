# Efficiency

The state API is an always-available read surface, so its **ambient cost must be near zero when no one is looking** and must scale sub-linearly when many observers watch the same thing. Two guarantees deliver that.

## Reader-less clusters sample nothing

Discovery, structure, and entry queries are pull-driven: they do work only when a request arrives, and they do exactly that request's work. The metrics surface is the only one that could sample in the background, and it does not sample at all unless something is subscribed. A cluster with no metric subscribers runs no sampling loop, so installing the state API on an idle cluster adds no recurring overhead.

## Subscribers to the same request share one sampling loop

When several callers subscribe to live metrics for the same request signature, they do **not** each spin up an independent sampling loop. A shared, reference-counted sampler runs **one** loop per distinct request signature and fans each sampled snapshot out to every subscriber. The cost of N subscribers to the same metrics request is the cost of one sampler plus N cheap fan-out hops, not N full samples.

The sampler is reference-counted: the loop starts when the first subscriber for a signature attaches and stops when the last one detaches. Fan-out uses a capacity-one, drop-oldest channel per subscriber, so a slow consumer can never back-pressure the shared loop or the other subscribers - it simply sees the latest snapshot, never a stalled queue.

## Live metric feeds are delta-coalesced

A metrics subscription emits the initial full snapshot, then only the **changes**. The shared sampler publishes a full aggregate map each interval; each subscription's own observer diffs that map against the last one **it** emitted and forwards only a delta snapshot, so trees whose metrics did not move contribute nothing to that subscriber's wire. Keeping the diff per-subscriber (rather than in the shared loop) lets every subscriber start from its own initial full tick regardless of when it attached. A largely-idle cluster with a live dashboard attached produces a small trickle of deltas, not a full re-send every interval.

## Reads do not stall writes

The read surfaces run alongside the write path without contending with it. Entry scans use the core library's snapshot-isolated cursors rather than locking the foreground, and metrics sampling reads aggregate counters rather than walking live state. A cluster under write load with many readers and subscribers attached keeps its writes prompt - this is asserted directly by the package's efficiency guardrail tests.

## What this means in practice

- Installing the package on a cluster nobody is observing costs effectively nothing.
- A dashboard with many panels watching the same tree's metrics drives one sampler, not many.
- A slow or disconnected observer degrades only its own view, never the cluster or its peers.

## Next

- [Surfaces](surfaces.md#metrics) - the metrics request shape and the snapshot / observe split.
- [Client](client.md) - consuming the live feeds.
