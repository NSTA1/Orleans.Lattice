# Recall and accuracy

`Orleans.Lattice.Vector` is an **approximate** index. This page states the
contract, shows how it is verified, and explains the one failure mode that can
degrade it quietly.

## Why the target is published rather than assumed

An approximate index that does not state its accuracy is indistinguishable from a
buggy exact one. Worse, if it is substituted for an exact path without saying so,
a caller keeps trusting a completeness guarantee nobody is providing any more.
This package therefore publishes a floor, asserts it in the ordinary unit lane on
every build, and reports per query which path produced the answer.

## The measured floors

Recall@k is the fraction of the true top-k that the approximate search returned,
measured against a brute-force oracle computed over the same corpus with the same
total ordering, so tie-breaking cannot confound the comparison.

| Corpus shape | Published floor | Measured |
|---|---|---|
| Clustered (real embedding geometry) | recall@10 >= 0.95 | 1.0000 |
| Unclustered (adversarial, independent Gaussian) | recall@10 >= 0.55 | 0.5880 |

Both are asserted. The adversarial figure is reported deliberately so the headline
is not flattered by a friendly corpus: on data with no cluster structure to
exploit, recall tracks the fraction of the corpus scanned almost exactly, which is
the honest floor for any partitioned index.

Real embedding corpora are strongly clustered - that is what an embedding model
produces - so the clustered row is the one that describes production behaviour.

## Recall holds as the corpus grows

This is the property that makes approximate search safe as a default. Recall does
not merely stay acceptable as the corpus grows; it stays high *while the fraction
of the corpus scanned falls*:

| Vectors | Fraction scanned | recall@10 |
|---|---|---|
| 5,000 | 25.4% | 0.9680 |
| 20,000 | 17.0% | 0.9980 |
| 60,000 | 13.1% | 1.0000 |

Stability across seeds 1, 2, 3 and the default is 1.0000 in every case, and
probing every partition reproduces the exact result set (recall 1.0) in both
corpora - which confirms the approximation is entirely in the partition selection
and not in the scoring.

## The probe sweep

Recall is a dial, not a constant. Probing more partitions costs more and returns
more of the true neighbours:

| Probes (of 141 partitions) | Fraction scanned | Clustered recall@10 | Unclustered recall@10 |
|---|---|---|---|
| 1 | 0.7% | 0.3560 | 0.0820 |
| 4 | 2.8% | 0.8460 | 0.1980 |
| 8 | 5.7% | 0.9700 | - |
| 16 | 11.3% | 1.0000 | - |
| 24 (default here) | 17.0% | 1.0000 | 0.5880 |
| 36 | - | - | 0.7260 |
| 71 | - | - | 0.9120 |
| 141 (all) | 100.0% | 1.0000 | 1.0000 |

Measured at 20,000 vectors, dimension 64, k = 10, over 50 queries.

## Distribution drift: the one quiet failure mode

Incremental maintenance keeps the index **correct** indefinitely - every vector is
placed in the cell nearest to it among the trained centroids, and nothing stale is
ever returned - but it cannot keep the cells **descriptive** once the corpus moves
away from the distribution they were trained on.

This matters because the loss is quiet. No record becomes invalid; every
individual placement is still the best available choice given the existing
centroids. The cells simply stop carving the space where the data now lives, so
more of a query's true neighbours fall outside the partitions it probes.

Measured on a workload that replaced a fifth of the corpus with vectors drawn
around a *different* set of cluster centres, recall fell to 0.875.

**The repair is retraining**, and the index tells you when to consider it:

- An update counter since the last training pass (`DurableVectorIndex.UpdatesSinceTraining`)
  is the drift signal.
- A retrain re-partitions the corpus in place. It re-reads nothing, because the
  corpus is already resident.
- Measured: 0.875 drifted, 1.000 after retraining.

For a realistic churn workload - where a re-embed perturbs a document's vector
within its own neighbourhood, which is what re-embedding actually does - recall
after 20% re-embedded, 10% retired and 10% added, with **no** rebuild, is 0.9975
clustered and 0.7875 unclustered. Both comfortably above their floors.

## What a caller sees

Every search reports the mode that produced it:

- **Approximate** - the partitioned path answered, and the recall floors above
  apply.
- **Exhaustive** - the index scored every vector, so the answer is **exact**. This
  happens when the index is still building, or when the corpus is below the
  training minimum.

An index in the building state is **not degraded**. It returns correct results by
exhaustive scan; it is simply slower and not yet sub-linear. It should be surfaced
as warming up, never as an error or as a fallback to a lesser search.

## How to verify the numbers yourself

The recall harness is committed and runs in the ordinary unit lane on every build,
so the published figures cannot drift away from the code. The scale figures
(build time, query latency, bytes per vector, up to 1,000,000 vectors) live in a
benchmark fixture that is gated behind an environment variable so it never slows a
normal test run.

Latency varies by tens of percent between runs on a shared machine and should be
read as a band. Recall does not: it is bit-identical across runs, because the index
is deterministic.
