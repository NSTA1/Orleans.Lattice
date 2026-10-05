using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Grains;

internal readonly record struct BootstrapCapturedEntry(
    HybridLogicalClock Timestamp,
    LatticeMergeMode MergeMode);
