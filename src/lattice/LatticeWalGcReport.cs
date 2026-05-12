using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Result of a single <see cref="ILatticeWalGc.RunOnceAsync"/>
/// pass. Diagnostic only; the GC run is the durable side-effect.
/// </summary>
/// <param name="TreeName">Tree the run targeted.</param>
/// <param name="MinCursor">Minimum reported consumer cursor at the time of the run, or <see langword="null"/> when no consumer has reported.</param>
/// <param name="TtlCeilingHlc">The wall-clock TTL ceiling expressed as an <see cref="HybridLogicalClock"/> (entries with <c>Timestamp &lt;= ceiling</c> are eligible for trim regardless of cursor), or <see langword="null"/> when <see cref="LatticeOptions.WalRetention"/> is unset.</param>
/// <param name="CausalStable">Causal-stable <see cref="VersionVector"/> frontier (pointwise minimum across every consumer that has reported a per-origin vector through the causal+ overload of <see cref="IWalCursorRegistry.ReportCursorAsync(string, string, HybridLogicalClock, VersionVector, CancellationToken)"/>), or <see langword="null"/> when no consumer has reported a vector. When non-<see langword="null"/> the GC AND-s <c>causalStable.DominatesOrEquals(entry.VectorClock)</c> into its trim predicate; legacy entries with a <see langword="null"/> <see cref="LatticeMutation.VectorClock"/> are unaffected.</param>
/// <param name="BlockedFloor">Blocked-floor: the pointwise minimum <see cref="HybridLogicalClock"/> across every consumer that has reported a non-<see langword="null"/> <c>BlockedAtHlc</c> through the blocked-floor overloads of <see cref="IWalCursorRegistry.ReportCursorAsync(string, string, HybridLogicalClock, HybridLogicalClock?, CancellationToken)"/>, or <see langword="null"/> when no consumer currently reports a buffer pin. When non-<see langword="null"/> the GC AND-s <c>entry.Timestamp &lt; blockedFloor</c> (strict-less) into its trim predicate so the producer cannot trim past an entry the receiver still needs to recover from atomic-batch staging buffer state.</param>
/// <param name="ShardsScanned">Number of WAL partitions visited during the run.</param>
/// <param name="EntriesTrimmed">Total number of entries this pass identified as eligible per the GC predicate and asked the storage provider to trim, summed across every partition. Zero when the predicate yielded no trim point or the WAL was already empty up to that point. Under concurrent GC passes for the same tree this counts entries the pass found eligible rather than entries the underlying <see cref="IWalStorageProvider.TrimAsync"/> physically removed from durable storage - a concurrent pass may have already removed some - but <see cref="IWalStorageProvider.TrimAsync"/> is idempotent, so the durable side converges either way.</param>
public readonly record struct LatticeWalGcReport(
    string TreeName,
    HybridLogicalClock? MinCursor,
    HybridLogicalClock? TtlCeilingHlc,
    VersionVector? CausalStable,
    HybridLogicalClock? BlockedFloor,
    int ShardsScanned,
    long EntriesTrimmed);
