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
/// <param name="ByteCeiling">The configured advisory retained-byte ceiling (<see cref="LatticeOptions.WalMaxRetainedBytes"/>) in effect for this run, or <see langword="null"/> when the byte-pressure policy is disabled. Diagnostic echo so a caller can correlate <see cref="RetainedBytesAfter"/> against the threshold without re-reading options.</param>
/// <param name="RetainedBytesBefore">Retained WAL bytes summed across every partition, sampled <i>before</i> this pass's safe trim, or <see langword="null"/> when the byte-pressure policy is disabled or the configured <see cref="IWalStorageProvider"/> does not support byte accounting (<see cref="IWalStorageProvider.GetRetainedByteSizeAsync"/> returned <c>-1</c> for every partition). Compared against <see cref="ByteCeiling"/> to derive <see cref="BytePressureTriggered"/>; the difference <c>RetainedBytesBefore - RetainedBytesAfter</c> is the byte count the pass reclaimed.</param>
/// <param name="RetainedBytesAfter">Retained WAL bytes summed across every partition, sampled <i>after</i> this pass's safe trim, or <see langword="null"/> when the byte-pressure policy is disabled or the configured <see cref="IWalStorageProvider"/> does not support byte accounting. Compared against <see cref="ByteCeiling"/> to derive <see cref="BytePressureOverThreshold"/>.</param>
/// <param name="BytePressureTriggered"><see langword="true"/> when the byte-pressure policy was enabled and <see cref="RetainedBytesBefore"/> exceeded <see cref="ByteCeiling"/> at the start of this pass, so the policy scheduled an advisory byte-pressure trim (and incremented <see cref="LatticeMetrics.StoragePolicyTrimTriggered"/>). The trim never crosses the safe frontier; this flags that the policy acted, not that the ceiling was met. <see langword="false"/> when within the ceiling at entry, the policy is disabled, or byte accounting is unsupported.</param>
/// <param name="BytePressureOverThreshold"><see langword="true"/> when the byte-pressure policy is enabled and <see cref="RetainedBytesAfter"/> still exceeds <see cref="ByteCeiling"/> after this pass's safe trim - i.e. a lagging consumer or a causal-stable pin is holding bytes the policy would otherwise reclaim. This is an advisory signal only: the GC never trims past the safe frontier to honour the ceiling, so an over-threshold report means "the data could not be safely reclaimed", not "the trim failed". <see langword="false"/> when within the ceiling, when the policy is disabled, or when byte accounting is unsupported.</param>
/// <param name="CursorFloorState">Why this pass's consumer-cursor trim branch was or was not usable. <see cref="MinCursor"/> is <see langword="null"/> both when no consumer has ever reported and when an unusable durable materialiser pin short-circuited the floor, so it cannot distinguish an ordinary quiet tree from one that <i>cannot reclaim at all</i>; this states which (issue #2702). Diagnostic and scheduling-facing only - it never changes what a pass is allowed to trim. Defaults to <see cref="WalGcCursorFloorState.Available"/>, the benign value, so a report constructed without it is read as nothing-unusual.</param>
/// <param name="BlockingConsumerId">The consumer id of the durable materialiser pin that short-circuited the cursor floor, or <see langword="null"/> when this pass was not blocked (issue #2734). <see cref="CursorFloorState"/> says a tree cannot reclaim; this says <i>which consumer</i> is holding it, which is the difference between knowing a tree is stranded and being able to do something about it. The floor short-circuits on the first unusable pin it encounters, so this names one blocking consumer and not necessarily the only one: a pass that reports a different id after the named leaf is healed is progress, not a regression, and the condition is cleared only when a pass stops reporting <see cref="WalGcCursorFloorState.BlockedByUnusablePin"/> at all. Consumer ids are structured as <c>{prefix}{treeId}_{grainId}</c> with a <c>_{partition}</c> suffix on a multi-partition leaf, so the blocking leaf is identifiable from this value alone. Diagnostic only - it never changes what a pass is allowed to trim.</param>
public readonly record struct LatticeWalGcReport(
    string TreeName,
    HybridLogicalClock? MinCursor,
    HybridLogicalClock? TtlCeilingHlc,
    VersionVector? CausalStable,
    HybridLogicalClock? BlockedFloor,
    int ShardsScanned,
    long EntriesTrimmed,
    long? ByteCeiling = null,
    long? RetainedBytesBefore = null,
    long? RetainedBytesAfter = null,
    bool BytePressureTriggered = false,
    bool BytePressureOverThreshold = false,
    WalGcCursorFloorState CursorFloorState = WalGcCursorFloorState.Available,
    string? BlockingConsumerId = null);
