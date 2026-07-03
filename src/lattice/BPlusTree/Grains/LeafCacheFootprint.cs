namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Diagnostic footprint of a <see cref="LeafCacheGrain"/>'s live cache
/// mirror: the number of retained rows and the aggregate size of their
/// non-null value payloads. Produced by
/// <see cref="LeafCacheGrain.DebugFootprint"/> for the memory-measurement
/// probe (<c>Bench.LeafCacheGrowth</c>) and the future per-activation budget
/// regression test. Not serialized and not part of any grain interface or
/// wire contract.
/// </summary>
/// <param name="EntryCount">Number of rows currently mirrored in the cache.</param>
/// <param name="ValueBytes">
/// Aggregate length of the non-null value payloads across all mirrored rows.
/// This is the dominant, unbounded memory dimension the eviction
/// investigation targets; the bounded per-row LWW-envelope metadata is
/// excluded.
/// </param>
internal readonly record struct LeafCacheFootprint(int EntryCount, long ValueBytes);
