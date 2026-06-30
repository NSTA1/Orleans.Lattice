namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// In-memory, per-tree snapshot of the leaf-materialiser drain lag the WAL GC
/// observed on its most recent pass. Written by
/// <c>LatticeWalGc.ObserveMaterialiserDrainLagAsync</c> and re-read on every
/// tick by <c>WalSaturationSampler</c>; never serialised or persisted.
/// </summary>
/// <param name="LagTicks">
/// Standing head-relative drain lag in <see cref="System.TimeSpan.Ticks"/>:
/// the WAL head HLC's wall clock minus the slowest durable leaf-materialiser
/// checkpoint's wall clock, clamped at zero. Reads zero when the materialiser
/// is caught up to the WAL head (including a quiescent tree), so it never
/// false-trips on an idle-but-healthy tree.
/// </param>
/// <param name="ObservedAtTicks">
/// <see cref="System.DateTimeOffset.UtcTicks"/> at which the GC recorded this
/// level. The sampler treats an observation older than its staleness window as
/// absent so a stale "lagging" reading cannot pin the regime after the GC
/// stops refreshing it.
/// </param>
internal readonly record struct MaterialiserDrainLagLevel(long LagTicks, long ObservedAtTicks);
