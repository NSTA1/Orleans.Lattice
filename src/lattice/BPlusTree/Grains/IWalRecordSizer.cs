namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Allocation-free sizer that reports the exact serialised byte length
/// of a single <see cref="WalRecord"/> under the WAL grain's chosen wire
/// format. Consumed by <see cref="WalShardGrain"/> on every
/// <c>AppendAsync</c> to decide whether the in-progress batch must cut
/// over before the new entry is admitted; the result of every call
/// participates in the per-batch byte budget compared against
/// <see cref="LatticeOptions.WalMaxBatchBytes"/>.
/// <para>
/// Replaces the historical heuristic
/// (<c>key.Length * 2 + value.Length + 128</c>) that under-counted
/// large <see cref="WalRecord.VectorClock"/> payloads and over-counted
/// small-key entries with no vector clock. The Azure Table Storage
/// 4 MB transactional-batch ceiling - the reason
/// <see cref="LatticeOptions.WalMaxBatchBytes"/> defaults to 4 MB in
/// the first place - has zero tolerance for under-counts (a single
/// batch over the limit fails the entire transaction), so the
/// production path must use a true byte count rather than an estimate.
/// </para>
/// <para>
/// Implementations must be safe for concurrent invocation from
/// multiple threads. The default
/// <see cref="OrleansBinaryWalRecordSizer"/> wraps the
/// thread-safe <c>Serializer&lt;WalRecord&gt;</c> from
/// <c>Orleans.Serialization</c>; replacement implementations are
/// expected to be similarly cheap (no per-call heap allocation in the
/// steady-state path).
/// </para>
/// </summary>
internal interface IWalRecordSizer
{
    /// <summary>
    /// Returns the exact serialised byte length of
    /// <paramref name="entry"/> under the WAL grain's wire format,
    /// without retaining the encoded bytes. The call must be
    /// allocation-free in the steady-state path so it can be invoked on
    /// every <c>AppendAsync</c> without inflating the commit-time hot
    /// path.
    /// </summary>
    int Measure(WalRecord entry);
}
