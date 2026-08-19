using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Typed value-surface accessor for a <see cref="Orleans.Lattice.VersionVector"/>
/// stored under a single key in an <see cref="ILattice"/>. Mutating
/// methods read-modify-write under optimistic concurrency, retrying on
/// CAS failure up to a configurable budget.
/// </summary>
public readonly record struct VersionVectorAccessor
{
    /// <summary>Default CAS retry budget for mutating operations.</summary>
    public const int DefaultMaxAttempts = 16;

    private readonly ILattice _lattice;
    private readonly string _key;

    internal VersionVectorAccessor(ILattice lattice, string key)
    {
        _lattice = lattice;
        _key = key;
    }

    /// <summary>The tree the accessor is bound to.</summary>
    public ILattice Lattice => _lattice;

    /// <summary>The key the accessor reads and writes.</summary>
    public string Key => _key;

    /// <summary>
    /// Reads the current vector state. Returns an empty <see cref="Orleans.Lattice.VersionVector"/>
    /// when the key is absent or tombstoned.
    /// </summary>
    public async Task<VersionVector> GetAsync(CancellationToken cancellationToken = default)
    {
        EnsureInitialised();
        var bytes = await _lattice.GetAsync(_key, cancellationToken).ConfigureAwait(false);
        return Decode(bytes);
    }

    /// <summary>
    /// Advances the entry for <paramref name="replicaId"/> via
    /// <see cref="VersionVector.Tick(string)"/> and persists the result.
    /// </summary>
    public Task TickAsync(string replicaId, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        EnsureInitialised();
        return MutateAsync(v => TickDelta(v, replicaId), cancellationToken, maxAttempts);
    }

    /// <summary>
    /// Advances the entry for <paramref name="replicaId"/> and stamps the whole
    /// entry with a per-entry time-to-live of <paramref name="ttl"/>. The expiry
    /// is resolved to an absolute UTC instant on the handling silo and folded
    /// under the max-absolute-ticks convergence rule, so re-writing with a later
    /// <paramref name="ttl"/> extends the entry's life and a durable (no-TTL)
    /// write leaves any existing expiry unchanged. Once the instant passes the
    /// whole vector reads as absent and is reaped by tombstone compaction.
    /// </summary>
    /// <param name="replicaId">The replica whose entry advances. Must be non-empty.</param>
    /// <param name="ttl">The positive time-to-live for the entry.</param>
    /// <param name="cancellationToken">Cancels the read and write hops.</param>
    /// <param name="maxAttempts">Reserved for API parity; the delta apply does not retry.</param>
    public Task TickAsync(string replicaId, TimeSpan ttl, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        EnsureInitialised();
        return MutateAsync(v => TickDelta(v, replicaId), cancellationToken, maxAttempts, ttl);
    }

    /// <summary>
    /// Stages a tick as a <see cref="LatticeStagedCrdtWrite"/> for a cross-tree
    /// atomic write instead of applying it now. The advanced clock is identical
    /// to <see cref="TickAsync(string, CancellationToken, int)"/>'s; add the
    /// returned token to a builder slice via
    /// <see cref="LatticeAtomicWriteBuilder.Set(LatticeStagedCrdtWrite)"/> on a
    /// version-vector-mode tree. See <see cref="LatticeStagedCrdtWrite"/> for the
    /// merge-mode-matching, single-cluster concurrent-writer, and compensation
    /// contract.
    /// </summary>
    /// <param name="replicaId">The replica whose entry advances. Must be non-empty.</param>
    /// <param name="cancellationToken">Cancels the snapshot read.</param>
    public Task<LatticeStagedCrdtWrite> StageTickAsync(string replicaId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        EnsureInitialised();
        return StageAsync(v => TickDelta(v, replicaId), cancellationToken);
    }

    /// <summary>Mints the tick delta advancing <paramref name="replicaId"/>'s entry in <paramref name="v"/>.</summary>
    private static VersionVectorDelta TickDelta(VersionVector v, string replicaId)
    {
        var clock = v.Tick(replicaId);
        return new VersionVectorDelta
        {
            Entries = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal) { [replicaId] = clock },
        };
    }

    /// <summary>Merges <paramref name="other"/> into the stored state under CAS.</summary>
    public Task MergeAsync(VersionVector other, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentNullException.ThrowIfNull(other);
        EnsureInitialised();
        return MutateAsync(v =>
        {
            v.MergeFrom(other);
            return new VersionVectorDelta
            {
                Entries = new Dictionary<string, HybridLogicalClock>(other.Entries, StringComparer.Ordinal),
            };
        }, cancellationToken, maxAttempts);
    }

    private async Task MutateAsync<TDelta>(
        Func<VersionVector, TDelta> mutate,
        CancellationToken cancellationToken,
        int maxAttempts,
        TimeSpan ttl = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);
        // CAS-free producer-side delta apply: see
        // PnCounterAccessor.MutateAsync for the single-read +
        // ApplyCrdtDeltaAsync rationale. Per-entry merge inside the
        // leaf takes the max HLC, so a stale local snapshot is
        // harmless - the worst case is a slightly lower tick that the
        // leaf will subsume on merge.
        _ = maxAttempts;
        cancellationToken.ThrowIfCancellationRequested();
        var current = await GetAsync(cancellationToken).ConfigureAwait(false);
        var delta = mutate(current);
        var deltaBytes = JsonLatticeSerializer<TDelta>.Default.Serialize(delta);
        if (ttl <= TimeSpan.Zero)
            await _lattice.ApplyCrdtDeltaAsync(_key, LatticeMergeMode.VersionVector, deltaBytes, cancellationToken).ConfigureAwait(false);
        else
            await _lattice.ApplyCrdtDeltaAsync(_key, LatticeMergeMode.VersionVector, deltaBytes, ttl, cancellationToken).ConfigureAwait(false);
    }

    private static VersionVector Decode(byte[]? bytes) =>
        bytes is null ? new VersionVector() : JsonLatticeSerializer<VersionVector>.Default.Deserialize(bytes);

    private async Task<LatticeStagedCrdtWrite> StageAsync(
        Func<VersionVector, VersionVectorDelta> mint,
        CancellationToken cancellationToken)
    {
        // Mint-once: a single read mints the typed delta, folds it into the
        // snapshot to produce the merged state, and serialises both. The mint
        // closure advances the snapshot's entry and the delta carries the same
        // clock, so the follow-up MergeDelta is an idempotent pointwise-max. No
        // ApplyCrdtDeltaAsync is issued here - the cross-tree saga performs the
        // durable write and replays the persisted delta verbatim.
        cancellationToken.ThrowIfCancellationRequested();
        var snapshot = await GetAsync(cancellationToken).ConfigureAwait(false);
        var delta = mint(snapshot);
        snapshot.MergeDelta(delta);
        var value = JsonLatticeSerializer<VersionVector>.Default.Serialize(snapshot);
        var deltaBytes = JsonLatticeSerializer<VersionVectorDelta>.Default.Serialize(delta);
        return new LatticeStagedCrdtWrite(_key, value, deltaBytes);
    }

    private void EnsureInitialised()
    {
        if (_lattice is null)
        {
            throw new InvalidOperationException(
                "VersionVectorAccessor is uninitialised; obtain it via ILattice.VersionVector(key) instead of `default`.");
        }
    }
}
