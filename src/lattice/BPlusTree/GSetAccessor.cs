using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Typed value-surface accessor for a <see cref="Orleans.Lattice.GSet"/>
/// stored under a single key in an <see cref="ILattice"/>. The accessor is a
/// lightweight, allocation-free wrapper - construct it once via
/// <see cref="CrdtLatticeExtensions.GSet(ILattice, string)"/> and reuse it for
/// any number of operations on the same key.
/// <para>
/// Mutating methods author a typed <see cref="GSetDelta"/> and apply it through
/// the single-writer leaf seam. Two callers operating on the same key from
/// different replicas converge because the underlying merge is the grow-only
/// set lattice (set union); concurrent adds from any number of replicas all
/// survive the merge. The set is grow-only - there is no remove operation by
/// design.
/// </para>
/// </summary>
public readonly record struct GSetAccessor
{
    /// <summary>Default CAS retry budget for mutating operations.</summary>
    public const int DefaultMaxAttempts = 16;

    private readonly ILattice _lattice;
    private readonly string _key;

    internal GSetAccessor(ILattice lattice, string key)
    {
        _lattice = lattice;
        _key = key;
    }

    /// <summary>The tree the accessor is bound to.</summary>
    public ILattice Lattice => _lattice;

    /// <summary>The key the accessor reads and writes.</summary>
    public string Key => _key;

    /// <summary>
    /// Reads the current set state. Returns an empty
    /// <see cref="Orleans.Lattice.GSet"/> when the key is absent.
    /// </summary>
    public async Task<GSet> GetAsync(CancellationToken cancellationToken = default)
    {
        EnsureInitialised();
        var bytes = await _lattice.GetAsync(_key, cancellationToken).ConfigureAwait(false);
        return Decode(bytes);
    }

    /// <summary>
    /// Adds <paramref name="element"/> to the set. Idempotent: adding an
    /// element already present converges to the same state.
    /// </summary>
    /// <param name="element">The element bytes to add. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the read and write hops.</param>
    /// <param name="maxAttempts">Maximum number of CAS retries before giving up.</param>
    public Task AddAsync(byte[] element, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentNullException.ThrowIfNull(element);
        EnsureInitialised();
        return MutateAsync(AddDelta(element), cancellationToken, maxAttempts);
    }

    /// <summary>
    /// Adds <paramref name="element"/> and stamps the whole entry with a
    /// per-entry time-to-live of <paramref name="ttl"/>. The expiry is
    /// resolved to an absolute UTC instant on the handling silo and folded
    /// under the max-absolute-ticks convergence rule, so re-writing with a
    /// later <paramref name="ttl"/> extends the entry's life and a durable
    /// (no-TTL) write leaves any existing expiry unchanged. Once the instant
    /// passes the whole set reads as absent and is reaped by tombstone
    /// compaction.
    /// </summary>
    /// <param name="element">The element bytes to add. Must not be <c>null</c>.</param>
    /// <param name="ttl">The positive time-to-live for the entry.</param>
    /// <param name="cancellationToken">Cancels the read and write hops.</param>
    /// <param name="maxAttempts">Reserved for API parity; the delta apply does not retry.</param>
    public Task AddAsync(byte[] element, TimeSpan ttl, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentNullException.ThrowIfNull(element);
        EnsureInitialised();
        return MutateAsync(AddDelta(element), cancellationToken, maxAttempts, ttl);
    }

    /// <summary>
    /// Stages an add as a <see cref="LatticeStagedCrdtWrite"/> for a cross-tree
    /// atomic write instead of applying it now. Add the returned token to a
    /// builder slice via
    /// <see cref="LatticeAtomicWriteBuilder.Set(LatticeStagedCrdtWrite)"/> on a
    /// G-Set-mode tree. See <see cref="LatticeStagedCrdtWrite"/> for the
    /// merge-mode-matching, single-cluster concurrent-writer, and compensation
    /// contract.
    /// </summary>
    /// <param name="element">The element bytes to add. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the snapshot read.</param>
    public Task<LatticeStagedCrdtWrite> StageAddAsync(byte[] element, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(element);
        EnsureInitialised();
        return StageAsync(AddDelta(element), cancellationToken);
    }

    /// <summary>Returns <c>true</c> when <paramref name="element"/> is a member of the set.</summary>
    public async Task<bool> ContainsAsync(byte[] element, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(element);
        EnsureInitialised();
        var set = await GetAsync(cancellationToken).ConfigureAwait(false);
        return set.Contains(element);
    }

    /// <summary>
    /// Reads the current members of the set as a list of element byte arrays in
    /// the set's deterministic order (see <see cref="GSet.Values"/>).
    /// </summary>
    public async Task<IReadOnlyList<byte[]>> ToListAsync(CancellationToken cancellationToken = default)
    {
        EnsureInitialised();
        var set = await GetAsync(cancellationToken).ConfigureAwait(false);
        return [.. set.Values()];
    }

    /// <summary>
    /// Merges <paramref name="other"/> into the stored state. Useful for
    /// replication consumers that have computed a set state out-of-band and
    /// want to apply it without reading the full set twice.
    /// </summary>
    public Task MergeAsync(GSet other, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentNullException.ThrowIfNull(other);
        EnsureInitialised();
        return MutateAsync(new GSetDelta { Adds = FlattenElements(other) }, cancellationToken, maxAttempts);
    }

    /// <summary>Mints the add delta for <paramref name="element"/>.</summary>
    private static GSetDelta AddDelta(byte[] element) => new()
    {
        Adds = new[] { element },
    };

    private static byte[][] FlattenElements(GSet set)
    {
        if (set.Count == 0) return Array.Empty<byte[]>();
        var result = new byte[set.Count][];
        var i = 0;
        foreach (var element in set.Values())
        {
            result[i++] = element;
        }
        return result;
    }

    private async Task MutateAsync(GSetDelta delta, CancellationToken cancellationToken, int maxAttempts, TimeSpan ttl = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);
        // Producer-side delta apply: the leaf grain is the single writer
        // authority per key, so no inner CAS retry loop is required for
        // convergence (set union is idempotent). The maxAttempts parameter is
        // preserved on the public surface for parity with the other accessors
        // and is validated for an early-failure signal on misconfiguration.
        _ = maxAttempts;
        cancellationToken.ThrowIfCancellationRequested();
        var deltaBytes = JsonLatticeSerializer<GSetDelta>.Default.Serialize(delta);
        if (ttl <= TimeSpan.Zero)
            await _lattice.ApplyCrdtDeltaAsync(_key, LatticeMergeMode.GSet, deltaBytes, cancellationToken).ConfigureAwait(false);
        else
            await _lattice.ApplyCrdtDeltaAsync(_key, LatticeMergeMode.GSet, deltaBytes, ttl, cancellationToken).ConfigureAwait(false);
    }

    private static GSet Decode(byte[]? bytes) =>
        bytes is null ? new GSet() : JsonLatticeSerializer<GSet>.Default.Deserialize(bytes);

    private async Task<LatticeStagedCrdtWrite> StageAsync(GSetDelta delta, CancellationToken cancellationToken)
    {
        // Mint-once: a single read folds the typed delta into the snapshot to
        // produce the merged state, and serialises both. No ApplyCrdtDeltaAsync
        // is issued here - the cross-tree saga performs the durable write and
        // replays the persisted delta verbatim.
        cancellationToken.ThrowIfCancellationRequested();
        var snapshot = await GetAsync(cancellationToken).ConfigureAwait(false);
        snapshot.MergeDelta(delta);
        var value = JsonLatticeSerializer<GSet>.Default.Serialize(snapshot);
        var deltaBytes = JsonLatticeSerializer<GSetDelta>.Default.Serialize(delta);
        return new LatticeStagedCrdtWrite(_key, value, deltaBytes);
    }

    private void EnsureInitialised()
    {
        if (_lattice is null)
        {
            throw new InvalidOperationException(
                "GSetAccessor is uninitialised; obtain it via ILattice.GSet(key) instead of `default`.");
        }
    }
}
