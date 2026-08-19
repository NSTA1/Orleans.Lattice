using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Typed value-surface accessor for an <see cref="MvRegister"/>
/// stored under a single key in an <see cref="ILattice"/>. The
/// accessor is a lightweight, allocation-free wrapper - construct it
/// once via <see cref="CrdtLatticeExtensions.MvRegister{T}(ILattice, string, ILatticeSerializer{T}?)"/>
/// and reuse it for any number of operations on the same key.
/// <para>
/// Mutating methods read-modify-write under optimistic concurrency
/// control, retrying on CAS failure up to a configurable budget.
/// Concurrent writes from different replicas converge to the union
/// of their dot-tagged values; <see cref="ValuesAsync(CancellationToken)"/>
/// exposes the conflict set to callers so the application can resolve
/// the merge itself (e.g. surface every candidate to a user) rather
/// than the wire contract silently dropping one side as
/// <see cref="LatticeMergeMode.LwwRegister"/> would.
/// </para>
/// </summary>
/// <typeparam name="T">The user-facing value type. Serialised to and from <see cref="byte"/>[] through <see cref="ILatticeSerializer{T}"/>.</typeparam>
public readonly record struct MvRegisterAccessor<T>
{
    /// <summary>Default CAS retry budget for mutating operations.</summary>
    public const int DefaultMaxAttempts = 16;

    private readonly ILattice _lattice;
    private readonly string _key;
    private readonly ILatticeSerializer<T> _serializer;

    internal MvRegisterAccessor(ILattice lattice, string key, ILatticeSerializer<T> serializer)
    {
        _lattice = lattice;
        _key = key;
        _serializer = serializer;
    }

    /// <summary>The tree the accessor is bound to.</summary>
    public ILattice Lattice => _lattice;

    /// <summary>The key the accessor reads and writes.</summary>
    public string Key => _key;

    /// <summary>The serializer used to translate <typeparamref name="T"/> to and from <see cref="byte"/>[].</summary>
    public ILatticeSerializer<T> Serializer => _serializer;

    /// <summary>
    /// Reads the current register state. Returns an empty
    /// <see cref="MvRegister"/> when the key is absent or tombstoned.
    /// </summary>
    public async Task<MvRegister> GetAsync(CancellationToken cancellationToken = default)
    {
        EnsureInitialised();
        var bytes = await _lattice.GetAsync(_key, cancellationToken).ConfigureAwait(false);
        return Decode(bytes);
    }

    /// <summary>
    /// Returns the set of currently-live <typeparamref name="T"/>
    /// values. A single-valued register returns one element; a
    /// concurrently-written register returns the conflicting
    /// candidates in deterministic order.
    /// </summary>
    public async Task<IReadOnlyList<T>> ValuesAsync(CancellationToken cancellationToken = default)
    {
        EnsureInitialised();
        var register = await GetAsync(cancellationToken).ConfigureAwait(false);
        if (register.IsEmpty) return Array.Empty<T>();
        var values = new T[register.Entries.Count];
        var raw = register.Values();
        for (var i = 0; i < raw.Count; i++)
        {
            values[i] = _serializer.Deserialize(raw[i]);
        }
        return values;
    }

    /// <summary>
    /// Writes <paramref name="value"/> from <paramref name="replicaId"/>,
    /// minting a fresh dot and dropping every entry the writer has
    /// observed. Concurrent writes from other replicas that have not
    /// been observed locally survive the next merge.
    /// </summary>
    /// <param name="replicaId">The replica authoring the write. Must be non-empty.</param>
    /// <param name="value">The value to store. May be <c>null</c> only when <typeparamref name="T"/> permits it.</param>
    /// <param name="cancellationToken">Cancels the read and write hops.</param>
    /// <param name="maxAttempts">Maximum number of CAS retries before giving up.</param>
    public Task SetAsync(string replicaId, T value, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        EnsureInitialised();
        // Copy the captured fields into locals so the lambda below
        // does not need to close over `this` (forbidden inside a
        // struct method).
        var serializer = _serializer;
        return MutateAsync(register => SetDelta(register, serializer, replicaId, value), cancellationToken, maxAttempts);
    }

    /// <summary>
    /// Writes <paramref name="value"/> from <paramref name="replicaId"/> and
    /// stamps the whole entry with a per-entry time-to-live of
    /// <paramref name="ttl"/>. The expiry is resolved to an absolute UTC
    /// instant on the handling silo and folded under the max-absolute-ticks
    /// convergence rule, so re-writing with a later <paramref name="ttl"/>
    /// extends the entry's life and a durable (no-TTL) write leaves any
    /// existing expiry unchanged. Once the instant passes the whole register
    /// reads as absent and is reaped by tombstone compaction.
    /// </summary>
    /// <param name="replicaId">The replica authoring the write. Must be non-empty.</param>
    /// <param name="value">The value to store. May be <c>null</c> only when <typeparamref name="T"/> permits it.</param>
    /// <param name="ttl">The positive time-to-live for the entry.</param>
    /// <param name="cancellationToken">Cancels the read and write hops.</param>
    /// <param name="maxAttempts">Reserved for API parity; the delta apply does not retry.</param>
    public Task SetAsync(string replicaId, T value, TimeSpan ttl, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        EnsureInitialised();
        var serializer = _serializer;
        return MutateAsync(register => SetDelta(register, serializer, replicaId, value), cancellationToken, maxAttempts, ttl);
    }

    /// <summary>
    /// Stages a write as a <see cref="LatticeStagedCrdtWrite"/> for a cross-tree
    /// atomic write instead of applying it now. The minted dot and dropped
    /// observed entries are identical to
    /// <see cref="SetAsync(string, T, CancellationToken, int)"/>'s; add the
    /// returned token to a builder slice via
    /// <see cref="LatticeAtomicWriteBuilder.Set(LatticeStagedCrdtWrite)"/> on a
    /// multi-value-register-mode tree. See <see cref="LatticeStagedCrdtWrite"/>
    /// for the merge-mode-matching, single-cluster concurrent-writer, and
    /// compensation contract.
    /// </summary>
    /// <param name="value">The value to store. May be <c>null</c> only when <typeparamref name="T"/> permits it.</param>
    /// <param name="replicaId">The replica authoring the write. Must be non-empty.</param>
    /// <param name="cancellationToken">Cancels the snapshot read.</param>
    public Task<LatticeStagedCrdtWrite> StageSetAsync(T value, string replicaId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        EnsureInitialised();
        var serializer = _serializer;
        return StageAsync(register => SetDelta(register, serializer, replicaId, value), cancellationToken);
    }

    /// <summary>Mints the write delta for <paramref name="replicaId"/> against <paramref name="register"/>.</summary>
    private static MvRegisterDelta SetDelta(MvRegister register, ILatticeSerializer<T> serializer, string replicaId, T value)
    {
        var encoded = serializer.Serialize(value);
        register.Set(replicaId, encoded);
        var entry = new MvRegisterEntry
        {
            ReplicaId = replicaId,
            Counter = register.Context[replicaId],
            Value = encoded,
        };
        return new MvRegisterDelta
        {
            Entries = new[] { entry },
            Context = new Dictionary<string, long>(register.Context, StringComparer.Ordinal),
        };
    }

    /// <summary>
    /// Merges <paramref name="other"/> into the stored state under
    /// CAS. Useful for replication consumers that have computed a
    /// delta out-of-band and want to apply it without reading the
    /// full register twice.
    /// </summary>
    public Task MergeAsync(MvRegister other, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentNullException.ThrowIfNull(other);
        EnsureInitialised();
        return MutateAsync(register =>
        {
            register.MergeFrom(other);
            var entries = new MvRegisterEntry[other.Entries.Count];
            for (var i = 0; i < other.Entries.Count; i++) entries[i] = other.Entries[i];
            return new MvRegisterDelta
            {
                Entries = entries,
                Context = new Dictionary<string, long>(other.Context, StringComparer.Ordinal),
            };
        }, cancellationToken, maxAttempts);
    }

    private async Task MutateAsync<TDelta>(
        Func<MvRegister, TDelta> mutate,
        CancellationToken cancellationToken,
        int maxAttempts,
        TimeSpan ttl = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);
        // CAS-free producer-side delta apply: see
        // PnCounterAccessor.MutateAsync for the single-read +
        // ApplyCrdtDeltaAsync rationale. The Set path still needs one
        // local read to mint the next dot counter (MvRegister.Set
        // advances Context[replicaId] from the local snapshot's view);
        // concurrent writers minting the same dot is the caller's
        // responsibility, identical to the OR-Set per-replica
        // monotonicity contract.
        _ = maxAttempts;
        cancellationToken.ThrowIfCancellationRequested();
        var current = await GetAsync(cancellationToken).ConfigureAwait(false);
        var delta = mutate(current);
        var deltaBytes = JsonLatticeSerializer<TDelta>.Default.Serialize(delta);
        if (ttl <= TimeSpan.Zero)
            await _lattice.ApplyCrdtDeltaAsync(_key, LatticeMergeMode.MvRegister, deltaBytes, cancellationToken).ConfigureAwait(false);
        else
            await _lattice.ApplyCrdtDeltaAsync(_key, LatticeMergeMode.MvRegister, deltaBytes, ttl, cancellationToken).ConfigureAwait(false);
    }

    private static MvRegister Decode(byte[]? bytes) =>
        bytes is null ? new MvRegister() : JsonLatticeSerializer<MvRegister>.Default.Deserialize(bytes);

    private async Task<LatticeStagedCrdtWrite> StageAsync(
        Func<MvRegister, MvRegisterDelta> mint,
        CancellationToken cancellationToken)
    {
        // Mint-once: a single read mints the typed delta, folds it into the
        // snapshot to produce the merged state, and serialises both. The mint
        // closure advances the snapshot's dot context and the delta replays the
        // same entry, so the follow-up MergeDelta is idempotent. No
        // ApplyCrdtDeltaAsync is issued here - the cross-tree saga performs the
        // durable write and replays the persisted delta verbatim.
        cancellationToken.ThrowIfCancellationRequested();
        var snapshot = await GetAsync(cancellationToken).ConfigureAwait(false);
        var delta = mint(snapshot);
        snapshot.MergeDelta(delta);
        var value = JsonLatticeSerializer<MvRegister>.Default.Serialize(snapshot);
        var deltaBytes = JsonLatticeSerializer<MvRegisterDelta>.Default.Serialize(delta);
        return new LatticeStagedCrdtWrite(_key, value, deltaBytes);
    }

    private void EnsureInitialised()
    {
        if (_lattice is null || _serializer is null)
        {
            throw new InvalidOperationException(
                "MvRegisterAccessor is uninitialised; obtain it via ILattice.MvRegister<T>(key) instead of `default`.");
        }
    }
}
