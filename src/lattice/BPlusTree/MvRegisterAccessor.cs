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
        return MutateAsync(register =>
        {
            var encoded = serializer.Serialize(value);
            register.Set(replicaId, encoded);
            return new CrdtDeltaPayloads.MvRegisterSetDelta(
                replicaId,
                register.Context[replicaId],
                encoded,
                BuildContextPayload(register.Context));
        }, CrdtDeltaKinds.MvRegisterSet, cancellationToken, maxAttempts);
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
            return new CrdtDeltaPayloads.MvRegisterMergeDelta(
                other.Entries
                    .Select(static e => new CrdtDeltaPayloads.MvRegisterEntryPayload(e.ReplicaId, e.Counter, e.Value))
                    .ToArray(),
                BuildContextPayload(other.Context));
        }, CrdtDeltaKinds.MvRegisterMerge, cancellationToken, maxAttempts);
    }

    private async Task MutateAsync<TDelta>(
        Func<MvRegister, TDelta> mutate,
        string deltaKind,
        CancellationToken cancellationToken,
        int maxAttempts)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);
        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var versioned = await _lattice.GetWithVersionAsync(_key, cancellationToken).ConfigureAwait(false);
            var current = Decode(versioned.Value);
            var delta = mutate(current);
            var bytes = JsonLatticeSerializer<MvRegister>.Default.Serialize(current);
            var deltaBytes = JsonLatticeSerializer<TDelta>.Default.Serialize(delta);
            using (LatticeDeltaContext.With(deltaKind, deltaBytes))
            {
                var ok = await _lattice.SetIfVersionAsync(_key, bytes, versioned.Version, cancellationToken).ConfigureAwait(false);
                if (ok) return;
            }
        }
        throw new InvalidOperationException(
            $"MvRegister CAS budget exhausted after {maxAttempts} attempts for key '{_key}'. " +
            "Increase maxAttempts or reduce contention.");
    }

    private static Dictionary<string, long> BuildContextPayload(Dictionary<string, long> context) =>
        new(context, StringComparer.Ordinal);

    private static MvRegister Decode(byte[]? bytes) =>
        bytes is null ? new MvRegister() : JsonLatticeSerializer<MvRegister>.Default.Deserialize(bytes);

    private void EnsureInitialised()
    {
        if (_lattice is null || _serializer is null)
        {
            throw new InvalidOperationException(
                "MvRegisterAccessor is uninitialised; obtain it via ILattice.MvRegister<T>(key) instead of `default`.");
        }
    }
}
