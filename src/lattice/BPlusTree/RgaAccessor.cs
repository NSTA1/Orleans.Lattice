using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Typed value-surface accessor for an <see cref="Rga"/> sequence
/// stored under a single key in an <see cref="ILattice"/>. The
/// accessor is a lightweight, allocation-free wrapper - construct it
/// once via
/// <see cref="CrdtLatticeExtensions.Sequence{T}(ILattice, string, ILatticeSerializer{T}?)"/>
/// and reuse it for any number of operations on the same key.
/// <para>
/// Mutating methods read-modify-write under optimistic concurrency,
/// retrying on CAS failure up to a configurable budget. The
/// high-level <see cref="InsertAtAsync(int, string, T, CancellationToken, int)"/>
/// and <see cref="RemoveAtAsync(int, CancellationToken, int)"/>
/// methods resolve the index on the freshly-read state inside the
/// CAS loop so the materialised position is always interpreted
/// against the same snapshot the write commits against. Tooling that
/// needs stable cursor identity across reads can use the lower-level
/// dot-explicit
/// <see cref="InsertAfterAsync(OrSetDot, string, T, CancellationToken, int)"/>.
/// </para>
/// </summary>
/// <typeparam name="T">The user-facing value type. Serialised to and from <see cref="byte"/>[] through <see cref="ILatticeSerializer{T}"/>.</typeparam>
public readonly record struct RgaAccessor<T>
{
    /// <summary>Default CAS retry budget for mutating operations.</summary>
    public const int DefaultMaxAttempts = 16;

    private readonly ILattice _lattice;
    private readonly string _key;
    private readonly ILatticeSerializer<T> _serializer;

    internal RgaAccessor(ILattice lattice, string key, ILatticeSerializer<T> serializer)
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
    /// Reads the current sequence state. Returns an empty
    /// <see cref="Rga"/> when the key is absent.
    /// </summary>
    public async Task<Rga> GetAsync(CancellationToken cancellationToken = default)
    {
        EnsureInitialised();
        var bytes = await _lattice.GetAsync(_key, cancellationToken).ConfigureAwait(false);
        return Decode(bytes);
    }

    /// <summary>
    /// Returns the live values of the sequence in resolved in-order
    /// projection. The list is materialised by the underlying
    /// <see cref="Rga.ToList"/> and deserialised through the
    /// configured <see cref="ILatticeSerializer{T}"/>.
    /// </summary>
    public async Task<IReadOnlyList<T>> ToListAsync(CancellationToken cancellationToken = default)
    {
        EnsureInitialised();
        var rga = await GetAsync(cancellationToken).ConfigureAwait(false);
        var snapshot = rga.ToList();
        if (snapshot.Count == 0) return Array.Empty<T>();
        var values = new T[snapshot.Count];
        for (var i = 0; i < snapshot.Count; i++)
        {
            values[i] = _serializer.Deserialize(snapshot[i].Value);
        }
        return values;
    }

    /// <summary>
    /// Inserts <paramref name="value"/> at the visible
    /// <paramref name="index"/> in the materialised in-order
    /// projection. Index <c>0</c> inserts at the head of the
    /// sequence; an index equal to the current count appends to the
    /// tail. Mints a fresh causal dot under the resolved parent and
    /// retries the read-modify-write under CAS up to
    /// <paramref name="maxAttempts"/>.
    /// </summary>
    /// <param name="index">The visible position to insert at. Must be in <c>[0, count]</c>.</param>
    /// <param name="replicaId">The replica authoring the insert. Must be non-empty.</param>
    /// <param name="value">The value to attach.</param>
    /// <param name="cancellationToken">Cancels the read and write hops.</param>
    /// <param name="maxAttempts">Maximum number of CAS retries before giving up.</param>
    public Task<OrSetDot> InsertAtAsync(int index, string replicaId, T value, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(index);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        EnsureInitialised();
        var serializer = _serializer;
        var encoded = serializer.Serialize(value);
        return MutateAsync(rga =>
        {
            var snapshot = rga.ToList();
            if (index > snapshot.Count)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(index),
                    $"Insert index {index} is beyond the resolved sequence length {snapshot.Count}.");
            }
            // Index N inserts after the live element at position N-1
            // (or at the root when N == 0). Inserting at the count
            // attaches under the last visible element.
            var parent = index == 0 ? Rga.Root : snapshot[index - 1].Dot;
            return rga.InsertAfter(parent, replicaId, encoded);
        }, cancellationToken, maxAttempts);
    }

    /// <summary>
    /// Inserts <paramref name="value"/> as a child of
    /// <paramref name="parentDot"/> (or the virtual root when
    /// <paramref name="parentDot"/> equals <see cref="Rga.Root"/>).
    /// Useful for tooling that has captured a stable cursor identity
    /// from a previous <see cref="ToListAsync(CancellationToken)"/> call
    /// and wants the insert to land at that exact causal position
    /// regardless of intervening edits.
    /// </summary>
    public Task<OrSetDot> InsertAfterAsync(OrSetDot parentDot, string replicaId, T value, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        EnsureInitialised();
        var serializer = _serializer;
        var encoded = serializer.Serialize(value);
        return MutateAsync(rga =>
        {
            return rga.InsertAfter(parentDot, replicaId, encoded);
        }, cancellationToken, maxAttempts);
    }

    /// <summary>
    /// Tombstones the live node at the visible
    /// <paramref name="index"/> in the materialised in-order
    /// projection. Index <c>0</c> removes the head; an index of
    /// <c>count - 1</c> removes the tail.
    /// </summary>
    public Task RemoveAtAsync(int index, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(index);
        EnsureInitialised();
        return MutateVoidAsync(rga =>
        {
            var snapshot = rga.ToList();
            if (index >= snapshot.Count)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(index),
                    $"Remove index {index} is at or beyond the resolved sequence length {snapshot.Count}.");
            }
            rga.Remove(snapshot[index].Dot);
        }, cancellationToken, maxAttempts);
    }

    /// <summary>
    /// Tombstones the node identified by <paramref name="dot"/>. A
    /// no-op when the dot is not present or already tombstoned.
    /// </summary>
    public Task RemoveAsync(OrSetDot dot, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        EnsureInitialised();
        return MutateVoidAsync(rga =>
        {
            rga.Remove(dot);
        }, cancellationToken, maxAttempts);
    }

    /// <summary>
    /// Merges <paramref name="other"/> into the stored state under
    /// CAS. Useful for replication consumers that have computed a
    /// delta out-of-band and want to apply it without reading the
    /// full sequence twice.
    /// </summary>
    public Task MergeAsync(Rga other, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentNullException.ThrowIfNull(other);
        EnsureInitialised();
        return MutateVoidAsync(rga =>
        {
            rga.MergeFrom(other);
        }, cancellationToken, maxAttempts);
    }

    private async Task<OrSetDot> MutateAsync(Func<Rga, OrSetDot> mutate, CancellationToken cancellationToken, int maxAttempts)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);
        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var versioned = await _lattice.GetWithVersionAsync(_key, cancellationToken).ConfigureAwait(false);
            var current = Decode(versioned.Value);
            var dot = mutate(current);
            var bytes = JsonLatticeSerializer<Rga>.Default.Serialize(current);
            var ok = await _lattice.SetIfVersionAsync(_key, bytes, versioned.Version, cancellationToken).ConfigureAwait(false);
            if (ok) return dot;
        }
        throw new InvalidOperationException(
            $"Rga CAS budget exhausted after {maxAttempts} attempts for key '{_key}'. " +
            "Increase maxAttempts or reduce contention.");
    }

    private async Task MutateVoidAsync(Action<Rga> mutate, CancellationToken cancellationToken, int maxAttempts)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);
        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var versioned = await _lattice.GetWithVersionAsync(_key, cancellationToken).ConfigureAwait(false);
            var current = Decode(versioned.Value);
            mutate(current);
            var bytes = JsonLatticeSerializer<Rga>.Default.Serialize(current);
            var ok = await _lattice.SetIfVersionAsync(_key, bytes, versioned.Version, cancellationToken).ConfigureAwait(false);
            if (ok) return;
        }
        throw new InvalidOperationException(
            $"Rga CAS budget exhausted after {maxAttempts} attempts for key '{_key}'. " +
            "Increase maxAttempts or reduce contention.");
    }

    private static Rga Decode(byte[]? bytes) =>
        bytes is null ? new Rga() : JsonLatticeSerializer<Rga>.Default.Deserialize(bytes);

    private void EnsureInitialised()
    {
        if (_lattice is null)
        {
            throw new InvalidOperationException(
                "RgaAccessor is uninitialised; obtain it via ILattice.Sequence<T>(key) instead of `default`.");
        }
    }
}