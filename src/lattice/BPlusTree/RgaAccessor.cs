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
/// Each mutating method performs a single local read to resolve the
/// dot-explicit operation (the index-resolving overloads
/// <see cref="InsertAtAsync(int, string, T, CancellationToken, int)"/> and
/// <see cref="RemoveAtAsync(int, CancellationToken, int)"/> resolve the
/// visible position to a dot / parent-dot against that snapshot), then
/// authors a typed <see cref="RgaDelta"/> and commits it through the
/// <see cref="LatticeMergeMode.Sequence"/> typed-delta WAL seam via
/// <c>ILattice.ApplyCrdtDeltaAsync</c>. The delta captures the
/// structural intent (the dots and parent dots), not the post-merge
/// materialised order, so a remote replica converges on an identical
/// ordered traversal. Tooling that needs stable cursor identity across
/// reads can use the lower-level dot-explicit
/// <see cref="InsertAfterAsync(OrSetDot, string, T, CancellationToken, int)"/>.
/// </para>
/// <para>
/// The delta apply is CAS-free on the producer side, mirroring
/// <see cref="MvRegisterAccessor{T}"/> and
/// <see cref="OrMapAccessor{TKey, TValue}"/>; the <c>maxAttempts</c>
/// parameter is validated (a value below <c>1</c> throws) and otherwise
/// retained for source compatibility.
/// </para>
/// </summary>
/// <typeparam name="T">The user-facing value type. Serialised to and from <see cref="byte"/>[] through <see cref="ILatticeSerializer{T}"/>.</typeparam>
public readonly record struct RgaAccessor<T>
{
    /// <summary>Default mutation retry budget (validated; the producer-side delta apply is CAS-free).</summary>
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
    /// commits the dot-explicit insert through the
    /// <see cref="LatticeMergeMode.Sequence"/> typed-delta seam.
    /// </summary>
    /// <param name="index">The visible position to insert at. Must be in <c>[0, count]</c>.</param>
    /// <param name="replicaId">The replica authoring the insert. Must be non-empty.</param>
    /// <param name="value">The value to attach.</param>
    /// <param name="cancellationToken">Cancels the read and write hops.</param>
    /// <param name="maxAttempts">Validated retry budget (the producer-side delta apply is CAS-free).</param>
    public Task<OrSetDot> InsertAtAsync(int index, string replicaId, T value, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(index);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        EnsureInitialised();
        var serializer = _serializer;
        var encoded = serializer.Serialize(value);
        return ApplyInsertDeltaAsync(rga =>
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
            // attaches under the last visible element. The parent dot is
            // resolved here, at the call site, so the emitted delta
            // captures the dot-explicit structural intent.
            var parent = index == 0 ? Rga.Root : snapshot[index - 1].Dot;
            var dot = rga.InsertAfter(parent, replicaId, encoded);
            return (dot, SingleInsertDelta(dot, parent, encoded));
        }, cancellationToken, maxAttempts);
    }

    /// <summary>
    /// Inserts <paramref name="value"/> as a child of
    /// <paramref name="parentDot"/> (or the virtual root when
    /// <paramref name="parentDot"/> equals <see cref="Rga.Root"/>).
    /// Useful for tooling that has captured a stable cursor identity
    /// from a previous <see cref="ToListAsync(CancellationToken)"/> call
    /// and wants the insert to land at that exact causal position
    /// regardless of intervening edits. The dot and parent dot are
    /// captured into the emitted <see cref="RgaDelta"/> so a remote
    /// replica replays the same structural insert.
    /// </summary>
    public Task<OrSetDot> InsertAfterAsync(OrSetDot parentDot, string replicaId, T value, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        EnsureInitialised();
        var serializer = _serializer;
        var encoded = serializer.Serialize(value);
        return ApplyInsertDeltaAsync(rga =>
        {
            var dot = rga.InsertAfter(parentDot, replicaId, encoded);
            return (dot, SingleInsertDelta(dot, parentDot, encoded));
        }, cancellationToken, maxAttempts);
    }

    /// <summary>
    /// Tombstones the live node at the visible
    /// <paramref name="index"/> in the materialised in-order
    /// projection. Index <c>0</c> removes the head; an index of
    /// <c>count - 1</c> removes the tail. The index is resolved here, at
    /// the call site, to the dot-explicit tombstone the emitted
    /// <see cref="RgaDelta"/> ships.
    /// </summary>
    public Task RemoveAtAsync(int index, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(index);
        EnsureInitialised();
        return ApplyDeltaAsync(rga =>
        {
            var snapshot = rga.ToList();
            if (index >= snapshot.Count)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(index),
                    $"Remove index {index} is at or beyond the resolved sequence length {snapshot.Count}.");
            }
            var dot = snapshot[index].Dot;
            rga.Remove(dot);
            return new RgaDelta
            {
                Inserts = Array.Empty<RgaDeltaNode>(),
                Tombstones = new[] { dot },
            };
        }, cancellationToken, maxAttempts);
    }

    /// <summary>
    /// Tombstones the node identified by <paramref name="dot"/>. A
    /// no-op when the dot is not present or already tombstoned.
    /// </summary>
    public Task RemoveAsync(OrSetDot dot, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        EnsureInitialised();
        return ApplyDeltaAsync(_ =>
            new RgaDelta
            {
                Inserts = Array.Empty<RgaDeltaNode>(),
                Tombstones = new[] { dot },
            }, cancellationToken, maxAttempts);
    }

    /// <summary>
    /// Merges <paramref name="other"/> into the stored state. Useful for
    /// replication consumers that have computed a delta out-of-band and
    /// want to apply it without reading the full sequence twice. Every
    /// node in <paramref name="other"/> ships its structural info as an
    /// insert (so the receiver has the correct parent and value), and
    /// tombstoned nodes additionally ship their dot in the delta's
    /// tombstone list.
    /// </summary>
    public Task MergeAsync(Rga other, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentNullException.ThrowIfNull(other);
        EnsureInitialised();
        return ApplyDeltaAsync(rga =>
        {
            rga.MergeFrom(other);
            var inserts = new RgaDeltaNode[other.Nodes.Count];
            var tombstones = new List<OrSetDot>();
            for (var i = 0; i < other.Nodes.Count; i++)
            {
                var n = other.Nodes[i];
                inserts[i] = new RgaDeltaNode
                {
                    ReplicaId = n.ReplicaId,
                    Counter = n.Counter,
                    ParentDot = n.ParentDot,
                    Value = n.Value,
                };
                if (n.IsTombstone) tombstones.Add(n.Dot);
            }
            return new RgaDelta
            {
                Inserts = inserts,
                Tombstones = tombstones.Count == 0 ? Array.Empty<OrSetDot>() : tombstones,
            };
        }, cancellationToken, maxAttempts);
    }

    private static RgaDelta SingleInsertDelta(OrSetDot dot, OrSetDot parent, byte[] encoded) =>
        new()
        {
            Inserts = new[]
            {
                new RgaDeltaNode
                {
                    ReplicaId = dot.ReplicaId,
                    Counter = dot.Counter,
                    ParentDot = parent,
                    Value = encoded,
                },
            },
            Tombstones = Array.Empty<OrSetDot>(),
        };

    private async Task<OrSetDot> ApplyInsertDeltaAsync(Func<Rga, (OrSetDot Dot, RgaDelta Delta)> mutate, CancellationToken cancellationToken, int maxAttempts)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);
        // CAS-free producer-side delta apply, matching MvRegisterAccessor
        // and OrMapAccessor: a single local read computes the dot-explicit
        // delta (the read mints the next per-replica counter from the
        // local snapshot's view), then ApplyCrdtDeltaAsync folds the delta
        // into the persisted state through the typed-delta WAL seam.
        // Concurrent local writers minting the same dot is the caller's
        // responsibility, identical to the OR-Set per-replica monotonicity
        // contract.
        _ = maxAttempts;
        cancellationToken.ThrowIfCancellationRequested();
        var current = await GetAsync(cancellationToken).ConfigureAwait(false);
        var (dot, delta) = mutate(current);
        var deltaBytes = JsonLatticeSerializer<RgaDelta>.Default.Serialize(delta);
        await _lattice.ApplyCrdtDeltaAsync(_key, LatticeMergeMode.Sequence, deltaBytes, cancellationToken).ConfigureAwait(false);
        return dot;
    }

    private async Task ApplyDeltaAsync(Func<Rga, RgaDelta> mutate, CancellationToken cancellationToken, int maxAttempts)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);
        _ = maxAttempts;
        cancellationToken.ThrowIfCancellationRequested();
        var current = await GetAsync(cancellationToken).ConfigureAwait(false);
        var delta = mutate(current);
        var deltaBytes = JsonLatticeSerializer<RgaDelta>.Default.Serialize(delta);
        await _lattice.ApplyCrdtDeltaAsync(_key, LatticeMergeMode.Sequence, deltaBytes, cancellationToken).ConfigureAwait(false);
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