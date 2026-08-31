using System.Linq.Expressions;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Fluent builder for a cross-tree atomic write, opened via
/// <see cref="LatticeCrossTreeAtomicWriteExtensions.BeginAtomicWrite"/>. Accumulates
/// per-tree slices, then commits them all-or-nothing through the cross-tree
/// coordinator. Not thread-safe: build on a single logical flow and
/// <see cref="CommitAsync"/> once.
/// <para>
/// Each tree may carry an optional guard predicate (set via
/// <see cref="SetWhere{T}(string, T, Expression{Func{T, bool}})"/>): the whole
/// cross-tree batch commits only if every guarded tree's keys satisfy their
/// predicate against their pre-saga value, mirroring the single-tree guarded
/// saga. A tree may have at most one predicate.
/// </para>
/// <para>
/// A slice may also couple a typed CRDT mutation into the atomic write via
/// <see cref="Set(LatticeStagedCrdtWrite)"/>, passing a token a CRDT accessor's
/// <c>Stage*</c> method produced (for example
/// <see cref="OrFlagAccessor.StageEnableAsync(string, CancellationToken)"/> or
/// <see cref="PnCounterAccessor.StageIncrementAsync(string, long, CancellationToken)"/>).
/// The staged value rides alongside sibling last-writer-wins <c>Set</c>/<c>SetWhere</c>
/// writes and other staged CRDT writes, committing all-or-nothing; the tree it is
/// added under must be configured with the matching CRDT merge mode. See
/// <see cref="LatticeStagedCrdtWrite"/> for the full caller contract.
/// </para>
/// </summary>
public sealed class LatticeAtomicWriteBuilder
{
    private readonly IGrainFactory _factory;
    private readonly string _operationId;
    private readonly List<TreeSlice> _slices = [];
    private TreeSlice? _current;

    internal LatticeAtomicWriteBuilder(IGrainFactory factory, string operationId)
    {
        _factory = factory;
        _operationId = operationId;
    }

    /// <summary>
    /// Selects (creating if necessary) the tree that subsequent
    /// <c>Set</c>/<c>SetWhere</c> calls target. Re-selecting an already-added
    /// tree continues appending to its slice.
    /// </summary>
    /// <param name="treeId">The logical tree to write into. Must be non-empty.</param>
    public LatticeAtomicWriteBuilder ForTree(string treeId)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        var existing = _slices.Find(s => string.Equals(s.TreeId, treeId, StringComparison.Ordinal));
        if (existing is null)
        {
            existing = new TreeSlice(treeId);
            _slices.Add(existing);
        }
        _current = existing;
        return this;
    }

    /// <summary>
    /// Stages a raw <paramref name="value"/> write for <paramref name="key"/> on
    /// the current tree.
    /// </summary>
    public LatticeAtomicWriteBuilder Set(string key, byte[] value)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        var slice = Current();
        // Defensively copy the caller-owned buffer on ingress: the builder holds
        // staged entries until CommitAsync, so aliasing the caller's array would
        // let a mutation made after staging but before commit corrupt the
        // committed payload.
        slice.Entries.Add(new KeyValuePair<string, byte[]>(key, (byte[])value.Clone()));
        slice.EntryDeltas.Add(null);
        slice.EntryDeletes.Add(false);
        return this;
    }

    /// <summary>
    /// Stages a retraction (tombstone) delete for <paramref name="key"/> on the
    /// current tree. The delete rides the all-or-nothing cross-tree batch
    /// alongside any sibling upserts, so a re-key projection (a row moving from
    /// one view key to another) can flip the upsert at the new key and the
    /// delete at the old key as a single atomic visibility change. The key is
    /// removed when the batch commits and left untouched when it aborts.
    /// </summary>
    /// <param name="key">The key to delete atomically. Must be non-null.</param>
    public LatticeAtomicWriteBuilder Delete(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        var slice = Current();
        slice.Entries.Add(new KeyValuePair<string, byte[]>(key, Array.Empty<byte>()));
        slice.EntryDeltas.Add(null);
        slice.EntryDeletes.Add(true);
        return this;
    }
    /// <summary>
    /// <paramref name="delta"/> that rides the atomic write alongside the value.
    /// The receiver applies the delta to the addressed key by its merge mode
    /// (the flag-CRDT membership-row path mints an enable-dot delta this way),
    /// while the locally stored value carries the merged CRDT state so local
    /// reads decode without replaying the delta. Internal: the public surface
    /// stages value-only entries; only the built-in tag index attaches deltas.
    /// </summary>
    internal LatticeAtomicWriteBuilder SetWithDelta(string key, byte[] value, byte[] delta)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(delta);
        var slice = Current();
        // Defensively copy both caller-owned buffers on ingress (see Set): the
        // staged value and delta outlive this call, so aliasing them would let a
        // post-stage mutation reach the committed batch.
        slice.Entries.Add(new KeyValuePair<string, byte[]>(key, (byte[])value.Clone()));
        slice.EntryDeltas.Add((byte[])delta.Clone());
        slice.EntryDeletes.Add(false);
        return this;
    }

    /// <summary>
    /// Couples a typed CRDT mutation prepared by a CRDT accessor's <c>Stage*</c>
    /// method into the atomic write on the current tree. The staged
    /// <see cref="LatticeStagedCrdtWrite.Value"/> (merged CRDT state) is written
    /// last-writer-wins for local reads and the staged
    /// <see cref="LatticeStagedCrdtWrite.Delta"/> rides alongside it so remote
    /// clusters fold the typed delta and converge. The current tree must be
    /// configured with the CRDT merge mode that matches the accessor the token
    /// came from. See <see cref="LatticeStagedCrdtWrite"/> for the full caller
    /// contract, including the single-cluster concurrent-writer caveat and the
    /// drop-the-delta compensation behaviour on abort.
    /// </summary>
    /// <param name="staged">The staging token returned by a CRDT accessor's <c>Stage*</c> method.</param>
    public LatticeAtomicWriteBuilder Set(LatticeStagedCrdtWrite staged)
    {
        ArgumentNullException.ThrowIfNull(staged.Key);
        ArgumentNullException.ThrowIfNull(staged.Value);
        ArgumentNullException.ThrowIfNull(staged.Delta);
        var slice = Current();
        // Defensively copy both staged buffers on ingress (see Set): the token's
        // arrays are caller-reachable, so aliasing them would let a post-stage
        // mutation reach the committed batch.
        slice.Entries.Add(new KeyValuePair<string, byte[]>(staged.Key, (byte[])staged.Value.Clone()));
        slice.EntryDeltas.Add((byte[])staged.Delta.Clone());
        slice.EntryDeletes.Add(false);
        return this;
    }

    /// <summary>
    /// Serializes <paramref name="value"/> with <paramref name="serializer"/>
    /// and stages it for <paramref name="key"/> on the current tree.
    /// </summary>
    public LatticeAtomicWriteBuilder Set<T>(string key, T value, ILatticeSerializer<T> serializer)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(serializer);
        var slice = Current();
        slice.Entries.Add(new KeyValuePair<string, byte[]>(key, serializer.Serialize(value)));
        slice.EntryDeltas.Add(null);
        slice.EntryDeletes.Add(false);
        return this;
    }

    /// <inheritdoc cref="Set{T}(string, T, ILatticeSerializer{T})"/>
    public LatticeAtomicWriteBuilder Set<T>(string key, T value) =>
        Set(key, value, JsonLatticeSerializer<T>.Default);

    /// <summary>
    /// Serializes <paramref name="value"/> and stages it for
    /// <paramref name="key"/> on the current tree under the guard
    /// <paramref name="predicate"/>. The predicate applies to every key in the
    /// current tree's slice and is evaluated once, server-side, against each
    /// key's pre-saga value; a tree may carry at most one predicate.
    /// </summary>
    /// <exception cref="InvalidOperationException">The current tree already has a different predicate.</exception>
    public LatticeAtomicWriteBuilder SetWhere<T>(
        string key,
        T value,
        Expression<Func<T, bool>> predicate,
        ILatticeSerializer<T> serializer)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentNullException.ThrowIfNull(serializer);
        var ir = LatticePredicatePushdown.Compile(predicate, serializer);
        var slice = Current();
        if (slice.Predicate is { } existing && !existing.Equals(ir))
        {
            throw new InvalidOperationException(
                $"Tree '{slice.TreeId}' already has a guard predicate; a cross-tree slice supports at most one.");
        }
        slice.Predicate = ir;
        slice.Entries.Add(new KeyValuePair<string, byte[]>(key, serializer.Serialize(value)));
        slice.EntryDeltas.Add(null);
        slice.EntryDeletes.Add(false);
        return this;
    }

    /// <inheritdoc cref="SetWhere{T}(string, T, Expression{Func{T, bool}}, ILatticeSerializer{T})"/>
    public LatticeAtomicWriteBuilder SetWhere<T>(string key, T value, Expression<Func<T, bool>> predicate) =>
        SetWhere(key, value, predicate, JsonLatticeSerializer<T>.Default);

    /// <summary>
    /// Commits every staged tree slice atomically. See
    /// <see cref="LatticeCrossTreeAtomicWriteExtensions.SetManyAtomicAsync"/>
    /// for the outcome and idempotency contract.
    /// </summary>
    public Task<CrossTreeAtomicWriteOutcome> CommitAsync(CancellationToken cancellationToken = default)
    {
        var batches = new List<LatticeTreeBatch>(_slices.Count);
        foreach (var slice in _slices)
        {
            // Forward the per-entry delta carry only when at least one entry on
            // the slice attached a typed CRDT delta (the flag-CRDT membership
            // path or a public Set(LatticeStagedCrdtWrite) staged mutation). A
            // value-only slice forwards a null carry so the cross-tree write
            // stays byte-identical to the pre-existing path.
            var entryDeltas = slice.EntryDeltas.Exists(static d => d is not null)
                ? slice.EntryDeltas
                : null;
            // Forward the per-entry delete (tombstone) channel only when at
            // least one entry on the slice is a Delete; an upsert-only slice
            // forwards a null carry so the cross-tree write stays byte-identical
            // to the pre-existing path.
            var entryDeletes = slice.EntryDeletes.Exists(static d => d)
                ? slice.EntryDeletes
                : null;
            batches.Add(new LatticeTreeBatch(slice.TreeId, slice.Entries, slice.Predicate, entryDeltas, entryDeletes));
        }
        return _factory.SetManyAtomicAsync(batches, _operationId, cancellationToken);
    }

    private TreeSlice Current() =>
        _current ?? throw new InvalidOperationException(
            "No tree selected; call ForTree(treeId) before staging writes.");

    private sealed class TreeSlice(string treeId)
    {
        public string TreeId { get; } = treeId;
        public List<KeyValuePair<string, byte[]>> Entries { get; } = [];
        public List<byte[]?> EntryDeltas { get; } = [];
        public List<bool> EntryDeletes { get; } = [];
        public LatticePredicateNode? Predicate { get; set; }
    }
}
