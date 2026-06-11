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
        Current().Entries.Add(new KeyValuePair<string, byte[]>(key, value));
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
        Current().Entries.Add(new KeyValuePair<string, byte[]>(key, serializer.Serialize(value)));
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
            batches.Add(new LatticeTreeBatch(slice.TreeId, slice.Entries, slice.Predicate));
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
        public LatticePredicateNode? Predicate { get; set; }
    }
}
