using System.Text;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// An in-memory <see cref="ILatticeDataApi"/> behind the Data area's write
/// controls: a write or delete lands in the <see cref="FakeStateClient"/> the
/// readers read, the capability probe answers from per-tree and per-key rights a
/// test sets, and a test can refuse a key, refuse a value as the schema policy
/// would, or slip a write in between the editor opening and saving.
/// </summary>
internal sealed class FakeDataWriter(FakeStateClient state) : ILatticeDataApi
{
    private long _ticks = 639_100_000_000_000_000;

    /// <summary>The rights the probe reports for a whole tree, by state id; absent reads as none.</summary>
    public Dictionary<string, (bool Write, bool Delete)> TreeRights { get; } = new(StringComparer.Ordinal);

    /// <summary>Keys the facade refuses whatever the probe said, as a forged request is refused.</summary>
    public HashSet<string> RefusedKeys { get; } = new(StringComparer.Ordinal);

    /// <summary>A schema check the facade applies: a reason refuses the value.</summary>
    public Func<byte[], string?>? Policy { get; set; }

    /// <summary>Runs just before a write lands, so a test can race it.</summary>
    public Action<string>? BeforeWrite { get; set; }

    /// <summary>The probe requests, as <c>tree</c> or <c>tree:key</c>.</summary>
    public List<string> Probes { get; } = [];

    /// <summary>The writes and deletes that landed, as <c>set:key</c> or <c>delete:key</c>.</summary>
    public List<string> Writes { get; } = [];

    /// <summary>Writes <paramref name="value"/> at <paramref name="key"/> as another writer would.</summary>
    public void WriteBehind(string treeId, string key, string value) => Put(treeId, key, Encoding.UTF8.GetBytes(value));

    /// <inheritdoc />
    public Task<LatticeDataCapabilities> ProbeCapabilitiesAsync(string treeId, string? key = null, CancellationToken cancellationToken = default)
    {
        Probes.Add(key is null ? treeId : treeId + ":" + key);
        var rights = TreeRights.TryGetValue(treeId, out var granted) ? granted : default;
        var refused = key is not null && RefusedKeys.Contains(key);
        return Task.FromResult(new LatticeDataCapabilities
        {
            TreeId = treeId,
            Key = key,
            CanWrite = rights.Write && !refused,
            CanDelete = rights.Delete && !refused,
        });
    }

    /// <inheritdoc />
    public Task SetAsync(string treeId, string key, byte[] value, CancellationToken cancellationToken = default)
    {
        Refuse(treeId, key);
        if (Policy?.Invoke(value) is { } reason)
        {
            throw new ShellValueValidationException("The value was rejected by the tree's schema policy.", treeId, key, reason, new InvalidOperationException());
        }

        BeforeWrite?.Invoke(key);
        Writes.Add("set:" + key);
        Put(treeId, key, value);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<bool> DeleteAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        Refuse(treeId, key);
        Writes.Add("delete:" + key);
        var removed = state.Entries.TryGetValue(treeId, out var entries) && entries.Remove(key);
        return Task.FromResult(removed);
    }

    /// <inheritdoc />
    public Task<DataReadResult> GetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        var found = state.Entries.TryGetValue(treeId, out var entries) && entries.TryGetValue(key, out var entry) ? entry : null;
        return Task.FromResult(new DataReadResult { TreeId = treeId, Key = key, Found = found is not null, Value = found?.ValuePreview ?? [] });
    }

    private void Refuse(string treeId, string key)
    {
        var rights = TreeRights.TryGetValue(treeId, out var granted) ? granted : default;
        if (!rights.Write || RefusedKeys.Contains(key))
        {
            throw new LatticeAuthorizationDeniedException(treeId, LatticeOperation.Write, "caller", "No write grant.");
        }
    }

    private void Put(string treeId, string key, byte[] value)
    {
        if (!state.Entries.TryGetValue(treeId, out var entries))
        {
            entries = new SortedDictionary<string, EntryRecord>(StringComparer.Ordinal);
            state.Entries[treeId] = entries;
        }

        _ticks += 10_000_000;
        entries[key] = new EntryRecord { Key = key, ValuePreview = value, ValueLength = value.Length, Hlc = new HybridLogicalClock { WallClockTicks = _ticks } };
    }

    /// <inheritdoc />
    public Task<DataRangeDeleteResult> DeleteRangeAsync(DataRangeDeleteRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task SetManyAtomicAsync(string treeId, DataAtomicBatch batch, string operationId, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task<CrossTreeAtomicWriteOutcome> SetManyAtomicCrossTreeAsync(IReadOnlyList<DataTreeBatch> batches, string operationId, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task SetManyAsync(string treeId, IReadOnlyList<Api.Data.DataEntry> upserts, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task<DataRangePage> ReadRangeAsync(DataRangeRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task CounterIncrementAsync(string treeId, string key, string replicaId, long amount, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task CounterDecrementAsync(string treeId, string key, string replicaId, long amount, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task<long> CounterGetAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task SetAddAsync(string treeId, string key, byte[] element, string replicaId, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task SetRemoveAsync(string treeId, string key, byte[] element, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task<IReadOnlyList<byte[]>> SetGetAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task OrFlagEnableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task OrFlagDisableAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task<bool> OrFlagGetAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task RwFlagEnableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task RwFlagDisableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task<bool> RwFlagGetAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task GCounterIncrementAsync(string treeId, string key, string replicaId, long amount, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task<long> GCounterGetAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task GSetAddAsync(string treeId, string key, byte[] element, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task<IReadOnlyList<byte[]>> GSetGetAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task RwSetAddAsync(string treeId, string key, byte[] element, string replicaId, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task RwSetRemoveAsync(string treeId, string key, byte[] element, string replicaId, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task<IReadOnlyList<byte[]>> RwSetGetAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task VersionVectorTickAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task<IReadOnlyDictionary<string, string>> VersionVectorGetAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task RegisterSetAsync(string treeId, string key, string replicaId, byte[] value, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task<IReadOnlyList<byte[]>> RegisterGetAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task MaxRegisterSetAsync(string treeId, string key, byte[] value, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task<byte[]?> MaxRegisterGetAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task MinRegisterSetAsync(string treeId, string key, byte[] value, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task<byte[]?> MinRegisterGetAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task SequenceInsertAtAsync(string treeId, string key, int index, string replicaId, byte[] value, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task SequenceRemoveAtAsync(string treeId, string key, int index, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task<IReadOnlyList<byte[]>> SequenceGetAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task MapSetAsync(string treeId, string key, string field, string replicaId, byte[] value, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task MapRemoveAsync(string treeId, string key, string field, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task<IReadOnlyDictionary<string, IReadOnlyList<byte[]>>> MapGetAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw new NotSupportedException();
}
