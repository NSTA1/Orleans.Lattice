namespace Orleans.Lattice.Api.Data;

/// <summary>
/// Typed CRDT operations for <see cref="LatticeDataApi"/> (issue #1361). Each
/// verb resolves the cluster <see cref="ILattice"/> grain and drives the matching
/// typed accessor from <see cref="CrdtLatticeExtensions"/>, which owns the
/// op-to-delta encoding and routes the write through the same authorized
/// <c>ApplyCrdtDeltaAsync</c> path the in-cluster client uses. The facade adds no
/// encoding or authorization of its own: a denied write throws
/// <see cref="LatticeAuthorizationDeniedException"/>, and a verb whose mode does
/// not match a replicated tree's enrolled mode (or an OR-Map verb on a tree with
/// no registered map shape) faults from the engine unchanged.
/// </summary>
/// <remarks>
/// The OR-Map surface is pinned to a string-keyed, multi-value-register shape
/// (<c>OrMap&lt;string, MvRegister&gt;</c>): a put stores the value as a
/// single-writer <see cref="MvRegister"/> under the field, and a read decodes
/// each live field's register back to its concurrent value bytes. This is the one
/// closed shape expressible over an opaque byte boundary without the caller
/// declaring a generic type.
/// </remarks>
internal sealed partial class LatticeDataApi
{
    private ILattice Tree(string treeId)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return _grainFactory.GetGrain<ILattice>(treeId);
    }

    /// <inheritdoc />
    public Task CounterIncrementAsync(string treeId, string key, string replicaId, long amount, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        cancellationToken.ThrowIfCancellationRequested();
        return Tree(treeId).PnCounter(key).IncrementAsync(replicaId, amount, cancellationToken);
    }

    /// <inheritdoc />
    public Task CounterDecrementAsync(string treeId, string key, string replicaId, long amount, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        cancellationToken.ThrowIfCancellationRequested();
        return Tree(treeId).PnCounter(key).DecrementAsync(replicaId, amount, cancellationToken);
    }

    /// <inheritdoc />
    public Task<long> CounterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();
        return Tree(treeId).PnCounter(key).ValueAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task SetAddAsync(string treeId, string key, byte[] element, string replicaId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(element);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        cancellationToken.ThrowIfCancellationRequested();
        return Tree(treeId).OrSet(key).AddAsync(element, replicaId, cancellationToken);
    }

    /// <inheritdoc />
    public Task SetRemoveAsync(string treeId, string key, byte[] element, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(element);
        cancellationToken.ThrowIfCancellationRequested();
        return Tree(treeId).OrSet(key).RemoveAsync(element, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<byte[]>> SetGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();
        var set = await Tree(treeId).OrSet(key).GetAsync(cancellationToken).ConfigureAwait(false);
        return [.. set.Elements()];
    }

    /// <inheritdoc />
    public Task OrFlagEnableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        cancellationToken.ThrowIfCancellationRequested();
        return Tree(treeId).OrFlag(key).EnableAsync(replicaId, cancellationToken);
    }

    /// <inheritdoc />
    public Task OrFlagDisableAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();
        return Tree(treeId).OrFlag(key).DisableAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task<bool> OrFlagGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();
        return Tree(treeId).OrFlag(key).IsEnabledAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task RwFlagEnableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        cancellationToken.ThrowIfCancellationRequested();
        return Tree(treeId).RwFlag(key).EnableAsync(replicaId, cancellationToken);
    }

    /// <inheritdoc />
    public Task RwFlagDisableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        cancellationToken.ThrowIfCancellationRequested();
        return Tree(treeId).RwFlag(key).DisableAsync(replicaId, cancellationToken);
    }

    /// <inheritdoc />
    public Task<bool> RwFlagGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();
        return Tree(treeId).RwFlag(key).IsEnabledAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task GCounterIncrementAsync(string treeId, string key, string replicaId, long amount, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        cancellationToken.ThrowIfCancellationRequested();
        return Tree(treeId).GCounter(key).IncrementAsync(replicaId, amount, cancellationToken);
    }

    /// <inheritdoc />
    public Task<long> GCounterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();
        return Tree(treeId).GCounter(key).ValueAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task GSetAddAsync(string treeId, string key, byte[] element, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(element);
        cancellationToken.ThrowIfCancellationRequested();
        return Tree(treeId).GSet(key).AddAsync(element, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<byte[]>> GSetGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();
        return await Tree(treeId).GSet(key).ToListAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task RwSetAddAsync(string treeId, string key, byte[] element, string replicaId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(element);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        cancellationToken.ThrowIfCancellationRequested();
        return Tree(treeId).RwSet(key).AddAsync(element, replicaId, cancellationToken);
    }

    /// <inheritdoc />
    public Task RwSetRemoveAsync(string treeId, string key, byte[] element, string replicaId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(element);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        cancellationToken.ThrowIfCancellationRequested();
        return Tree(treeId).RwSet(key).RemoveAsync(element, replicaId, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<byte[]>> RwSetGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();
        var set = await Tree(treeId).RwSet(key).GetAsync(cancellationToken).ConfigureAwait(false);
        return [.. set.Elements()];
    }

    /// <inheritdoc />
    public Task VersionVectorTickAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        cancellationToken.ThrowIfCancellationRequested();
        return Tree(treeId).VersionVector(key).TickAsync(replicaId, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyDictionary<string, string>> VersionVectorGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();
        var vector = await Tree(treeId).VersionVector(key).GetAsync(cancellationToken).ConfigureAwait(false);
        var result = new Dictionary<string, string>(vector.Entries.Count);
        foreach (var (replicaId, clock) in vector.Entries)
        {
            result[replicaId] = $"{clock.WallClockTicks}:{clock.Counter}";
        }

        return result;
    }

    /// <inheritdoc />
    public Task RegisterSetAsync(string treeId, string key, string replicaId, byte[] value, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        ArgumentNullException.ThrowIfNull(value);
        cancellationToken.ThrowIfCancellationRequested();
        return Tree(treeId).MvRegister<byte[]>(key).SetAsync(replicaId, value, cancellationToken);
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<byte[]>> RegisterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();
        return Tree(treeId).MvRegister<byte[]>(key).ValuesAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task MaxRegisterSetAsync(string treeId, string key, byte[] value, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(value);
        cancellationToken.ThrowIfCancellationRequested();
        return Tree(treeId).MaxRegister<byte[]>(key, static v => v).SetAsync(value, cancellationToken);
    }

    /// <inheritdoc />
    public Task<byte[]?> MaxRegisterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();
        return Tree(treeId).MaxRegister<byte[]>(key, static v => v).GetAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task MinRegisterSetAsync(string treeId, string key, byte[] value, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(value);
        cancellationToken.ThrowIfCancellationRequested();
        return Tree(treeId).MinRegister<byte[]>(key, static v => v).SetAsync(value, cancellationToken);
    }

    /// <inheritdoc />
    public Task<byte[]?> MinRegisterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();
        return Tree(treeId).MinRegister<byte[]>(key, static v => v).GetAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task SequenceInsertAtAsync(string treeId, string key, int index, string replicaId, byte[] value, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        ArgumentNullException.ThrowIfNull(value);
        cancellationToken.ThrowIfCancellationRequested();
        return Tree(treeId).Sequence<byte[]>(key).InsertAtAsync(index, replicaId, value, cancellationToken);
    }

    /// <inheritdoc />
    public Task SequenceRemoveAtAsync(string treeId, string key, int index, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();
        return Tree(treeId).Sequence<byte[]>(key).RemoveAtAsync(index, cancellationToken);
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<byte[]>> SequenceGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();
        return Tree(treeId).Sequence<byte[]>(key).ToListAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task MapSetAsync(string treeId, string key, string field, string replicaId, byte[] value, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(field);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        ArgumentNullException.ThrowIfNull(value);
        cancellationToken.ThrowIfCancellationRequested();

        var register = new MvRegister();
        register.Set(replicaId, value);
        return Tree(treeId).OrMap<string, MvRegister>(key).SetAsync(field, replicaId, register, cancellationToken);
    }

    /// <inheritdoc />
    public Task MapRemoveAsync(string treeId, string key, string field, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(field);
        cancellationToken.ThrowIfCancellationRequested();
        return Tree(treeId).OrMap<string, MvRegister>(key).RemoveAsync(field, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyDictionary<string, IReadOnlyList<byte[]>>> MapGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();

        var map = await Tree(treeId).OrMap<string, MvRegister>(key).GetAsync(cancellationToken).ConfigureAwait(false);
        var result = new Dictionary<string, IReadOnlyList<byte[]>>();
        foreach (var field in map.Keys())
        {
            var register = map.Get(field);
            result[field] = register is null ? Array.Empty<byte[]>() : register.Values();
        }

        return result;
    }
}
