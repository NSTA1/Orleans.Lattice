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
    // Tree resolution lives on the main partial (TreeAsync), which composes the
    // caller-supplied name under the active tenant before dialling the grain, so a
    // CRDT write lands in the caller's own namespace rather than a shared one.

    /// <inheritdoc />
    public async Task CounterIncrementAsync(string treeId, string key, string replicaId, long amount, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        cancellationToken.ThrowIfCancellationRequested();
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        await tree.PnCounter(key).IncrementAsync(replicaId, amount, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task CounterDecrementAsync(string treeId, string key, string replicaId, long amount, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        cancellationToken.ThrowIfCancellationRequested();
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        await tree.PnCounter(key).DecrementAsync(replicaId, amount, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<long> CounterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        return await tree.PnCounter(key).ValueAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task SetAddAsync(string treeId, string key, byte[] element, string replicaId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(element);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        cancellationToken.ThrowIfCancellationRequested();
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        await tree.OrSet(key).AddAsync(element, replicaId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task SetRemoveAsync(string treeId, string key, byte[] element, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(element);
        cancellationToken.ThrowIfCancellationRequested();
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        await tree.OrSet(key).RemoveAsync(element, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<byte[]>> SetGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();
        var set = await (await TreeAsync(treeId, cancellationToken).ConfigureAwait(false)).OrSet(key).GetAsync(cancellationToken).ConfigureAwait(false);
        return [.. set.Elements()];
    }

    /// <inheritdoc />
    public async Task OrFlagEnableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        cancellationToken.ThrowIfCancellationRequested();
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        await tree.OrFlag(key).EnableAsync(replicaId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task OrFlagDisableAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        await tree.OrFlag(key).DisableAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<bool> OrFlagGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        return await tree.OrFlag(key).IsEnabledAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task RwFlagEnableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        cancellationToken.ThrowIfCancellationRequested();
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        await tree.RwFlag(key).EnableAsync(replicaId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task RwFlagDisableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        cancellationToken.ThrowIfCancellationRequested();
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        await tree.RwFlag(key).DisableAsync(replicaId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<bool> RwFlagGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        return await tree.RwFlag(key).IsEnabledAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task GCounterIncrementAsync(string treeId, string key, string replicaId, long amount, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        cancellationToken.ThrowIfCancellationRequested();
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        await tree.GCounter(key).IncrementAsync(replicaId, amount, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<long> GCounterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        return await tree.GCounter(key).ValueAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task GSetAddAsync(string treeId, string key, byte[] element, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(element);
        cancellationToken.ThrowIfCancellationRequested();
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        await tree.GSet(key).AddAsync(element, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<byte[]>> GSetGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();
        return await (await TreeAsync(treeId, cancellationToken).ConfigureAwait(false)).GSet(key).ToListAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task RwSetAddAsync(string treeId, string key, byte[] element, string replicaId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(element);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        cancellationToken.ThrowIfCancellationRequested();
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        await tree.RwSet(key).AddAsync(element, replicaId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task RwSetRemoveAsync(string treeId, string key, byte[] element, string replicaId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(element);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        cancellationToken.ThrowIfCancellationRequested();
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        await tree.RwSet(key).RemoveAsync(element, replicaId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<byte[]>> RwSetGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();
        var set = await (await TreeAsync(treeId, cancellationToken).ConfigureAwait(false)).RwSet(key).GetAsync(cancellationToken).ConfigureAwait(false);
        return [.. set.Elements()];
    }

    /// <inheritdoc />
    public async Task VersionVectorTickAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        cancellationToken.ThrowIfCancellationRequested();
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        await tree.VersionVector(key).TickAsync(replicaId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyDictionary<string, string>> VersionVectorGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();
        var vector = await (await TreeAsync(treeId, cancellationToken).ConfigureAwait(false)).VersionVector(key).GetAsync(cancellationToken).ConfigureAwait(false);
        var result = new Dictionary<string, string>(vector.Entries.Count);
        foreach (var (replicaId, clock) in vector.Entries)
        {
            result[replicaId] = $"{clock.WallClockTicks}:{clock.Counter}";
        }

        return result;
    }

    /// <inheritdoc />
    public async Task RegisterSetAsync(string treeId, string key, string replicaId, byte[] value, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        ArgumentNullException.ThrowIfNull(value);
        cancellationToken.ThrowIfCancellationRequested();
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        await tree.MvRegister<byte[]>(key).SetAsync(replicaId, value, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<byte[]>> RegisterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        return await tree.MvRegister<byte[]>(key).ValuesAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task MaxRegisterSetAsync(string treeId, string key, byte[] value, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(value);
        cancellationToken.ThrowIfCancellationRequested();
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        await tree.MaxRegister<byte[]>(key, static v => v).SetAsync(value, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<byte[]?> MaxRegisterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        return await tree.MaxRegister<byte[]>(key, static v => v).GetAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task MinRegisterSetAsync(string treeId, string key, byte[] value, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(value);
        cancellationToken.ThrowIfCancellationRequested();
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        await tree.MinRegister<byte[]>(key, static v => v).SetAsync(value, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<byte[]?> MinRegisterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        return await tree.MinRegister<byte[]>(key, static v => v).GetAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task SequenceInsertAtAsync(string treeId, string key, int index, string replicaId, byte[] value, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        ArgumentNullException.ThrowIfNull(value);
        cancellationToken.ThrowIfCancellationRequested();
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        await tree.Sequence<byte[]>(key).InsertAtAsync(index, replicaId, value, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task SequenceRemoveAtAsync(string treeId, string key, int index, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        await tree.Sequence<byte[]>(key).RemoveAtAsync(index, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<byte[]>> SequenceGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        return await tree.Sequence<byte[]>(key).ToListAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task MapSetAsync(string treeId, string key, string field, string replicaId, byte[] value, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(field);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        ArgumentNullException.ThrowIfNull(value);
        cancellationToken.ThrowIfCancellationRequested();

        var register = new MvRegister();
        register.Set(replicaId, value);
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        await tree.OrMap<string, MvRegister>(key).SetAsync(field, replicaId, register, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task MapRemoveAsync(string treeId, string key, string field, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(field);
        cancellationToken.ThrowIfCancellationRequested();
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        await tree.OrMap<string, MvRegister>(key).RemoveAsync(field, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyDictionary<string, IReadOnlyList<byte[]>>> MapGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();

        var map = await (await TreeAsync(treeId, cancellationToken).ConfigureAwait(false)).OrMap<string, MvRegister>(key).GetAsync(cancellationToken).ConfigureAwait(false);
        var result = new Dictionary<string, IReadOnlyList<byte[]>>();
        foreach (var field in map.Keys())
        {
            var register = map.Get(field);
            result[field] = register is null ? Array.Empty<byte[]>() : register.Values();
        }

        return result;
    }
}
