using Microsoft.Extensions.Options;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Coordinator grain implementing <see cref="ILatticeQueueGrain"/>. Storage,
/// caching, monotonic-id assignment, and FIFO eviction are delegated to a
/// <see cref="LatticeQueueCore"/> bound to the reserved
/// <c>_lattice_queue_{queueName}</c> system tree
/// (<see cref="LatticeConstants.QueueTreePrefix"/>). A head-cursor row is
/// persisted so steady-state dequeue and cold start skip already-dequeued
/// ids rather than re-walking from the head of the prefix.
/// </summary>
internal sealed class LatticeQueueGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeOptions> optionsMonitor) : ILatticeQueueGrain, IGrainBase
{
    /// <summary>Row-key prefix every queue entry carries inside the backing system tree.</summary>
    internal const string EntryKeyPrefix = "e/";

    private string _queueName = "";
    private LatticeQueueCore _core = null!;
    private bool _initialized;

    /// <inheritdoc />
    IGrainContext IGrainBase.GrainContext => context;

    /// <inheritdoc />
    public async Task OnActivateAsync(CancellationToken cancellationToken)
    {
        var key = context.GrainId.Key.ToString();
        if (string.IsNullOrEmpty(key))
        {
            throw new InvalidOperationException(
                $"{nameof(LatticeQueueGrain)} activation key is empty; expected the queue name.");
        }

        _queueName = key;
        var store = grainFactory.GetGrain<ISystemLattice>(BackingTreeId(_queueName));
        _core = new LatticeQueueCore(store, EntryKeyPrefix, persistHeadCursor: true);
        await _core.InitializeAsync(cancellationToken).ConfigureAwait(true);
        _initialized = true;
    }

    /// <inheritdoc />
    public async Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        if (_initialized)
        {
            await _core.FlushHeadCursorAsync(cancellationToken).ConfigureAwait(true);
        }
    }

    /// <summary>
    /// Test-only initialisation seam. Bypasses Orleans activation by
    /// supplying the queue name and a pre-bound <see cref="ISystemLattice"/>
    /// store, then runs the same bulk-load
    /// <see cref="OnActivateAsync(CancellationToken)"/> uses.
    /// </summary>
    internal async Task InitializeForTestingAsync(string queueName, ISystemLattice store, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(queueName);
        ArgumentNullException.ThrowIfNull(store);

        _queueName = queueName;
        _core = new LatticeQueueCore(store, EntryKeyPrefix, persistHeadCursor: true);
        await _core.InitializeAsync(cancellationToken).ConfigureAwait(true);
        _initialized = true;
    }

    /// <inheritdoc />
    public async Task<long> EnqueueAsync(byte[] value, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(value);
        EnsureInitialized();
        var capacity = optionsMonitor.Get(_queueName).QueueCapacity;
        return await _core.EnqueueAsync(_ => value, capacity, cancellationToken).ConfigureAwait(true);
    }

    /// <inheritdoc />
    public async Task<LatticeQueueByteEntry?> TryDequeueAsync(CancellationToken cancellationToken = default)
    {
        EnsureInitialized();
        var head = await _core.TryDequeueAsync(cancellationToken).ConfigureAwait(true);
        return head is { } h ? new LatticeQueueByteEntry { EntryId = h.Id, Value = h.Value } : null;
    }

    /// <inheritdoc />
    public Task<LatticeQueueByteEntry?> PeekAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();
        var head = _core.Peek();
        return Task.FromResult(head is { } h ? new LatticeQueueByteEntry { EntryId = h.Id, Value = h.Value } : (LatticeQueueByteEntry?)null);
    }

    /// <inheritdoc />
    public Task<int> CountAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();
        return Task.FromResult(_core.Count);
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<LatticeQueueByteEntry>> ListAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();
        var snapshot = _core.Snapshot();
        var result = new LatticeQueueByteEntry[snapshot.Count];
        for (var i = 0; i < snapshot.Count; i++)
        {
            result[i] = new LatticeQueueByteEntry { EntryId = snapshot[i].Id, Value = snapshot[i].Value };
        }
        return Task.FromResult<IReadOnlyList<LatticeQueueByteEntry>>(result);
    }

    private void EnsureInitialized()
    {
        if (!_initialized)
        {
            throw new InvalidOperationException(
                $"{nameof(LatticeQueueGrain)} for queue '{_queueName}' has not completed activation.");
        }
    }

    /// <summary>
    /// Composes the reserved system-tree id backing the queue named
    /// <paramref name="queueName"/>. Lives under
    /// <see cref="LatticeConstants.QueueTreePrefix"/> (itself subsumed by
    /// the reserved <see cref="LatticeConstants.SystemTreePrefix"/>) so user
    /// trees cannot collide with it.
    /// </summary>
    internal static string BackingTreeId(string queueName) => $"{LatticeConstants.QueueTreePrefix}{queueName}";
}
