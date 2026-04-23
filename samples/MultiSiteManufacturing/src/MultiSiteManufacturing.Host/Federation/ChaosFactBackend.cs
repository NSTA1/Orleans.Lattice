using MultiSiteManufacturing.Host.Domain;

namespace MultiSiteManufacturing.Host.Federation;

/// <summary>
/// Decorator over an <see cref="IFactBackend"/> that consults the
/// per-backend <see cref="IBackendChaosGrain"/> on every emit and
/// applies jitter, transient failures, and write amplification as
/// configured.
/// </summary>
/// <remarks>
/// Plan §4.3 Tier 2. Read paths (<see cref="GetStateAsync"/>,
/// <see cref="GetFactsAsync"/>, <see cref="ListPartsAsync"/>) pass
/// straight through to the inner backend — chaos only affects writes,
/// mirroring real storage failure modes (partial writes, retry
/// amplification, and latency) without corrupting the read layer.
/// </remarks>
public sealed class ChaosFactBackend : IFactBackend
{
    private readonly IFactBackend _inner;
    private readonly IGrainFactory _grains;
    private readonly Func<Random> _rngFactory;

    // Reorder buffer state (plan §4.3 Tier 2 "ReorderWindowMs"). A single
    // pending batch per decorator instance; the first write to arrive
    // opens the window and schedules a flush, subsequent writes within
    // the window join the same batch and share its (shuffled) drain.
    private readonly object _reorderLock = new();
    private List<ReorderEntry>? _pendingBatch;
    private Task? _flushTask;

    /// <summary>Creates a decorator with a per-thread <see cref="Random"/>.</summary>
    public ChaosFactBackend(IFactBackend inner, IGrainFactory grains)
        : this(inner, grains, () => Random.Shared)
    {
    }

    /// <summary>Creates a decorator with a caller-supplied RNG factory (used by tests for determinism).</summary>
    public ChaosFactBackend(IFactBackend inner, IGrainFactory grains, Func<Random> rngFactory)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(grains);
        ArgumentNullException.ThrowIfNull(rngFactory);
        _inner = inner;
        _grains = grains;
        _rngFactory = rngFactory;
    }

    /// <inheritdoc />
    public string Name => _inner.Name;

    /// <summary>Access to the undecorated backend (useful for test assertions).</summary>
    public IFactBackend Inner => _inner;

    /// <inheritdoc />
    public async Task EmitAsync(Fact fact, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fact);

        var config = await ChaosGrain().GetConfigAsync();

        if (config.ReorderWindowMs > 0)
        {
            await EnqueueForReorderAsync(fact, config.ReorderWindowMs, cancellationToken);
            return;
        }

        await ApplyChaosAndEmitAsync(fact, config, cancellationToken);
    }

    /// <summary>
    /// Inline (non-buffered) path: jitter → transient-failure roll →
    /// inner emit → optional write-amplification. Shared by both the
    /// direct path and the reorder-buffer flush path.
    /// </summary>
    private async Task ApplyChaosAndEmitAsync(
        Fact fact,
        BackendChaosConfig config,
        CancellationToken cancellationToken)
    {
        var rng = _rngFactory();

        if (config.JitterMsMax > 0)
        {
            var delay = rng.Next(config.JitterMsMin, config.JitterMsMax + 1);
            if (delay > 0)
            {
                await Task.Delay(delay, cancellationToken);
            }
        }

        if (config.TransientFailureRate > 0 && rng.NextDouble() < config.TransientFailureRate)
        {
            throw new ChaosTransientFailureException(
                $"Chaos: transient failure injected on backend '{Name}' for fact {fact.FactId}.");
        }

        await _inner.EmitAsync(fact, cancellationToken);

        if (config.WriteAmplificationRate > 0 && rng.NextDouble() < config.WriteAmplificationRate)
        {
            await _inner.EmitAsync(fact, cancellationToken);
        }
    }

    private Task EnqueueForReorderAsync(Fact fact, int windowMs, CancellationToken cancellationToken)
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entry = new ReorderEntry(fact, tcs, cancellationToken);

        lock (_reorderLock)
        {
            _pendingBatch ??= new List<ReorderEntry>();
            _pendingBatch.Add(entry);
            _flushTask ??= Task.Run(() => FlushAfterDelayAsync(windowMs));
        }

        return tcs.Task;
    }

    private async Task FlushAfterDelayAsync(int windowMs)
    {
        try
        {
            await Task.Delay(windowMs).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // No cancellation token is threaded into the flush timer on
            // purpose — individual entries carry their own tokens below.
        }

        List<ReorderEntry> batch;
        lock (_reorderLock)
        {
            batch = _pendingBatch ?? new List<ReorderEntry>();
            _pendingBatch = null;
            _flushTask = null;
        }

        if (batch.Count == 0)
        {
            return;
        }

        // Fisher–Yates shuffle using the caller-supplied RNG factory.
        var rng = _rngFactory();
        for (var i = batch.Count - 1; i > 0; i--)
        {
            var j = rng.Next(i + 1);
            (batch[i], batch[j]) = (batch[j], batch[i]);
        }

        // Read chaos config once per batch so jitter / fault-rate /
        // amplification decisions are consistent across the drain.
        // Strip ReorderWindowMs so the recursive call doesn't re-buffer.
        var config = await ChaosGrain().GetConfigAsync().ConfigureAwait(false);
        config = config with { ReorderWindowMs = 0 };

        foreach (var entry in batch)
        {
            if (entry.Token.IsCancellationRequested)
            {
                entry.Completion.TrySetCanceled(entry.Token);
                continue;
            }

            try
            {
                await ApplyChaosAndEmitAsync(entry.Fact, config, entry.Token).ConfigureAwait(false);
                entry.Completion.TrySetResult();
            }
            catch (OperationCanceledException oce) when (entry.Token.IsCancellationRequested)
            {
                entry.Completion.TrySetCanceled(oce.CancellationToken);
            }
            catch (Exception ex)
            {
                entry.Completion.TrySetException(ex);
            }
        }
    }

    /// <summary>One entry in the reorder buffer: the fact, the caller's completion source, and the caller's cancellation token.</summary>
    private readonly record struct ReorderEntry(Fact Fact, TaskCompletionSource Completion, CancellationToken Token);

    /// <inheritdoc />
    public Task<ComplianceState> GetStateAsync(PartSerialNumber serial, CancellationToken cancellationToken = default) =>
        _inner.GetStateAsync(serial, cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyList<Fact>> GetFactsAsync(PartSerialNumber serial, CancellationToken cancellationToken = default) =>
        _inner.GetFactsAsync(serial, cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyList<PartSerialNumber>> ListPartsAsync(CancellationToken cancellationToken = default) =>
        _inner.ListPartsAsync(cancellationToken);

    private IBackendChaosGrain ChaosGrain() =>
        _grains.GetGrain<IBackendChaosGrain>(_inner.Name);
}

/// <summary>Thrown by <see cref="ChaosFactBackend"/> when a transient-failure roll lands on a write.</summary>
public sealed class ChaosTransientFailureException : Exception
{
    /// <summary>Creates a new exception with the given message.</summary>
    public ChaosTransientFailureException(string message) : base(message) { }
}
