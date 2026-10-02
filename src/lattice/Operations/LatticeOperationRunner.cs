using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Operations;

/// <summary>
/// The reusable engine-side coordinator for long-running operations. A client
/// package (backup and restore first; schema remediation, view rebuild, WAL move
/// and the rest later) hands it a unit of work and gets back an operation that is
/// tracked durably by an <see cref="ILatticeOperationGrain"/>: idempotent start by
/// id, progress through an <see cref="ILatticeOperationProgress"/> sink, cancellation
/// from any silo, bounded retention, and <see cref="LatticeOperationState.Failed"/>
/// rather than a stuck operation when this silo is lost.
/// </summary>
/// <remarks>
/// <para>
/// The work runs <b>in this process</b>, on the silo that accepted it, on the
/// caller's captured execution context (so its credential and tenant flow into
/// the engine exactly as they would for a direct call), and independently of the
/// caller's own call: the caller's cancellation token does not reach it. Running
/// in-process also means a caller that wants to wait can await
/// <see cref="LatticeOperationLaunch{TResult}.Completion"/> and observe the
/// engine's own result and exception types, unchanged by any serialization hop.
/// </para>
/// <para>
/// Resuming an interrupted operation is out of scope: losing this silo loses the
/// work, and the tracking grain then reports the operation failed (see
/// <see cref="LatticeOperationGrain"/>).
/// </para>
/// </remarks>
internal sealed class LatticeOperationRunner(
    IGrainFactory grainFactory,
    ILocalSiloDetails localSilo,
    IOptions<LatticeOperationOptions> options,
    ILogger<LatticeOperationRunner> logger)
{
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _running = new(StringComparer.Ordinal);

    /// <summary>
    /// The clock that paces heartbeats. Defaults to <see cref="TimeProvider.System"/>;
    /// unit tests substitute a controllable one.
    /// </summary>
    internal TimeProvider Clock { get; set; } = TimeProvider.System;

    /// <summary>The number of operations currently running in this process.</summary>
    internal int RunningCount => _running.Count;

    /// <summary>
    /// Starts an operation, or returns the existing one when an operation with the
    /// same tenant and id already exists (an idempotent start, which starts nothing).
    /// </summary>
    /// <typeparam name="TResult">The engine result type.</typeparam>
    /// <param name="start">What to start. Must not be <c>null</c>.</param>
    /// <param name="work">The work. Receives the progress sink and the operation's cancellation token.</param>
    /// <param name="onSucceeded">Maps the engine result to the recorded completion.</param>
    /// <returns>The accepted record and, when this call started the work, its in-process task.</returns>
    /// <exception cref="ArgumentNullException">An argument is <c>null</c>.</exception>
    /// <exception cref="ArgumentException">The operation id is malformed.</exception>
    /// <exception cref="InvalidOperationException">The id is in use by an operation of a different kind.</exception>
    public async Task<LatticeOperationLaunch<TResult>> StartAsync<TResult>(
        LatticeOperationStart start,
        Func<ILatticeOperationProgress, CancellationToken, Task<TResult>> work,
        Func<TResult, LatticeOperationCompletion> onSucceeded)
    {
        ArgumentNullException.ThrowIfNull(start);
        ArgumentNullException.ThrowIfNull(work);
        ArgumentNullException.ThrowIfNull(onSucceeded);
        ArgumentException.ThrowIfNullOrEmpty(start.TenantId);
        ArgumentException.ThrowIfNullOrEmpty(start.Kind);
        LatticeOperationKey.ThrowIfInvalid(start.OperationId, nameof(start));

        var key = LatticeOperationKey.For(start.TenantId, start.OperationId);
        var grain = grainFactory.GetGrain<ILatticeOperationGrain>(key);
        var begin = await grain.BeginAsync(new LatticeOperationBeginRequest
        {
            Kind = start.Kind,
            TreeIds = start.TreeIds,
            Phases = start.Phases,
            RunnerSilo = localSilo.SiloAddress,
            Attributes = start.Attributes,
        }).ConfigureAwait(false);

        if (!begin.Created)
        {
            return new LatticeOperationLaunch<TResult>(begin.Record, null);
        }

        var cancellation = new CancellationTokenSource();
        _running[key] = cancellation;
        var sink = new LatticeOperationProgressSink(grain, cancellation);
        var firstPhase = start.Phases.Count > 0 ? start.Phases[0] : start.Kind;

        // Task.Run captures the caller's execution context, so the engine sees the
        // caller's credential and tenant, but not its cancellation token.
        var completion = Task.Run(() => RunAsync(key, grain, sink, cancellation, firstPhase, work, onSucceeded));

        // A start-only caller never awaits the task; observe a fault so it is not
        // reported as unobserved. The fault is recorded on the grain regardless.
        _ = completion.ContinueWith(
            static t => _ = t.Exception,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

        return new LatticeOperationLaunch<TResult>(begin.Record, completion);
    }

    /// <summary>Reads an operation's record.</summary>
    /// <param name="tenantId">The owning tenant.</param>
    /// <param name="operationId">The operation id.</param>
    /// <returns>The record, or <see langword="null"/> when none exists in that tenant.</returns>
    public Task<LatticeOperationRecord?> GetAsync(string tenantId, string operationId)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        if (!LatticeOperationKey.IsValid(operationId))
        {
            return Task.FromResult<LatticeOperationRecord?>(null);
        }

        return grainFactory.GetGrain<ILatticeOperationGrain>(LatticeOperationKey.For(tenantId, operationId)).GetAsync();
    }

    /// <summary>
    /// Requests cancellation. Cancels at once when the operation runs in this
    /// process; otherwise its runner observes the request at its next report or
    /// heartbeat.
    /// </summary>
    /// <param name="tenantId">The owning tenant.</param>
    /// <param name="operationId">The operation id.</param>
    /// <returns>The record, or <see langword="null"/> when none exists in that tenant.</returns>
    public async Task<LatticeOperationRecord?> RequestCancelAsync(string tenantId, string operationId)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        if (!LatticeOperationKey.IsValid(operationId))
        {
            return null;
        }

        var key = LatticeOperationKey.For(tenantId, operationId);
        var record = await grainFactory.GetGrain<ILatticeOperationGrain>(key).RequestCancelAsync().ConfigureAwait(false);
        if (record is { IsTerminal: false } && _running.TryGetValue(key, out var cancellation))
        {
            cancellation.Cancel();
        }

        return record;
    }

    /// <summary>Lists one page of a tenant's operations, newest-first.</summary>
    /// <param name="tenantId">The owning tenant.</param>
    /// <param name="kindPrefix">When not <see langword="null"/>, only kinds with this prefix are listed.</param>
    /// <param name="pageToken">The previous page's token, or <see langword="null"/>.</param>
    /// <param name="pageSize">The maximum records to return. Must be positive.</param>
    /// <returns>The records (an operation pruned since it was indexed is skipped) and the next page token.</returns>
    public async Task<(IReadOnlyList<LatticeOperationRecord> Records, string? NextPageToken)> ListAsync(
        string tenantId,
        string? kindPrefix,
        string? pageToken,
        int pageSize)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        var page = await grainFactory
            .GetGrain<ILatticeOperationIndexGrain>(LatticeOperationKey.ForIndex(tenantId))
            .ListAsync(kindPrefix, pageToken, pageSize)
            .ConfigureAwait(false);

        var reads = new Task<LatticeOperationRecord?>[page.OperationIds.Count];
        for (var i = 0; i < reads.Length; i++)
        {
            reads[i] = grainFactory
                .GetGrain<ILatticeOperationGrain>(LatticeOperationKey.For(tenantId, page.OperationIds[i]))
                .GetAsync();
        }

        var records = await Task.WhenAll(reads).ConfigureAwait(false);
        var live = new List<LatticeOperationRecord>(records.Length);
        foreach (var record in records)
        {
            if (record is not null)
            {
                live.Add(record);
            }
        }

        return (live, page.NextPageToken);
    }

    private async Task<TResult> RunAsync<TResult>(
        string key,
        ILatticeOperationGrain grain,
        LatticeOperationProgressSink sink,
        CancellationTokenSource cancellation,
        string firstPhase,
        Func<ILatticeOperationProgress, CancellationToken, Task<TResult>> work,
        Func<TResult, LatticeOperationCompletion> onSucceeded)
    {
        using var heartbeatStop = new CancellationTokenSource();
        var heartbeat = HeartbeatAsync(grain, cancellation, heartbeatStop.Token);
        try
        {
            TResult result;
            using (LatticeOperationProgress.Enter(sink))
            {
                await sink.ReportAsync(firstPhase).ConfigureAwait(false);
                result = await work(sink, cancellation.Token).ConfigureAwait(false);
            }

            await sink.FlushAsync().ConfigureAwait(false);
            await RecordAsync(grain, onSucceeded(result)).ConfigureAwait(false);
            return result;
        }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
        {
            await BankProgressAsync(sink).ConfigureAwait(false);
            await RecordAsync(grain, LatticeOperationCompletion.Cancelled("Cancellation was requested.")).ConfigureAwait(false);
            throw;
        }
        catch (Exception ex)
        {
            await BankProgressAsync(sink).ConfigureAwait(false);

            // A tracked grain call observes a cancel request through its own relay
            // and stops before this runner's heartbeat has seen it, so its
            // cancellation surfaces here with this runner's token still live.
            var completion = ex is OperationCanceledException && await IsCancelRequestedAsync(grain).ConfigureAwait(false)
                ? LatticeOperationCompletion.Cancelled("Cancellation was requested.")
                : LatticeOperationCompletion.Failed($"{ex.GetType().Name}: {ex.Message}");
            await RecordAsync(grain, completion).ConfigureAwait(false);
            throw;
        }
        finally
        {
            heartbeatStop.Cancel();
            await heartbeat.ConfigureAwait(false);
            _running.TryRemove(key, out _);
        }
    }

    /// <summary>
    /// Banks the coalesced progress before a fault or cancellation is recorded, so
    /// the units completed before the fault are never lost. It never swallows the
    /// operation's own fault: the caller rethrows after it returns.
    /// </summary>
    private static Task BankProgressAsync(LatticeOperationProgressSink sink) => sink.BankProgressAsync();

    private async Task<bool> IsCancelRequestedAsync(ILatticeOperationGrain grain)
    {
        try
        {
            return await grain.GetAsync().ConfigureAwait(false) is { CancelRequested: true };
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Could not read whether a coordinated operation was cancelled; recording it as failed.");
            return false;
        }
    }

    private async Task RecordAsync(ILatticeOperationGrain grain, LatticeOperationCompletion completion)
    {
        try
        {
            await grain.CompleteAsync(completion).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // The heartbeat lease turns an unrecorded outcome into Failed later; the
            // in-process caller still receives the real outcome.
            logger.LogWarning(ex, "Could not record the {State} outcome of a coordinated operation.", completion.State);
        }
    }

    private async Task HeartbeatAsync(
        ILatticeOperationGrain grain,
        CancellationTokenSource cancellation,
        CancellationToken stop)
    {
        var interval = options.Value.HeartbeatInterval;
        while (!stop.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(interval, Clock, stop).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            try
            {
                if (await grain.HeartbeatAsync().ConfigureAwait(false))
                {
                    cancellation.Cancel();
                }
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex, "A coordinated-operation heartbeat failed; it is retried on the next interval.");
            }
        }
    }
}
