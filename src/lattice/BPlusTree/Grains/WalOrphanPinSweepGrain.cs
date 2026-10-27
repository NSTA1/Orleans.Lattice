using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Runs one tree's WAL GC orphan-pin sweep at most once per
/// <see cref="LatticeWalGcScheduler.OrphanSweepInterval"/> for the whole
/// cluster, keyed by <c>{treeId}</c> (issue #5097).
/// <para>
/// Reuses <see cref="CoordinatorGrain{TSelf}"/> for its phase-timer work pump,
/// failure accounting and teardown token. Demand is in memory only and no
/// keepalive reminder is registered: every silo's scheduler re-raises demand on
/// each pass over a blocked tree, so a lost activation loses nothing that the
/// next pass does not restore. The phase timer does not keep the activation
/// alive, so a tree that stops asking lets it be collected.
/// </para>
/// <para>
/// <see cref="RequestSweepAsync"/> interleaves and never waits for a sweep, so a
/// long sweep never stalls a scheduler pass on any silo.
/// </para>
/// </summary>
internal sealed class WalOrphanPinSweepGrain(
    IGrainContext context,
    IReminderRegistry reminderRegistry,
    IWalOrphanPinSweepRunner runner,
    ILogger<WalOrphanPinSweepGrain> logger)
    : CoordinatorGrain<WalOrphanPinSweepGrain>(context, reminderRegistry, logger), IWalOrphanPinSweepGrain
{
    private readonly Guid _epoch = Guid.NewGuid();
    private bool _demandPending;
    private bool _classifyPending;
    private bool _running;
    private bool _unavailable;
    private DateTimeOffset? _lastSweepStarted;
    private long _generation;
    private WalOrphanPinSweepOutcome? _latest;

    /// <summary>Clock used to enforce the sweep interval. Settable for tests.</summary>
    internal TimeProvider TimeProvider { get; set; } = TimeProvider.System;

    private string TreeId => Context.GrainId.Key.ToString()!;

    /// <inheritdoc/>
    protected override string KeepaliveReminderName => "wal-orphan-pin-sweep-keepalive";

    /// <inheritdoc/>
    protected override bool InProgress => _demandPending;

    /// <inheritdoc/>
    protected override TimeSpan PhaseTimerPeriod => TimeSpan.FromSeconds(5);

    /// <inheritdoc/>
    public Task<WalOrphanPinSweepOutcome?> RequestSweepAsync(bool classifyFloorHolders)
    {
        _demandPending = true;
        _classifyPending |= classifyFloorHolders;
        StartPhaseTimer();
        return Task.FromResult(_unavailable ? WalOrphanPinSweepOutcome.Unavailable : _latest);
    }

    /// <inheritdoc/>
    protected internal override async Task ProcessNextPhaseAsync()
    {
        if (!_demandPending || _running)
        {
            return;
        }

        var now = TimeProvider.GetUtcNow();
        if (_lastSweepStarted is { } last && now - last < LatticeWalGcScheduler.OrphanSweepInterval)
        {
            return;
        }

        var classify = _classifyPending;
        _demandPending = false;
        _classifyPending = false;
        _lastSweepStarted = now;
        _running = true;
        try
        {
            var outcome = await runner.RunAsync(TreeId, classify, PhaseTickToken);
            if (outcome is not null)
            {
                _latest = outcome with { Epoch = _epoch, Generation = ++_generation };
                _unavailable = false;
            }
            else
            {
                _unavailable = true;
                _demandPending = true;
                _classifyPending |= classify;
                Logger.LogWarning(
                    "WAL GC orphan sweep coordinator for tree {Tree} has no leaf-state storage; callers must sweep locally.",
                    TreeId);
            }
        }
        catch
        {
            // The demand that started this sweep is not served, so it stays owed.
            _demandPending = true;
            _classifyPending |= classify;
            _unavailable = true;
            throw;
        }
        finally
        {
            _running = false;
        }
    }
}
