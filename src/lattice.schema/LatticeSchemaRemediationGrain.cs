using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Schema;

/// <summary>
/// The durable, per-tree background schema-remediation coordinator. One activation
/// exists per logical tree, keyed by <c>{treeId}</c>. Given a target
/// <see cref="LatticeSchemaPolicy"/> and a caller-supplied remediation
/// <see cref="LatticeValueTransform"/>, it:
/// <list type="number">
/// <item><description>runs a read-only dry-run gate over the tree's current entries
/// (rewrite each value, revalidate against the target policy) and aborts with no
/// cutover on the first offending key, leaving the original tree untouched;</description></item>
/// <item><description>builds a fresh destination physical tree by scanning source
/// entries, transforming each value, revalidating it, and writing it into the
/// destination - aborting and discarding the partial destination on the first
/// offending value;</description></item>
/// <item><description>cuts over atomically by repointing the logical tree's alias
/// (<see cref="ILatticeRegistry.SetAliasAsync"/>) to the destination and installing
/// the target policy so subsequent writes are enforced.</description></item>
/// </list>
/// The coordinator mirrors <c>TreeResizeGrain</c>'s durability discipline: it
/// persists each phase transition before performing that phase's external side
/// effects, and rolls back the in-memory state on a <c>WriteStateAsync</c> failure.
/// A duplicate trigger with the same parameters resumes idempotently; a trigger
/// with different parameters while a remediation is in flight throws.
/// <para>
/// <b>Concurrent-writes contract (v1).</b> The build copies at the logical
/// <see cref="ILattice"/> level and does not shadow-forward writes that land on the
/// source during the build window, so remediation requires the tree be
/// write-quiesced for the duration of the build. A write accepted on the source
/// after the dry-run scan but before cutover is not carried into the destination
/// and is superseded by the alias swap. Making the build lossless under concurrent
/// writes (an online snapshot that rewrites values as it copies, reusing the resize
/// shadow-forward machinery) is the documented follow-up.
/// </para>
/// Key format: <c>{treeId}</c>.
/// </summary>
internal sealed class LatticeSchemaRemediationGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    ILatticeSchemaPolicyStore policyStore,
    ILatticeSchemaPolicyProvider policyProvider,
    IOptions<LatticeSchemaEnforcementOptions> options,
    ILogger<LatticeSchemaRemediationGrain> logger,
    [PersistentState("schema-remediation", LatticeOptions.StorageProviderName)]
    IPersistentState<SchemaRemediationState> state)
    : IGrainBase, ILatticeSchemaRemediationGrain
{
    private readonly int _previewMaxBytes = Math.Max(1, options.Value.DeadLetterPreviewMaxBytes);

    IGrainContext IGrainBase.GrainContext => context;

    private string TreeId => context.GrainId.Key.ToString()!;

    /// <inheritdoc />
    public async Task<LatticeSchemaRemediationReport> StartAsync(
        LatticeValueTransform transform,
        LatticeSchemaPolicy targetPolicy,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(targetPolicy);

        // Reject an uncompilable / non-linear regex here rather than mid-build.
        _ = CompiledSchemaPolicy.Compile(targetPolicy);

        if (state.State.InProgress)
        {
            if (IsSameParameters(transform, targetPolicy))
            {
                // Idempotent resume: drive the in-flight remediation to completion.
                await RunRemediationPassAsync();
                return GetStatus();
            }

            throw new InvalidOperationException(
                $"A schema remediation is already in progress for tree '{TreeId}' with different parameters.");
        }

        await InitiateAsync(transform, targetPolicy);
        await RunRemediationPassAsync();
        return GetStatus();
    }

    /// <inheritdoc />
    public async Task RunRemediationPassAsync()
    {
        if (!state.State.InProgress)
        {
            return;
        }

        // Remediation is enforcement infrastructure: its reads and writes to the
        // source, destination, and reserved policy tree run under a system-origin
        // scope so the access gate never blocks them and the write interceptor is
        // not re-entered while the destination is populated.
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            if (state.State.Phase == LatticeSchemaRemediationPhase.DryRun)
            {
                if (!await RunDryRunGateAsync())
                {
                    return;
                }
            }

            if (state.State.Phase == LatticeSchemaRemediationPhase.Build)
            {
                if (!await BuildDestinationAsync())
                {
                    return;
                }
            }

            if (state.State.Phase == LatticeSchemaRemediationPhase.Cutover)
            {
                await CutoverAsync();
            }

            await CompleteAsync();
        }
    }

    /// <inheritdoc />
    public Task<LatticeSchemaRemediationReport> GetStatusAsync() => Task.FromResult(GetStatus());

    private LatticeSchemaRemediationReport GetStatus()
    {
        if (state.State.InProgress)
        {
            return LatticeSchemaRemediationReport.InFlight(
                state.State.Phase, state.State.ScannedCount, state.State.DestinationTreeId, state.State.OperationId);
        }

        return state.State.LastReport ?? LatticeSchemaRemediationReport.Idle;
    }

    private async Task InitiateAsync(LatticeValueTransform transform, LatticeSchemaPolicy targetPolicy)
    {
        var operationId = Guid.NewGuid().ToString("N");
        var destinationTreeId = $"{TreeId}/remediated/{operationId}";

        // Snapshot every field this method writes so a transient WriteStateAsync
        // failure cannot leak in-memory mutations past the InProgress guard.
        var prevInProgress = state.State.InProgress;
        var prevPhase = state.State.Phase;
        var prevOperationId = state.State.OperationId;
        var prevDestinationTreeId = state.State.DestinationTreeId;
        var prevTransform = state.State.Transform;
        var prevTargetPolicy = state.State.TargetPolicy;
        var prevLastReport = state.State.LastReport;
        var prevScannedCount = state.State.ScannedCount;

        // Persist intent BEFORE any external side effect.
        state.State.InProgress = true;
        state.State.Phase = LatticeSchemaRemediationPhase.DryRun;
        state.State.OperationId = operationId;
        state.State.DestinationTreeId = destinationTreeId;
        state.State.Transform = transform;
        state.State.TargetPolicy = targetPolicy;
        state.State.LastReport = null;
        state.State.ScannedCount = 0;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.InProgress = prevInProgress;
            state.State.Phase = prevPhase;
            state.State.OperationId = prevOperationId;
            state.State.DestinationTreeId = prevDestinationTreeId;
            state.State.Transform = prevTransform;
            state.State.TargetPolicy = prevTargetPolicy;
            state.State.LastReport = prevLastReport;
            state.State.ScannedCount = prevScannedCount;
            throw;
        }
    }

    /// <summary>
    /// Runs the read-only dry-run gate. Returns <c>true</c> to advance to the build
    /// phase, <c>false</c> when the remediation aborted (no destination was built;
    /// the original tree is untouched).
    /// </summary>
    private async Task<bool> RunDryRunGateAsync()
    {
        var source = grainFactory.GetGrain<ILattice>(TreeId);
        var outcome = await LatticeSchemaRemediation.DryRunAsync(
            source.EntriesAsync(),
            state.State.Transform,
            state.State.TargetPolicy!,
            _previewMaxBytes);

        if (!outcome.Succeeded)
        {
            await AbortAsync(outcome.ScannedCount, outcome.OffendingKey!, outcome.Reason!, outcome.OffendingValuePreview!)
                ;
            return false;
        }

        await AdvancePhaseAsync(LatticeSchemaRemediationPhase.Build, outcome.ScannedCount);
        return true;
    }

    /// <summary>
    /// Populates the destination physical tree with transformed, revalidated
    /// values. Returns <c>true</c> to advance to cutover, <c>false</c> when the
    /// remediation aborted (the partial destination is discarded).
    /// </summary>
    private async Task<bool> BuildDestinationAsync()
    {
        var source = grainFactory.GetGrain<ILattice>(TreeId);
        var destination = grainFactory.GetGrain<ILattice>(state.State.DestinationTreeId!);
        var compiled = CompiledSchemaPolicy.Compile(state.State.TargetPolicy!);
        var transform = state.State.Transform;
        var scanned = 0;

        await foreach (var entry in source.EntriesAsync())
        {
            scanned++;

            byte[] transformed;
            try
            {
                transformed = LatticeValueTransformEvaluation.Evaluate(entry.Value, in transform);
            }
            catch (InvalidOperationException ex)
            {
                await DiscardDestinationAsync(destination);
                await AbortAsync(scanned, entry.Key, ex.Message, Preview(entry.Value));
                return false;
            }

            var reason = compiled.Validate(transformed);
            if (reason is not null)
            {
                await DiscardDestinationAsync(destination);
                await AbortAsync(scanned, entry.Key, reason, Preview(transformed));
                return false;
            }

            await destination.SetAsync(entry.Key, transformed);
        }

        await AdvancePhaseAsync(LatticeSchemaRemediationPhase.Cutover, scanned);
        return true;
    }

    /// <summary>
    /// Atomically repoints the logical tree to the destination and installs the
    /// target policy. Idempotent: re-running after a mid-cutover restart repeats
    /// the same alias and policy writes.
    /// </summary>
    private async Task CutoverAsync()
    {
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.SetAliasAsync(TreeId, state.State.DestinationTreeId!);

        // Enforce the shape the data now satisfies on subsequent writes. Evict the
        // local policy cache eagerly (as the admin does) so the coordinating silo
        // enforces the new policy on its next write without waiting for the change
        // feed to propagate the eviction.
        await policyStore.SetPolicyAsync(TreeId, state.State.TargetPolicy!);
        policyProvider.Invalidate(TreeId);
    }

    private async Task DiscardDestinationAsync(ILattice destination)
    {
        try
        {
            await destination.DeleteTreeAsync();
        }
        catch (Exception ex)
        {
            // A failed soft-delete of the throwaway destination must not mask the
            // remediation abort; the orphan is left for the soft-delete sweeper.
            logger.LogWarning(
                ex, "Schema remediation for tree '{TreeId}' failed to discard partial destination '{DestinationTreeId}'.",
                TreeId, state.State.DestinationTreeId);
        }
    }

    private async Task AdvancePhaseAsync(LatticeSchemaRemediationPhase phase, int scannedCount)
    {
        var prevPhase = state.State.Phase;
        var prevScannedCount = state.State.ScannedCount;

        state.State.Phase = phase;
        state.State.ScannedCount = scannedCount;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Phase = prevPhase;
            state.State.ScannedCount = prevScannedCount;
            throw;
        }
    }

    private async Task CompleteAsync()
    {
        var prevInProgress = state.State.InProgress;
        var prevPhase = state.State.Phase;
        var prevLastReport = state.State.LastReport;

        state.State.InProgress = false;
        state.State.Phase = LatticeSchemaRemediationPhase.Completed;
        state.State.LastReport = LatticeSchemaRemediationReport.Completed(
            state.State.ScannedCount, state.State.DestinationTreeId!, state.State.OperationId!);
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.InProgress = prevInProgress;
            state.State.Phase = prevPhase;
            state.State.LastReport = prevLastReport;
            throw;
        }
    }

    private async Task AbortAsync(int scannedCount, string offendingKey, string reason, byte[] offendingValuePreview)
    {
        var prevInProgress = state.State.InProgress;
        var prevPhase = state.State.Phase;
        var prevLastReport = state.State.LastReport;
        var prevScannedCount = state.State.ScannedCount;

        state.State.InProgress = false;
        state.State.Phase = LatticeSchemaRemediationPhase.Aborted;
        state.State.ScannedCount = scannedCount;
        state.State.LastReport = LatticeSchemaRemediationReport.Aborted(
            scannedCount, offendingKey, reason, offendingValuePreview, state.State.OperationId!);
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.InProgress = prevInProgress;
            state.State.Phase = prevPhase;
            state.State.LastReport = prevLastReport;
            state.State.ScannedCount = prevScannedCount;
            throw;
        }
    }

    private bool IsSameParameters(LatticeValueTransform transform, LatticeSchemaPolicy targetPolicy) =>
        state.State.Transform.Equals(transform) && PolicyEquivalent(state.State.TargetPolicy, targetPolicy);

    private static bool PolicyEquivalent(LatticeSchemaPolicy? a, LatticeSchemaPolicy? b)
    {
        if (ReferenceEquals(a, b))
        {
            return true;
        }

        if (a is null || b is null)
        {
            return false;
        }

        return a.StrictIngest == b.StrictIngest
            && a.Rules.Count == b.Rules.Count
            && a.Rules.SequenceEqual(b.Rules);
    }

    private byte[] Preview(byte[]? value)
    {
        if (value is null || value.Length == 0)
        {
            return Array.Empty<byte>();
        }

        var length = Math.Min(value.Length, _previewMaxBytes);
        return value.AsSpan(0, length).ToArray();
    }
}
