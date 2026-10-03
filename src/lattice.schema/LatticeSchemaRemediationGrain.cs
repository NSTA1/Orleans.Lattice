using System.Runtime.ExceptionServices;
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
/// offending value. The destination is registered with the source's routing map,
/// split allocation mark, structural pins and runtime configuration overrides
/// (<see cref="DerivedTreeEntries.InheritingAsync"/>), so it lays keys out exactly
/// as the source does and only the values change;</description></item>
/// <item><description>cuts over by installing the target policy, then repointing
/// the logical tree's alias to the destination together with the destination's
/// shard map in one registry write, and arming the source to redirect stale
/// routers, so subsequent writes are enforced.</description></item>
/// </list>
/// The coordinator mirrors <c>TreeResizeGrain</c>'s durability discipline: it
/// persists each phase transition before performing that phase's external side
/// effects, and rolls back the in-memory state on a <c>WriteStateAsync</c> failure.
/// A duplicate trigger with the same parameters resumes idempotently; a trigger
/// with different parameters while a remediation is in flight throws.
/// <para>
/// <b>Two build modes.</b> The same dry-run / build / cutover / durable-state /
/// idempotent-resume / abort machinery serves both an enforcement remediation
/// (<see cref="StartAsync"/>: one static <see cref="LatticeValueTransform"/> per
/// value, revalidated against a <b>new</b> target policy that is installed at
/// cutover) and an eager schema-version migration
/// (<see cref="StartVersionMigrationAsync"/>: each value re-stamped to the tree's
/// target schema version through the registry's upcaster chain, revalidated against
/// the tree's <b>existing</b> policy when it has one, which is left untouched at
/// cutover). <see cref="SchemaRemediationState.Mode"/> selects the per-value rewrite
/// and the cutover policy behaviour; both are persisted before any side effect so a
/// failover resumes and re-evaluates identically.
/// </para>
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
    IOptions<LatticeSchemaEnforcementOptions> options,
    ILogger<LatticeSchemaRemediationGrain> logger,
    [PersistentState("schema-remediation", LatticeOptions.StorageProviderName)]
    IPersistentState<SchemaRemediationState> state,
    ILatticeSchemaPolicyStore? policyStore = null,
    ILatticeSchemaPolicyProvider? policyProvider = null,
    ILatticeSchemaRegistry? schemaRegistry = null)
    : IGrainBase, ILatticeSchemaRemediationGrain
{
    private readonly int _previewMaxBytes = Math.Max(1, options.Value.DeadLetterPreviewMaxBytes);

    IGrainContext IGrainBase.GrainContext => context;

    private string TreeId => context.GrainId.Key.ToString()!;

    private void EnsureControlPlaneOrigin() =>
        LatticeInternalOriginContext.EnsureInternalGrainOrigin(
            context.ActivationServices, TreeId, LatticeOperation.SchemaAdmin);

    private async Task ReserveAliasAsync()
    {
        if (!state.State.InProgress) await ReleaseAliasAsync();
        if (state.State.AliasReservationId is null)
        {
            state.State.AliasReservationId = $"remediation:{Guid.NewGuid():N}";
            try { await WriteAndPublishStateAsync(); }
            catch { state.State.AliasReservationId = null; throw; }
        }
        await grainFactory.GetGrain<ITreeDeletionGrain>(TreeId)
            .BeginAliasChangeAsync(state.State.AliasReservationId);
    }

    private async Task ReleaseAliasAsync()
    {
        if (state.State.AliasReservationId is not { } id) return;
        using var origin = LatticeAccessGateContext.EnterSystemOrigin();
        await grainFactory.GetGrain<ITreeDeletionGrain>(TreeId).EndAliasChangeAsync(id);
        state.State.AliasReservationId = null;
        try { await WriteAndPublishStateAsync(); }
        catch { state.State.AliasReservationId = id; throw; }
    }

    /// <summary>The number of values one <see cref="RunSliceAsync"/> processes before it persists its progress and returns.</summary>
    internal const int DefaultSliceSize = 512;

    /// <summary>
    /// The number of values one slice processes. Defaults to
    /// <see cref="DefaultSliceSize"/>; unit tests lower it to drive a tree across
    /// several slices.
    /// </summary>
    internal int SliceSize { get; set; } = DefaultSliceSize;

    /// <inheritdoc />
    public async Task<LatticeSchemaRemediationReport> StartAsync(
        LatticeValueTransform transform,
        LatticeSchemaPolicy targetPolicy,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(targetPolicy);
        EnsureControlPlaneOrigin();
        await AcceptCoreAsync(transform, targetPolicy, operationId: null);
        await DriveToTerminalAsync();
        return GetStatus();
    }

    /// <inheritdoc />
    public async Task<LatticeSchemaRemediationReport> StartVersionMigrationAsync(
        uint schemaId, uint targetVersion, CancellationToken cancellationToken = default)
    {
        EnsureControlPlaneOrigin();
        await AcceptVersionMigrationCoreAsync(schemaId, targetVersion, operationId: null);
        await DriveToTerminalAsync();
        return GetStatus();
    }

    /// <inheritdoc />
    public Task<LatticeSchemaRemediationReport> AcceptAsync(
        LatticeValueTransform transform, LatticeSchemaPolicy targetPolicy, string operationId)
    {
        ArgumentNullException.ThrowIfNull(targetPolicy);
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        EnsureControlPlaneOrigin();
        return AcceptCoreAsync(transform, targetPolicy, operationId);
    }

    /// <inheritdoc />
    public Task<LatticeSchemaRemediationReport> AcceptVersionMigrationAsync(
        uint schemaId, uint targetVersion, string operationId)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        EnsureControlPlaneOrigin();
        return AcceptVersionMigrationCoreAsync(schemaId, targetVersion, operationId);
    }

    /// <inheritdoc />
    public async Task<SchemaRemediationSlice> RunSliceAsync()
    {
        EnsureControlPlaneOrigin();
        await RunSliceCoreAsync();
        return new SchemaRemediationSlice(GetStatus(), state.State.InProgress ? state.State.PhaseTotal : null);
    }

    /// <inheritdoc />
    public async Task RunRemediationPassAsync()
    {
        EnsureControlPlaneOrigin();
        if (!state.State.InProgress)
        {
            await ReleaseAliasAsync();
            return;
        }

        await DriveToTerminalAsync();
    }

    /// <inheritdoc />
    public async Task<LatticeSchemaRemediationReport> CancelAsync(string operationId)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        EnsureControlPlaneOrigin();

        // Cutover is the point of no return: the target policy may already be
        // installed and the alias swapped, so a cancel there is declined and the
        // remediation runs on to completion.
        if (!state.State.InProgress
            || !string.Equals(state.State.OperationId, operationId, StringComparison.Ordinal)
            || state.State.Phase is not (LatticeSchemaRemediationPhase.DryRun or LatticeSchemaRemediationPhase.Build))
        {
            return GetStatus();
        }

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            try
            {
                if (state.State.Phase == LatticeSchemaRemediationPhase.Build)
                {
                    await DiscardDestinationAsync(grainFactory.GetGrain<ILattice>(state.State.DestinationTreeId!));
                }

                await FinishCancelledAsync();
            }
            finally
            {
                if (!state.State.InProgress)
                    await ReleaseAliasAsync();
            }
        }

        return GetStatus();
    }

    private async Task<LatticeSchemaRemediationReport> AcceptCoreAsync(
        LatticeValueTransform transform, LatticeSchemaPolicy targetPolicy, string? operationId)
    {
        // Reject an uncompilable / non-linear regex here rather than mid-build.
        _ = CompiledSchemaPolicy.Compile(targetPolicy);

        if (state.State.InProgress)
        {
            if (IsSameParameters(transform, targetPolicy))
            {
                // Idempotent: the in-flight remediation is the one requested.
                return GetStatus();
            }

            throw new InvalidOperationException(
                $"A schema remediation is already in progress for tree '{TreeId}' with different parameters.");
        }

        await InitiateCoreAsync(
            SchemaRemediationMode.Transform, transform, targetPolicy, migrationSchemaId: 0, migrationTargetVersion: 0,
            operationId);
        return GetStatus();
    }

    private async Task<LatticeSchemaRemediationReport> AcceptVersionMigrationCoreAsync(
        uint schemaId, uint targetVersion, string? operationId)
    {
        if (schemaRegistry is null)
        {
            throw new InvalidOperationException(
                $"Schema versioning is not registered on this silo; a schema-version migration of tree '{TreeId}' " +
                "cannot run. Call AddLatticeSchemaVersioning(...) on the silo.");
        }

        if (state.State.InProgress)
        {
            if (IsSameMigration(schemaId, targetVersion))
            {
                // Idempotent: the in-flight migration is the one requested.
                return GetStatus();
            }

            throw new InvalidOperationException(
                $"A schema remediation is already in progress for tree '{TreeId}' with different parameters.");
        }

        // Already fully migrated to this exact (schema, version): a genuine no-op
        // success, so a repeat / retry does not rebuild an identical destination.
        if (state.State.Mode == SchemaRemediationMode.SchemaVersionMigration
            && state.State.LastReport is { Succeeded: true }
            && state.State.MigrationSchemaId == schemaId
            && state.State.LastCompletedMigrationVersion == targetVersion)
        {
            return GetStatus();
        }

        await InitiateMigrationAsync(schemaId, targetVersion, operationId);
        return GetStatus();
    }

    /// <summary>
    /// Runs slices until the remediation is terminal, all within the current turn.
    /// Serves the in-turn <see cref="StartAsync"/>, <see cref="StartVersionMigrationAsync"/>
    /// and <see cref="RunRemediationPassAsync"/> verbs; a caller that must not hold
    /// one call open for the whole run drives <see cref="RunSliceAsync"/> instead.
    /// </summary>
    private async Task DriveToTerminalAsync()
    {
        while (state.State.InProgress)
        {
            await RunSliceCoreAsync();
        }
    }

    private async Task RunSliceCoreAsync()
    {
        if (!state.State.InProgress)
        {
            await ReleaseAliasAsync();
            return;
        }

        // Remediation is enforcement infrastructure: its reads and writes to the
        // source, destination, and reserved policy tree run under a system-origin
        // scope so the access gate never blocks them and the write interceptor is
        // not re-entered while the destination is populated.
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await ReserveAliasAsync();
            try
            {
                switch (state.State.Phase)
                {
                    case LatticeSchemaRemediationPhase.DryRun:
                        await RunDryRunSliceAsync();
                        break;
                    case LatticeSchemaRemediationPhase.Build:
                        await RunBuildSliceAsync();
                        break;
                    case LatticeSchemaRemediationPhase.Cutover:
                        await CutoverAsync();
                        await CompleteAsync();
                        break;
                    default:
                        throw new InvalidOperationException(
                            $"The schema remediation of tree '{TreeId}' is in flight in the unexpected phase {state.State.Phase}.");
                }
            }
            finally
            {
                if (!state.State.InProgress)
                    await ReleaseAliasAsync();
            }
        }
    }
    /// <inheritdoc />
    /// <remarks>
    /// Interleaved (<see cref="Orleans.Concurrency.AlwaysInterleaveAttribute"/> on the
    /// interface), so it may run while a start turn is suspended at any await. It
    /// therefore answers from <see cref="PublishedStatus"/>, never from the live
    /// state a running phase is mutating.
    /// </remarks>
    public Task<LatticeSchemaRemediationReport> GetStatusAsync() => Task.FromResult(PublishedStatus);

    // What the interleaved GetStatusAsync answers from (issue #4123). Every phase
    // transition mutates the in-memory state first, then awaits WriteStateAsync,
    // and reverts the mutation if that write fails - so a read of the live state
    // could land inside that window and report a transition that is not yet
    // durable, or that is then rolled back. This immutable report is captured
    // from the state being written and replaced in a single assignment only once
    // the write has succeeded; it is also captured at activation, after the
    // persisted state has been read. All of it runs on the activation scheduler,
    // so a reader sees one whole published report, never a mix of two. Phase
    // turns, and the report a start call returns, keep reading the live state.
    private LatticeSchemaRemediationReport? _publishedStatus;

    private LatticeSchemaRemediationReport PublishedStatus => _publishedStatus ??= GetStatus();

    /// <inheritdoc />
    Task IGrainBase.OnActivateAsync(CancellationToken token)
    {
        _publishedStatus = GetStatus();
        return Task.CompletedTask;
    }

    /// <summary>
    /// Persists <see cref="SchemaRemediationState"/> and, only once the write has
    /// succeeded, publishes the status it describes to the interleaved
    /// <see cref="GetStatusAsync"/>.
    /// </summary>
    private async Task WriteAndPublishStateAsync()
    {
        var written = GetStatus();
        await state.WriteStateAsync();
        _publishedStatus = written;
    }

    private LatticeSchemaRemediationReport GetStatus()
    {
        if (state.State.InProgress)
        {
            return LatticeSchemaRemediationReport.InFlight(
                state.State.Phase, state.State.ScannedCount, state.State.DestinationTreeId, state.State.OperationId);
        }

        return state.State.LastReport ?? LatticeSchemaRemediationReport.Idle;
    }

    /// <summary>
    /// Reads the tree's current enforcement policy (when it has one) and starts a
    /// schema-version migration against it. The policy, if present, is validated
    /// post-upcast during the build but is <b>not</b> reinstalled at cutover (it is
    /// keyed by the logical tree id, so the alias flip leaves it governing the tree
    /// unchanged - tightening a policy is the separate enforcement-remediation path).
    /// </summary>
    private async Task InitiateMigrationAsync(uint schemaId, uint targetVersion, string? operationId)
    {
        LatticeSchemaPolicy? existingPolicy;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            existingPolicy = policyStore is null
                ? null
                : await policyStore.GetPolicyAsync(TreeId);
        }

        // Reject an uncompilable existing policy up front rather than mid-build. A
        // policy that reached the store is already validated, so this is defensive.
        if (existingPolicy is { } policy)
        {
            _ = CompiledSchemaPolicy.Compile(policy);
        }

        await InitiateCoreAsync(
            SchemaRemediationMode.SchemaVersionMigration, transform: default, existingPolicy, schemaId, targetVersion,
            operationId);
    }

    /// <summary>
    /// Persists the intent to run a shadow build (in either mode) before any
    /// external side effect, snapshotting every field this method writes so a
    /// transient <c>WriteStateAsync</c> failure cannot leak in-memory mutations past
    /// the <see cref="SchemaRemediationState.InProgress"/> guard.
    /// </summary>
    /// <remarks>
    /// The destination tree id always takes a fresh suffix, never a caller-supplied
    /// <paramref name="operationId"/>: an operation id can be chosen by a caller and
    /// reused once its record is pruned, and a reused id must never name the
    /// physical tree an earlier remediation cut the tree over to.
    /// </remarks>
    private async Task InitiateCoreAsync(
        SchemaRemediationMode mode,
        LatticeValueTransform transform,
        LatticeSchemaPolicy? targetPolicy,
        uint migrationSchemaId,
        uint migrationTargetVersion,
        string? operationId)
    {
        var destinationSuffix = Guid.NewGuid().ToString("N");
        operationId ??= destinationSuffix;
        var destinationTreeId = $"{TreeId}/remediated/{destinationSuffix}";

        // Resolve the source tree's physical id BEFORE any alias swap, so cutover
        // can arm the correct (source) shards even on a resume after a partial
        // cutover. Registry reads run under a system-origin scope.
        string sourcePhysical;
        var registry = grainFactory.GetLatticeRegistry();
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await ReserveAliasAsync();
            sourcePhysical = await registry.ResolveAsync(TreeId);
        }

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
        var prevSourcePhysical = state.State.SourcePhysicalTreeId;
        var prevMode = state.State.Mode;
        var prevMigrationSchemaId = state.State.MigrationSchemaId;
        var prevMigrationTargetVersion = state.State.MigrationTargetVersion;
        var prevScanCursor = state.State.ScanCursor;
        var prevPhaseTotal = state.State.PhaseTotal;

        // Persist intent BEFORE any external side effect.
        state.State.InProgress = true;
        state.State.Phase = LatticeSchemaRemediationPhase.DryRun;
        state.State.OperationId = operationId;
        state.State.DestinationTreeId = destinationTreeId;
        state.State.Transform = transform;
        state.State.TargetPolicy = targetPolicy;
        state.State.LastReport = null;
        state.State.ScannedCount = 0;
        state.State.SourcePhysicalTreeId = sourcePhysical;
        state.State.Mode = mode;
        state.State.MigrationSchemaId = migrationSchemaId;
        state.State.MigrationTargetVersion = migrationTargetVersion;
        state.State.ScanCursor = null;
        state.State.PhaseTotal = null;
        try
        {
            await WriteAndPublishStateAsync();
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
            state.State.SourcePhysicalTreeId = prevSourcePhysical;
            state.State.Mode = prevMode;
            state.State.MigrationSchemaId = prevMigrationSchemaId;
            state.State.MigrationTargetVersion = prevMigrationTargetVersion;
            state.State.ScanCursor = prevScanCursor;
            state.State.PhaseTotal = prevPhaseTotal;
            throw;
        }
    }

    /// <summary>
    /// Runs one slice of the read-only dry-run gate: up to <see cref="SliceSize"/>
    /// values after the durable cursor, each rewritten and revalidated. Aborts on the
    /// first offender with no destination built and the original tree untouched;
    /// advances to the build once the source is exhausted, recording the dry run's
    /// count as the build's total; otherwise records how far it got.
    /// </summary>
    private async Task RunDryRunSliceAsync()
    {
        var source = grainFactory.GetGrain<ILattice>(TreeId);
        var rewrite = CreateRewrite();
        var policyView = PolicyViewOrNull();
        var compiled = CompiledPolicyOrNull();
        var sliceSize = Math.Max(1, SliceSize);
        var done = 0;
        string? lastKey = null;
        Offender? offender = null;

        var slice = await ReadSliceAsync(source, sliceSize);
        try
        {
            foreach (var entry in slice.Entries)
            {
                byte[] rewritten;
                try
                {
                    rewritten = rewrite(entry.Value);
                }
                catch (Exception ex) when (ex is InvalidOperationException or NotSupportedException)
                {
                    offender = new Offender(entry.Key, ex.Message, Preview(entry.Value));
                    break;
                }

                var validated = policyView is null ? rewritten : policyView(rewritten);
                if (compiled?.Validate(validated) is { } reason)
                {
                    offender = new Offender(entry.Key, reason, Preview(validated));
                    break;
                }

                done++;
                lastKey = entry.Key;
            }
        }
        catch
        {
            await BankSliceProgressAsync(done, lastKey);
            throw;
        }

        var scanned = state.State.ScannedCount + done;
        if (offender is { } failed)
        {
            await AbortAsync(scanned + 1, failed.Key, failed.Reason, failed.Preview);
        }
        else if (slice.Fault is { } fault)
        {
            await BankSliceProgressAsync(done, lastKey);
            fault.Throw();
        }
        else if (slice.Exhausted)
        {
            await AdvancePhaseAsync(LatticeSchemaRemediationPhase.Build, scannedCount: 0, phaseTotal: scanned);
        }
        else
        {
            await RecordSliceProgressAsync(scanned, slice.LastScannedKey);
        }
    }

    /// <summary>
    /// Runs one slice of the destination build: up to <see cref="SliceSize"/> values
    /// after the durable cursor, each rewritten, revalidated and written to the
    /// destination. Aborts on the first offender, discarding the partial destination;
    /// advances to cutover once the source is exhausted; otherwise records how far it
    /// got. A slice re-run after a fault rewrites at most the values the fault
    /// interrupted, and a destination write is idempotent.
    /// </summary>
    private async Task RunBuildSliceAsync()
    {
        if (state.State.ScanCursor is null)
        {
            // The destination inherits the logical tree's routing map, split
            // allocation mark, structural pins and runtime overrides, so the build
            // lays keys out exactly as the source does and the cutover, which
            // carries the destination's map onto the logical tree, hands the tree
            // back with the topology and sizing it had. Registered with defaults,
            // it silently reset a resharded tree to the default shard count.
            var inherited = await DerivedTreeEntries.InheritingAsync(grainFactory, TreeId, derivedFrom: TreeId);
            await grainFactory.GetLatticeRegistry().RegisterAsync(state.State.DestinationTreeId!, inherited);
        }

        var source = grainFactory.GetGrain<ILattice>(TreeId);
        var destination = grainFactory.GetGrain<ILattice>(state.State.DestinationTreeId!);
        var compiled = CompiledPolicyOrNull();
        var policyView = PolicyViewOrNull();
        var rewrite = CreateRewrite();
        var sliceSize = Math.Max(1, SliceSize);
        var done = 0;
        string? lastKey = null;
        Offender? offender = null;

        var slice = await ReadSliceAsync(source, sliceSize);
        try
        {
            foreach (var entry in slice.Entries)
            {
                byte[] rewritten;
                try
                {
                    rewritten = rewrite(entry.Value);
                }
                catch (Exception ex) when (ex is InvalidOperationException or NotSupportedException)
                {
                    offender = new Offender(entry.Key, ex.Message, Preview(entry.Value));
                    break;
                }

                var validated = policyView is null ? rewritten : policyView(rewritten);
                if (compiled?.Validate(validated) is { } reason)
                {
                    offender = new Offender(entry.Key, reason, Preview(validated));
                    break;
                }

                await destination.SetAsync(entry.Key, rewritten);
                done++;
                lastKey = entry.Key;
            }
        }
        catch
        {
            await BankSliceProgressAsync(done, lastKey);
            throw;
        }

        var scanned = state.State.ScannedCount + done;
        if (offender is { } failed)
        {
            await DiscardDestinationAsync(destination);
            await AbortAsync(scanned + 1, failed.Key, failed.Reason, failed.Preview);
        }
        else if (slice.Fault is { } fault)
        {
            await BankSliceProgressAsync(done, lastKey);
            fault.Throw();
        }
        else if (slice.Exhausted)
        {
            await AdvancePhaseAsync(LatticeSchemaRemediationPhase.Cutover, scanned, phaseTotal: null);
        }
        else
        {
            await RecordSliceProgressAsync(scanned, slice.LastScannedKey);
        }
    }

    /// <summary>
    /// Reads the next slice of the source after the durable cursor: up to
    /// <paramref name="sliceSize"/> keys in key order from a scan, each with the
    /// value a point read returns for it (issue #4361).
    /// <para>
    /// The scan supplies the order and the resumable position only. Its values are
    /// not used: a full scan merges every shard's rows, and a shard can hold a stale
    /// copy of a key whose slot it does not own (an atomic write's cross-migration
    /// backstop writes the whole batch into every split shard it may reach), so a
    /// scan served by a routing activation holding an older map could return that
    /// copy. Copying it would put a value into the remediated tree that no reader of
    /// the original ever saw. A point read is routed to the key's owner, so the copy
    /// holds exactly what the original served. A key a point read finds absent is
    /// skipped for the same reason.
    /// </para>
    /// </summary>
    private async Task<SourceSlice> ReadSliceAsync(ILattice source, int sliceSize)
    {
        var keys = new List<string>(Math.Min(sliceSize, 1024));
        ExceptionDispatchInfo? fault = null;
        try
        {
            await foreach (var entry in source.ScanEntriesAsync(startInclusive: After(state.State.ScanCursor)))
            {
                keys.Add(entry.Key);
                if (keys.Count == sliceSize)
                {
                    break;
                }
            }
        }
        catch (Exception ex) when (keys.Count > 0)
        {
            // The keys read before the fault are still processed, so the slice can
            // bank them (issue #2545) before it rethrows the fault.
            fault = ExceptionDispatchInfo.Capture(ex);
        }

        var exhausted = fault is null && keys.Count < sliceSize;
        if (keys.Count == 0)
        {
            return new SourceSlice([], LastScannedKey: null, exhausted, Fault: null);
        }

        var values = await source.GetManyAsync(keys);
        var entries = new List<KeyValuePair<string, byte[]>>(values.Count);
        foreach (var key in keys)
        {
            if (values.TryGetValue(key, out var value) && value is not null)
            {
                entries.Add(new KeyValuePair<string, byte[]>(key, value));
            }
        }

        return new SourceSlice(entries, keys[^1], exhausted, fault);
    }

    /// <summary>
    /// One slice of the source: its entries, with point-read values; the last key
    /// the scan reached, which the durable cursor resumes after even when that key
    /// read absent; whether the scan reached the end of the source; and the fault
    /// that cut the scan short after some keys were read, rethrown once the slice
    /// has banked them.
    /// </summary>
    private sealed record SourceSlice(
        List<KeyValuePair<string, byte[]>> Entries,
        string? LastScannedKey,
        bool Exhausted,
        ExceptionDispatchInfo? Fault);
    /// <summary>
    /// The smallest key strictly after <paramref name="cursor"/> in ordinal order, or
    /// <c>null</c> to start at the beginning: appending U+0000 yields the immediate
    /// successor, so a resumed scan neither repeats nor skips a key.
    /// </summary>
    private static string? After(string? cursor) => cursor is null ? null : cursor + '\0';

    /// <summary>The first value a slice found it could not remediate.</summary>
    private readonly record struct Offender(string Key, string Reason, byte[] Preview);

    /// <summary>Persists a slice's progress within the current phase.</summary>
    private async Task RecordSliceProgressAsync(int scannedCount, string? lastKey)
    {
        var prevScannedCount = state.State.ScannedCount;
        var prevScanCursor = state.State.ScanCursor;

        state.State.ScannedCount = scannedCount;
        state.State.ScanCursor = lastKey;
        try
        {
            await WriteAndPublishStateAsync();
        }
        catch
        {
            state.State.ScannedCount = prevScannedCount;
            state.State.ScanCursor = prevScanCursor;
            throw;
        }
    }

    /// <summary>
    /// Banks the values a faulted slice had already processed (issue #2545's
    /// coalesced-durable-state rule), so the next slice resumes after them instead
    /// of repeating them. Called only from a slice's fault path, which rethrows the
    /// slice's own fault once this returns; a failure to bank is therefore logged
    /// and swallowed here rather than allowed to mask that fault.
    /// </summary>
    private async Task BankSliceProgressAsync(int done, string? lastKey)
    {
        if (done == 0 || lastKey is null)
        {
            return;
        }

        try
        {
            await RecordSliceProgressAsync(state.State.ScannedCount + done, lastKey);
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex, "Schema remediation for tree '{TreeId}' could not bank the progress of a faulted slice.", TreeId);
        }
    }
    /// <summary>
    /// Snapshots the current build mode and its parameters from durable state (on the
    /// activation thread) into a <b>pure</b> per-value rewrite delegate that is safe to
    /// invoke off the activation scheduler - which the shared dry-run loop does, since
    /// it enumerates the source with <c>ConfigureAwait(false)</c>. The delegate closes
    /// over local snapshots and the thread-safe singleton registry only, never
    /// <c>state</c>, so it never touches activation services.
    /// <see cref="SchemaRemediationMode.Transform"/> evaluates the static transform;
    /// <see cref="SchemaRemediationMode.SchemaVersionMigration"/> re-stamps the value to
    /// the target schema version through the registry. The delegate throws
    /// <see cref="InvalidOperationException"/> or <see cref="NotSupportedException"/> on
    /// a per-value failure, which the dry-run / build turns into an abort.
    /// </summary>
    private Func<byte[], byte[]> CreateRewrite()
    {
        if (state.State.Mode == SchemaRemediationMode.SchemaVersionMigration)
        {
            var registry = schemaRegistry
                ?? throw new InvalidOperationException(
                    $"Schema versioning is not registered on this silo; cannot resume the version migration of tree '{TreeId}'.");
            var schemaId = state.State.MigrationSchemaId;
            var targetVersion = state.State.MigrationTargetVersion;
            return value => LatticeSchemaVersionMigration.Migrate(value, schemaId, targetVersion, registry);
        }

        var transform = state.State.Transform;
        return value => LatticeValueTransformEvaluation.Evaluate(value, in transform);
    }

    /// <summary>
    /// Compiles the target policy for build-time revalidation, or returns <c>null</c>
    /// when the tree has no policy (a pure version migration of an unenforced tree).
    /// </summary>
    private CompiledSchemaPolicy? CompiledPolicyOrNull() =>
        state.State.TargetPolicy is { } policy ? CompiledSchemaPolicy.Compile(policy) : null;

    /// <summary>
    /// Projects a rewritten value to the shape the policy validates. In
    /// <see cref="SchemaRemediationMode.SchemaVersionMigration"/> the stored value is
    /// enveloped, but the enforcement policy must see the plain upcast body (a JSON
    /// rule cannot parse the binary envelope header), so this strips the envelope.
    /// Returns <c>null</c> in <see cref="SchemaRemediationMode.Transform"/>, where the
    /// rewritten value is already the plain value the policy validates directly.
    /// </summary>
    private Func<byte[], byte[]>? PolicyViewOrNull() =>
        state.State.Mode == SchemaRemediationMode.SchemaVersionMigration ? StripEnvelopeForPolicy : null;

    /// <summary>
    /// Strips the schema envelope so the enforcement policy validates the plain upcast
    /// body. A value that is not enveloped (a legacy value migrated in place) is
    /// validated as-is.
    /// </summary>
    private static byte[] StripEnvelopeForPolicy(byte[] rewritten) =>
        LatticeSchemaEnvelope.IsEnveloped(rewritten) ? LatticeSchemaEnvelope.StripToBody(rewritten) : rewritten;

    /// <summary>
    /// Installs the target policy, repoints the logical tree to the destination,
    /// and arms the source tree's shards to redirect stale logical-alias-routed
    /// traffic onto the destination (so an already-active routing activation
    /// self-heals instead of serving the pre-remediation snapshot). Mirrors the
    /// backup shadow-cutover commit. Idempotent: re-running after a mid-cutover
    /// restart repeats the same policy write, alias swap, and shard redirects
    /// (idempotent per operation id). If <c>ITreeOwnershipGuard</c> refuses
    /// the alias swap, the remediation remains in Cutover with the target policy
    /// already installed and the alias reservation still held for retry or
    /// operator repair.
    /// </summary>
    private async Task CutoverAsync()
    {
        var registry = grainFactory.GetLatticeRegistry();
        var destinationTreeId = state.State.DestinationTreeId!;
        var operationId = state.State.OperationId!;
        var sourcePhysical = state.State.SourcePhysicalTreeId!;

        // The destination is a fresh, never-aliased tree, so its physical id equals
        // its logical id.
        var destinationPhysical = destinationTreeId;

        // Arm enforcement BEFORE the alias swap so there is no window in which the
        // remediated destination is live (logical-alias-routed) yet unenforced. The
        // destination already satisfies the target policy (the shadow build wrote
        // every value through the transform), so installing the policy first only
        // guards the source's logical id for the brief instant before the swap - it
        // can never reject an existing destination value. Evict the local policy
        // cache eagerly (as the admin does) so the coordinating silo enforces the new
        // policy on its next write without waiting for the change feed to propagate.
        //
        // Only the enforcement-transform mode installs a policy. A pure version
        // migration validates each value against the tree's EXISTING policy during
        // the build but does not change it: the policy is keyed by the logical tree
        // id, so the alias flip leaves it governing the re-stamped destination
        // unchanged. (Tightening a policy is the separate enforcement path.)
        if (state.State.Mode == SchemaRemediationMode.Transform)
        {
            await policyStore!.SetPolicyAsync(TreeId, state.State.TargetPolicy!);
            policyProvider!.Invalidate(TreeId);
        }

        // Carry the destination's map onto the logical entry before the swap (#4250).
        // Routing reads the map under the logical id, but the destination was written
        // by routing under its own, so swapping only the alias would read most of the
        // remediated values as absent. The map the logical tree addressed the source
        // by is recorded on the destination and returned: it is the authoritative
        // description of the source's shards, because when the tree was already
        // aliased its splits and reshards wrote the map under the logical id, never
        // the source's own. A cutover resumed after the swap gets the recorded map
        // back rather than following the new alias to the destination's.
        var replacedPhysical = await registry.ResolveAsync(TreeId);
        var replacedMap = await AliasCutoverShardMaps.PrepareCutoverAsync(grainFactory, TreeId, destinationPhysical);
        RoutingInfo? retainedRouting = null;
        var describesSource = replacedMap is not null
            && (string.Equals(replacedPhysical, sourcePhysical, StringComparison.Ordinal)
                || string.Equals(replacedPhysical, destinationPhysical, StringComparison.Ordinal));
        if (!describesSource)
        {
            // Forced (#4206), and pinned to the source so a resume does not follow
            // the installed alias to the destination.
            var resolvedSource = await grainFactory.GetGrain<ILattice>(sourcePhysical).GetRoutingAsync(forceRefresh: true);
            retainedRouting = resolvedSource with { PhysicalTreeId = sourcePhysical };
        }

        // Repoint the logical tree to the remediated destination, moving the
        // destination's map onto the logical entry in the same registry write
        // (#4336), so no reader pairs one copy with the other's map. The map the
        // swap replaced is the source's final layout.
        var finalReplacedMap = await AliasCutoverShardMaps.SwapCutoverAsync(grainFactory, TreeId, destinationPhysical);
        retainedRouting ??= new RoutingInfo(sourcePhysical, finalReplacedMap ?? replacedMap!);

        // Arm every source shard to redirect logical-alias-routed traffic onto the
        // destination. Without this, a stale stateless-worker routing activation
        // that still caches the pre-cutover alias would keep serving the old
        // (un-remediated) values forever - it never sees a staleness signal to
        // re-resolve. The redirect fires only for logical-alias traffic; direct-
        // physical access and internal maintenance keep reading the old snapshot.
        // Skipped in the degenerate case where the alias already resolved across.
        if (!string.Equals(destinationPhysical, retainedRouting.PhysicalTreeId, StringComparison.Ordinal))
        {
            foreach (var shardIndex in retainedRouting.Map.GetPhysicalShardIndices())
            {
                await grainFactory.GetGrain<IShardRootGrain>($"{retainedRouting.PhysicalTreeId}/{shardIndex}")
                    .MarkRetainedRedirectAsync(destinationPhysical, operationId, TreeId);
            }
        }

        // Proactively invalidate this activation's cached alias / routing so a
        // caller observes the cutover without waiting for a reactivation.
        await grainFactory.GetGrain<ILattice>(TreeId).GetRoutingAsync(forceRefresh: true);
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

    /// <summary>
    /// Moves to <paramref name="phase"/>, which starts from the beginning of the
    /// source: the scan cursor is cleared and <paramref name="phaseTotal"/> becomes
    /// the new phase's total.
    /// </summary>
    private async Task AdvancePhaseAsync(LatticeSchemaRemediationPhase phase, int scannedCount, int? phaseTotal)
    {
        var prevPhase = state.State.Phase;
        var prevScannedCount = state.State.ScannedCount;
        var prevScanCursor = state.State.ScanCursor;
        var prevPhaseTotal = state.State.PhaseTotal;

        state.State.Phase = phase;
        state.State.ScannedCount = scannedCount;
        state.State.ScanCursor = null;
        state.State.PhaseTotal = phaseTotal;
        try
        {
            await WriteAndPublishStateAsync();
        }
        catch
        {
            state.State.Phase = prevPhase;
            state.State.ScannedCount = prevScannedCount;
            state.State.ScanCursor = prevScanCursor;
            state.State.PhaseTotal = prevPhaseTotal;
            throw;
        }
    }

    private async Task FinishCancelledAsync()
    {
        var prevInProgress = state.State.InProgress;
        var prevPhase = state.State.Phase;
        var prevLastReport = state.State.LastReport;

        state.State.InProgress = false;
        state.State.Phase = LatticeSchemaRemediationPhase.Cancelled;
        state.State.LastReport = LatticeSchemaRemediationReport.Cancelled(
            state.State.ScannedCount, state.State.OperationId!);
        try
        {
            await WriteAndPublishStateAsync();
        }
        catch
        {
            state.State.InProgress = prevInProgress;
            state.State.Phase = prevPhase;
            state.State.LastReport = prevLastReport;
            throw;
        }
    }

    private async Task CompleteAsync()
    {
        var prevInProgress = state.State.InProgress;
        var prevPhase = state.State.Phase;
        var prevLastReport = state.State.LastReport;
        var prevLastCompletedMigrationVersion = state.State.LastCompletedMigrationVersion;

        state.State.InProgress = false;
        state.State.Phase = LatticeSchemaRemediationPhase.Completed;
        state.State.LastReport = LatticeSchemaRemediationReport.Completed(
            state.State.ScannedCount, state.State.DestinationTreeId!, state.State.OperationId!);

        // Record the version a successful migration re-stamped the tree to, so a
        // repeat MigrateToTargetVersionAsync to the same target short-circuits.
        if (state.State.Mode == SchemaRemediationMode.SchemaVersionMigration)
        {
            state.State.LastCompletedMigrationVersion = state.State.MigrationTargetVersion;
        }

        try
        {
            await WriteAndPublishStateAsync();
        }
        catch
        {
            state.State.InProgress = prevInProgress;
            state.State.Phase = prevPhase;
            state.State.LastReport = prevLastReport;
            state.State.LastCompletedMigrationVersion = prevLastCompletedMigrationVersion;
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
            await WriteAndPublishStateAsync();
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
        state.State.Mode == SchemaRemediationMode.Transform
            && TransformEquivalent(state.State.Transform, transform)
            && PolicyEquivalent(state.State.TargetPolicy, targetPolicy);

    private bool IsSameMigration(uint schemaId, uint targetVersion) =>
        state.State.Mode == SchemaRemediationMode.SchemaVersionMigration
            && state.State.MigrationSchemaId == schemaId
            && state.State.MigrationTargetVersion == targetVersion;

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

    // Structural equality for the transform IR. The default record-struct Equals
    // compares the Children (and Condition's Children) arrays by reference, so two
    // structurally-identical transforms built from different array instances - as
    // happens on every grain call, because Orleans deserializes the argument into a
    // fresh graph - compare unequal. That would break the idempotent same-parameter
    // resume contract for any non-trivial transform. Compare the tree by value
    // instead, normalising a null child list to an empty one.
    private static bool TransformEquivalent(LatticeValueTransform a, LatticeValueTransform b)
    {
        if (a.Kind != b.Kind
            || !string.Equals(a.MemberPath, b.MemberPath, StringComparison.Ordinal)
            || !string.Equals(a.ToPath, b.ToPath, StringComparison.Ordinal)
            || !a.Constant.Equals(b.Constant)
            || a.ComputeOperator != b.ComputeOperator
            || !PredicateEquivalent(a.Condition, b.Condition))
        {
            return false;
        }

        var ac = a.Children;
        var bc = b.Children;
        var count = ac?.Length ?? 0;
        if (count != (bc?.Length ?? 0))
        {
            return false;
        }

        for (var i = 0; i < count; i++)
        {
            if (!TransformEquivalent(ac![i], bc![i]))
            {
                return false;
            }
        }

        return true;
    }

    // Structural equality for the embedded boolean predicate IR, with the same
    // array-by-reference caveat as the transform IR.
    private static bool PredicateEquivalent(LatticePredicateNode a, LatticePredicateNode b)
    {
        if (a.Kind != b.Kind
            || !string.Equals(a.MemberPath, b.MemberPath, StringComparison.Ordinal)
            || !a.Constant.Equals(b.Constant)
            || a.ComparisonOperator != b.ComparisonOperator
            || a.BooleanOperator != b.BooleanOperator
            || a.StringMethod != b.StringMethod)
        {
            return false;
        }

        var ac = a.Children;
        var bc = b.Children;
        var count = ac?.Length ?? 0;
        if (count != (bc?.Length ?? 0))
        {
            return false;
        }

        for (var i = 0; i < count; i++)
        {
            if (!PredicateEquivalent(ac![i], bc![i]))
            {
                return false;
            }
        }

        return true;
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
