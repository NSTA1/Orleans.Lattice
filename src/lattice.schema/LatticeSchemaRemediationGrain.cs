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
    public async Task<LatticeSchemaRemediationReport> StartVersionMigrationAsync(
        uint schemaId, uint targetVersion, CancellationToken cancellationToken = default)
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
                // Idempotent resume: drive the in-flight migration to completion.
                await RunRemediationPassAsync();
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

        await InitiateMigrationAsync(schemaId, targetVersion);
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

    private Task InitiateAsync(LatticeValueTransform transform, LatticeSchemaPolicy targetPolicy) =>
        InitiateCoreAsync(SchemaRemediationMode.Transform, transform, targetPolicy, migrationSchemaId: 0, migrationTargetVersion: 0);

    /// <summary>
    /// Reads the tree's current enforcement policy (when it has one) and starts a
    /// schema-version migration against it. The policy, if present, is validated
    /// post-upcast during the build but is <b>not</b> reinstalled at cutover (it is
    /// keyed by the logical tree id, so the alias flip leaves it governing the tree
    /// unchanged - tightening a policy is the separate enforcement-remediation path).
    /// </summary>
    private async Task InitiateMigrationAsync(uint schemaId, uint targetVersion)
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
            SchemaRemediationMode.SchemaVersionMigration, transform: default, existingPolicy, schemaId, targetVersion);
    }

    /// <summary>
    /// Persists the intent to run a shadow build (in either mode) before any
    /// external side effect, snapshotting every field this method writes so a
    /// transient <c>WriteStateAsync</c> failure cannot leak in-memory mutations past
    /// the <see cref="SchemaRemediationState.InProgress"/> guard.
    /// </summary>
    private async Task InitiateCoreAsync(
        SchemaRemediationMode mode,
        LatticeValueTransform transform,
        LatticeSchemaPolicy? targetPolicy,
        uint migrationSchemaId,
        uint migrationTargetVersion)
    {
        var operationId = Guid.NewGuid().ToString("N");
        var destinationTreeId = $"{TreeId}/remediated/{operationId}";

        // Resolve the source tree's physical id BEFORE any alias swap, so cutover
        // can arm the correct (source) shards even on a resume after a partial
        // cutover. Registry reads run under a system-origin scope.
        string sourcePhysical;
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
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
            state.State.SourcePhysicalTreeId = prevSourcePhysical;
            state.State.Mode = prevMode;
            state.State.MigrationSchemaId = prevMigrationSchemaId;
            state.State.MigrationTargetVersion = prevMigrationTargetVersion;
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
        var outcome = await LatticeSchemaRemediation.DryRunCoreAsync(
            source.EntriesAsync(),
            CreateRewrite(),
            PolicyViewOrNull(),
            CompiledPolicyOrNull(),
            _previewMaxBytes,
            CancellationToken.None);

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
    /// Populates the destination physical tree with rewritten, revalidated values.
    /// Returns <c>true</c> to advance to cutover, <c>false</c> when the remediation
    /// aborted (the partial destination is discarded).
    /// </summary>
    private async Task<bool> BuildDestinationAsync()
    {
        var source = grainFactory.GetGrain<ILattice>(TreeId);
        var destination = grainFactory.GetGrain<ILattice>(state.State.DestinationTreeId!);
        var compiled = CompiledPolicyOrNull();
        var policyView = PolicyViewOrNull();
        var rewrite = CreateRewrite();
        var scanned = 0;

        await foreach (var entry in source.EntriesAsync())
        {
            scanned++;

            byte[] rewritten;
            try
            {
                rewritten = rewrite(entry.Value);
            }
            catch (Exception ex) when (ex is InvalidOperationException or NotSupportedException)
            {
                await DiscardDestinationAsync(destination);
                await AbortAsync(scanned, entry.Key, ex.Message, Preview(entry.Value));
                return false;
            }

            var validated = policyView is null ? rewritten : policyView(rewritten);
            var reason = compiled?.Validate(validated);
            if (reason is not null)
            {
                await DiscardDestinationAsync(destination);
                await AbortAsync(scanned, entry.Key, reason, Preview(validated));
                return false;
            }

            await destination.SetAsync(entry.Key, rewritten);
        }

        await AdvancePhaseAsync(LatticeSchemaRemediationPhase.Cutover, scanned);
        return true;
    }

    /// <summary>
    /// Snapshots the current build mode and its parameters from durable state (on the
    /// activation thread) into a <b>pure</b> per-value rewrite delegate that is safe to
    /// invoke off the activation scheduler - which the shared dry-run loop does, since
    /// it enumerates the source with <c>ConfigureAwait(false)</c>. The delegate closes
    /// over local snapshots and the thread-safe singleton registry only, never
    /// <see cref="state"/>, so it never touches activation services.
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
    /// Atomically repoints the logical tree to the destination, arms the source
    /// tree's shards to redirect stale logical-alias-routed traffic onto the
    /// destination (so an already-active routing activation self-heals instead of
    /// serving the pre-remediation snapshot), and installs the target policy.
    /// Mirrors the backup shadow-cutover commit. Idempotent: re-running after a
    /// mid-cutover restart repeats the same alias swap, shard redirects (idempotent
    /// per operation id), and policy write.
    /// </summary>
    private async Task CutoverAsync()
    {
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var destinationTreeId = state.State.DestinationTreeId!;
        var operationId = state.State.OperationId!;
        var sourcePhysical = state.State.SourcePhysicalTreeId!;

        // The destination is a fresh, never-aliased tree, so its physical id equals
        // its logical id. Resolve the retained (source) routing by PHYSICAL id -
        // alias-safe, so a resume after a partial cutover re-derives the same
        // shards rather than following the just-installed alias to the destination.
        var destinationPhysical = destinationTreeId;
        var retainedRouting = await grainFactory.GetGrain<ILattice>(sourcePhysical).GetRoutingAsync();

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

        // Repoint the logical tree to the remediated destination.
        await registry.SetAliasAsync(TreeId, destinationPhysical);

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
            await state.WriteStateAsync();
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
