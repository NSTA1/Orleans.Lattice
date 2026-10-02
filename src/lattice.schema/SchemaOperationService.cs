using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Schema;

/// <summary>
/// Runs schema remediations and eager migrations as tracked long-running
/// operations: the schema engine's client of the shared
/// <see cref="LatticeOperationRunner"/>. The work accepts the remediation on the
/// tree's coordinator grain and then drives it one bounded slice at a time through
/// <see cref="SchemaRemediationDriver"/>, reporting each slice's phase and values
/// processed, and turning a cancellation into a cancel of the remediation itself.
/// </summary>
/// <remarks>
/// Callers authorize before starting: this service starts exactly the work it is
/// given. The returned <see cref="LatticeOperationLaunch{TResult}.Completion"/>
/// carries the terminal <see cref="LatticeSchemaRemediationReport"/>, or the
/// engine's own exception, which is what lets the blocking facade verbs wrap a
/// start without changing what they return or throw.
/// </remarks>
internal sealed class SchemaOperationService(
    LatticeOperationRunner runner,
    IGrainFactory grainFactory,
    IServiceProvider services)
{
    /// <summary>The phases a remediation or migration reports.</summary>
    internal static readonly IReadOnlyList<string> RemediationPhases =
        [SchemaOperationPhases.DryRun, SchemaOperationPhases.Build, SchemaOperationPhases.Cutover];

    /// <summary>The phases an advance-and-migrate reports.</summary>
    internal static readonly IReadOnlyList<string> AdvanceAndMigratePhases =
    [
        SchemaOperationPhases.Advance,
        SchemaOperationPhases.DryRun,
        SchemaOperationPhases.Build,
        SchemaOperationPhases.Cutover,
    ];

    /// <summary>The shared runner, for status, list and cancel.</summary>
    internal LatticeOperationRunner Runner => runner;

    /// <summary>Starts a remediation of <paramref name="treeId"/>.</summary>
    /// <param name="tenantId">The owning tenant.</param>
    /// <param name="operationId">The validated operation id.</param>
    /// <param name="treeId">The effective tree id.</param>
    /// <param name="transform">The per-value transform.</param>
    /// <param name="targetPolicy">The target policy. Must not be <c>null</c>.</param>
    /// <returns>The launch.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="targetPolicy"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is reserved, or <paramref name="targetPolicy"/> carries an uncompilable rule.</exception>
    internal Task<LatticeOperationLaunch<LatticeSchemaRemediationReport>> StartRemediationAsync(
        string tenantId,
        string operationId,
        string treeId,
        LatticeValueTransform transform,
        LatticeSchemaPolicy targetPolicy)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(targetPolicy);
        SchemaConstants.ThrowIfReservedTree(treeId, nameof(treeId));

        // Refuse an uncompilable policy before anything is accepted, as the blocking
        // verb always has.
        _ = CompiledSchemaPolicy.Compile(targetPolicy);
        var grain = Remediation(treeId);
        return runner.StartAsync(
            Start(tenantId, operationId, SchemaOperationKinds.Remediation, treeId, RemediationPhases),
            async (progress, ct) =>
            {
                var accepted = await grain.AcceptAsync(transform, targetPolicy, operationId).ConfigureAwait(false);
                return await SchemaRemediationDriver.DriveAsync(grain, accepted, progress, ct).ConfigureAwait(false);
            },
            ToCompletion);
    }

    /// <summary>Starts an eager migration of <paramref name="treeId"/> to its current target version.</summary>
    /// <param name="tenantId">The owning tenant.</param>
    /// <param name="operationId">The validated operation id.</param>
    /// <param name="treeId">The effective tree id.</param>
    /// <returns>The launch.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is reserved.</exception>
    /// <exception cref="InvalidOperationException">Schema versioning is not registered.</exception>
    internal Task<LatticeOperationLaunch<LatticeSchemaRemediationReport>> StartMigrationAsync(
        string tenantId,
        string operationId,
        string treeId)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        SchemaConstants.ThrowIfReservedTree(treeId, nameof(treeId));
        var versions = RequireVersionAdmin();
        var grain = Remediation(treeId);
        return runner.StartAsync(
            Start(tenantId, operationId, SchemaOperationKinds.Migration, treeId, RemediationPhases),
            async (progress, ct) =>
            {
                var config = await versions.GetVersionConfigAsync(treeId, ct).ConfigureAwait(false)
                    ?? throw LatticeSchemaVersionAdmin.NotVersioned(treeId);
                var accepted = await grain
                    .AcceptVersionMigrationAsync(config.SchemaId, config.TargetVersion, operationId)
                    .ConfigureAwait(false);
                return await SchemaRemediationDriver.DriveAsync(grain, accepted, progress, ct).ConfigureAwait(false);
            },
            ToCompletion);
    }

    /// <summary>Starts an advance of <paramref name="treeId"/>'s target version, then an eager migration to it.</summary>
    /// <param name="tenantId">The owning tenant.</param>
    /// <param name="operationId">The validated operation id.</param>
    /// <param name="treeId">The effective tree id.</param>
    /// <param name="newTargetVersion">The new target version.</param>
    /// <returns>The launch.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is reserved.</exception>
    /// <exception cref="InvalidOperationException">Schema versioning is not registered.</exception>
    internal Task<LatticeOperationLaunch<LatticeSchemaRemediationReport>> StartAdvanceAndMigrateAsync(
        string tenantId,
        string operationId,
        string treeId,
        uint newTargetVersion)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        SchemaConstants.ThrowIfReservedTree(treeId, nameof(treeId));
        var versions = RequireVersionAdmin();
        var grain = Remediation(treeId);
        return runner.StartAsync(
            Start(tenantId, operationId, SchemaOperationKinds.AdvanceAndMigrate, treeId, AdvanceAndMigratePhases),
            async (progress, ct) =>
            {
                var advanced = await versions.AdvanceTargetVersionAsync(treeId, newTargetVersion, ct).ConfigureAwait(false);
                var accepted = await grain
                    .AcceptVersionMigrationAsync(advanced.SchemaId, advanced.TargetVersion, operationId)
                    .ConfigureAwait(false);
                return await SchemaRemediationDriver.DriveAsync(grain, accepted, progress, ct).ConfigureAwait(false);
            },
            ToCompletion);
    }

    /// <summary>
    /// Maps a terminal report to the recorded outcome: a completed remediation
    /// succeeds; one that stopped at an offending value fails, naming the value; one
    /// cancelled before cutover is cancelled.
    /// </summary>
    /// <param name="report">The terminal report.</param>
    /// <returns>The completion.</returns>
    internal static LatticeOperationCompletion ToCompletion(LatticeSchemaRemediationReport report)
    {
        var processed = report.ScannedCount.ToString(System.Globalization.CultureInfo.InvariantCulture);
        if (report.DidAbort)
        {
            var result = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [SchemaOperationResultKeys.Outcome] = SchemaOperationResultKeys.Aborted,
                [SchemaOperationResultKeys.ValuesProcessed] = processed,
                [SchemaOperationResultKeys.OffendingKey] = report.OffendingKey ?? string.Empty,
                [SchemaOperationResultKeys.Reason] = report.Reason ?? string.Empty,
            };
            AddRemediationId(result, report);
            return new LatticeOperationCompletion
            {
                State = LatticeOperationState.Failed,
                FailureReason = $"Stopped at key '{report.OffendingKey}': {report.Reason} Nothing was cut over.",
                Result = result,
            };
        }

        if (report.WasCancelled)
        {
            var result = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [SchemaOperationResultKeys.Outcome] = SchemaOperationResultKeys.Cancelled,
                [SchemaOperationResultKeys.ValuesProcessed] = processed,
            };
            AddRemediationId(result, report);
            return new LatticeOperationCompletion
            {
                State = LatticeOperationState.Cancelled,
                FailureReason = "Cancelled before cutover. Nothing was cut over.",
                Result = result,
            };
        }

        if (!report.Succeeded)
        {
            return LatticeOperationCompletion.Failed(
                $"The remediation ended in the unexpected phase {report.Phase}.");
        }

        var succeeded = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [SchemaOperationResultKeys.Outcome] = SchemaOperationResultKeys.Completed,
            [SchemaOperationResultKeys.ValuesProcessed] = processed,
        };
        AddRemediationId(succeeded, report);
        return LatticeOperationCompletion.Succeeded(result: succeeded);
    }

    private static void AddRemediationId(Dictionary<string, string> result, LatticeSchemaRemediationReport report)
    {
        if (report.OperationId is { } id)
        {
            result[SchemaOperationResultKeys.RemediationOperationId] = id;
        }
    }

    private ILatticeSchemaRemediationGrain Remediation(string treeId) =>
        grainFactory.GetGrain<ILatticeSchemaRemediationGrain>(treeId);

    private ILatticeSchemaVersionAdmin RequireVersionAdmin() =>
        services.GetService<ILatticeSchemaVersionAdmin>()
        ?? throw new InvalidOperationException(
            "Schema versioning is not registered on this silo; call AddLatticeSchemaVersioning(...) to enable "
            + "schema-version migrations.");

    private static LatticeOperationStart Start(
        string tenantId,
        string operationId,
        string kind,
        string treeId,
        IReadOnlyList<string> phases) =>
        new()
        {
            TenantId = tenantId,
            OperationId = operationId,
            Kind = kind,
            TreeIds = [treeId],
            Phases = phases,
        };
}
