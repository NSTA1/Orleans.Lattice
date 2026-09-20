using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// The operations the local agent is granted on every repository-context tree so
/// the whole <c>repocontext_*</c> tool surface is both advertised and callable, and
/// so the tree-administration facade's orphaned-leaf <b>repair</b> is callable
/// rather than merely advertised. It is the mask the MCP discovery core requires
/// for the repository-context group (read plus the full mutation surface), plus the
/// two distinct capabilities the repair path enforces.
/// </summary>
/// <remarks>
/// <para>
/// <b>The repair is gated twice, at two different seams, on two different
/// capabilities.</b> This is measured, not inferred, and it is the whole reason
/// this mask carries both of the operations below:
/// </para>
/// <list type="number">
///   <item><description>
///     <c>LatticeTreeAdmin.RepairOrphanedLeavesAsync</c> calls
///     <c>TreeAdminAccessAuthorizer.AuthorizeTreeLifecycleAsync</c>, which enforces
///     <see cref="LatticeOperation.TreeLifecycle"/> over the whole tree.
///   </description></item>
///   <item><description>
///     The call then reaches <c>LatticeGrain.DriveOrphanedLeafPassAsync</c>, which
///     enforces a <b>second, independent</b> whole-tree gate - on
///     <see cref="LatticeOperation.Admin"/>, because the non-dry-run pass removes
///     leaves. The sibling inspection verb takes the same path with
///     <see cref="LatticeOperation.Read"/>, which is why the audit was already
///     reachable under the narrower mask while the repair was not.
///   </description></item>
/// </list>
/// <para>
/// Granting only the first leaves the repair advertised and then refused at the
/// grain with <c>Access denied: ... not authorized to perform Admin</c> - exactly
/// the failure mode the tool group's lifecycle opt-in was held back to avoid. Both
/// are therefore required, and dropping either one makes the verb unreachable
/// again.
/// </para>
/// <para>
/// <b>These are whole capabilities, not single verbs.</b> Together they confer
/// every tree-lifecycle and tree-administration verb the facade and the grain
/// expose over every repository-context tree, not only the unsplice. That is
/// accepted here because the grant is scoped per tree to the eleven
/// repository-context trees - never cluster-wide - this container is a single-user
/// local box whose sole caller is the trusted local agent, and the facade's
/// <c>ThrowIfReserved</c> plus the grain's <c>ThrowIfSystemTree</c> still refuse the
/// reserved <c>_lattice_</c> namespace outright. On any shared or multi-tenant
/// deployment this mask would need to be narrower, and the repair would need a
/// capability of its own.
/// </para>
/// <para>
/// The motivation is WAL growth: an orphaned leaf holds a materialiser pin that
/// never advances, and the trim floor is the minimum over all pins, so an
/// unrepairable orphan pins the WAL open indefinitely.
/// </para>
/// </remarks>
public static class RepoContextGrant
{
    /// <summary>
    /// The full repository-context data-plane operation mask, plus the two
    /// capabilities the orphaned-leaf repair is gated on.
    /// </summary>
    public const LatticeOperation Operations =
        LatticeOperation.Read
        | LatticeOperation.Write
        | LatticeOperation.Delete
        | LatticeOperation.RangeRead
        | LatticeOperation.RangeDelete
        | LatticeOperation.CrdtApply
        | LatticeOperation.AtomicWrite
        | LatticeOperation.BulkLoad
        | LatticeOperation.TreeLifecycle
        | LatticeOperation.Admin;
}

/// <summary>
/// The startup / shutdown coordinator that ties the container's lifecycle to its
/// readiness signal. On application start it runs the warmup: it seeds the local
/// agent's access grant on every repository-context tree - a write through the
/// reserved auth-policy Lattice tree that proves the grain-storage and WAL
/// providers are reachable and writable, which doubles as the readiness gate -
/// then flips the host to ready. On application stop it flips readiness to
/// not-ready <b>before</b> the silo begins to drain, so a load balancer stops
/// routing new MCP requests while in-flight writes flush to the WAL.
/// </summary>
public sealed class RepoContextStartupService : IHostedService
{
    private readonly ILatticeAuthorizationPolicyStore _policyStore;
    private readonly ILatticeSchemaVersionAdmin _versionAdmin;
    private readonly RepoContextReadinessState _readiness;
    private readonly IHostApplicationLifetime _lifetime;
    private readonly ILogger<RepoContextStartupService> _logger;
    private readonly CancellationTokenSource _stopping = new();
    private Task? _warmup;

    /// <summary>Initializes the coordinator.</summary>
    /// <param name="policyStore">The authorization policy store used to seed the local agent's grant.</param>
    /// <param name="versionAdmin">The schema-version admin used to opt the symbol tree in to envelope versioning.</param>
    /// <param name="readiness">The shared readiness state.</param>
    /// <param name="lifetime">The host application lifetime.</param>
    /// <param name="logger">The logger.</param>
    /// <exception cref="ArgumentNullException">Any argument is null.</exception>
    public RepoContextStartupService(
        ILatticeAuthorizationPolicyStore policyStore,
        ILatticeSchemaVersionAdmin versionAdmin,
        RepoContextReadinessState readiness,
        IHostApplicationLifetime lifetime,
        ILogger<RepoContextStartupService> logger)
    {
        _policyStore = policyStore ?? throw new ArgumentNullException(nameof(policyStore));
        _versionAdmin = versionAdmin ?? throw new ArgumentNullException(nameof(versionAdmin));
        _readiness = readiness ?? throw new ArgumentNullException(nameof(readiness));
        _lifetime = lifetime ?? throw new ArgumentNullException(nameof(lifetime));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        _lifetime.ApplicationStarted.Register(() => _warmup = WarmupAsync(_stopping.Token));
        _lifetime.ApplicationStopping.Register(() => _readiness.BeginDrain());
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        // Readiness already flipped to draining on ApplicationStopping; make sure
        // the warmup loop is cancelled so it cannot re-open readiness mid-drain.
        _readiness.BeginDrain();
        await _stopping.CancelAsync().ConfigureAwait(false);

        if (_warmup is not null)
        {
            try
            {
                await _warmup.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected when shutdown interrupts a warmup retry.
            }
        }
    }

    /// <summary>
    /// Seeds the local agent's access grant on every repository-context tree,
    /// retrying with backoff until it succeeds or shutdown is requested, then marks
    /// the host ready. A successful seed proves the durable stores are reachable.
    /// </summary>
    /// <param name="cancellationToken">Cancelled when the host begins to stop.</param>
    internal async Task WarmupAsync(CancellationToken cancellationToken)
    {
        var attempt = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await SeedAccessAsync(cancellationToken).ConfigureAwait(false);
                _readiness.MarkReady();
                _logger.LogInformation(
                    "RepoContext host warmup complete: local-agent grant seeded on {TreeCount} trees; host is ready.",
                    RepoContextHostTrees.All.Count);
                return;
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception ex)
            {
                attempt++;
                var delay = TimeSpan.FromSeconds(Math.Min(30, 1 << Math.Min(attempt, 5)));
                _logger.LogWarning(
                    ex,
                    "RepoContext host warmup attempt {Attempt} failed (durable stores not yet reachable); "
                    + "retrying in {Delay}. Readiness stays not-ready.",
                    attempt,
                    delay);
                try
                {
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }
    }

    /// <summary>
    /// Seeds one Allow rule per repository-context tree granting the local agent
    /// the full data-plane mask. Runs as the bootstrap administrator so the writes
    /// to the reserved policy tree bypass the default-deny gate.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token.</param>
    internal async Task SeedAccessAsync(CancellationToken cancellationToken)
    {
        using (LatticeCredentialContext.Use(
            LocalTrustedAgent.BootstrapAdministrator,
            scheme: LocalTrustedAgent.Scheme))
        {
            foreach (var tree in RepoContextHostTrees.All)
            {
                var rule = new LatticeAuthorizationRule(
                    ruleId: $"local-agent-{tree}",
                    subject: LatticeSubjectSelector.User(LocalTrustedAgent.SubjectId),
                    scope: LatticeScope.Tree(tree),
                    operations: RepoContextGrant.Operations,
                    effect: LatticeEffect.Allow);

                await _policyStore.PutRuleAsync(rule, cancellationToken).ConfigureAwait(false);
            }

            // Opt the symbol tree in to envelope versioning at its target version.
            // SetVersionConfigAsync is SchemaAdmin-gated; it succeeds here because the
            // bootstrap administrator bypasses the default-deny gate, exactly as the
            // reserved-policy-tree writes above do. The call is idempotent: a restart
            // re-installs the same (schemaId, version) config with no observable
            // change, so warmup stays safe to retry.
            var existing = await _versionAdmin
                .GetVersionConfigAsync(RepoContextHostTrees.Symbol, cancellationToken)
                .ConfigureAwait(false);
            if (existing is null)
            {
                await _versionAdmin.SetVersionConfigAsync(
                    RepoContextHostTrees.Symbol,
                    new LatticeSchemaVersionConfig(
                        RepoContextHostTrees.SymbolSchemaId,
                        RepoContextHostTrees.SymbolSchemaVersion),
                    cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
