using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// The data-plane operations the local agent is granted on every
/// repository-context tree so the whole <c>repocontext_*</c> tool surface is both
/// advertised and callable. It is exactly the mask the MCP discovery core requires
/// for the repository-context group (read plus the full mutation surface).
/// </summary>
public static class RepoContextGrant
{
    /// <summary>The full repository-context data-plane operation mask.</summary>
    public const LatticeOperation Operations =
        LatticeOperation.Read
        | LatticeOperation.Write
        | LatticeOperation.Delete
        | LatticeOperation.RangeRead
        | LatticeOperation.RangeDelete
        | LatticeOperation.CrdtApply
        | LatticeOperation.AtomicWrite
        | LatticeOperation.BulkLoad;
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
