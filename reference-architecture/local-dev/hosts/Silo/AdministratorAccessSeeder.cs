using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.ReferenceArchitecture.Silo;

/// <summary>
/// Seeds every configured bootstrap administrator
/// (<c>Auth:BootstrapAdministrators</c>) with a cluster-wide, full-capability
/// authorization rule at silo startup, so the estate's designated security
/// administrator can discover and use every MCP tool group (state, data, backup,
/// auth, telemetry, replication) immediately after deployment - without first
/// hand-authoring a grant for themselves through the Explorer Access tab.
/// </summary>
/// <remarks>
/// <para>
/// The bootstrap-administrator root of trust is <b>unconditionally Admin on every
/// tree and operation</b> at the data-plane access gate, so an administrator can
/// already <em>invoke</em> anything. MCP <b>discovery</b>, however, is
/// deliberately independent of that bypass: it advertises a tool group only when
/// the caller holds an <em>authored</em> Allow rule covering one of the group's
/// operations (it reads effective permissions, never the bootstrap set). A freshly
/// deployed administrator therefore has full call-time authority but is offered no
/// tools until a rule exists for their subject. Seeding that rule here closes the
/// gap declaratively as part of the deployment.
/// </para>
/// <para>
/// The rule is authored on the <see cref="LatticeScope.ClusterWide"/> sentinel so
/// a single grant lights up every facade group during discovery; call-time
/// enforcement is unaffected because the administrator is already allowed
/// everywhere by the bootstrap bypass. Writing the reserved policy tree is
/// authorization infrastructure and runs system-origin inside the store, so no
/// ambient administrator credential is required. Each rule carries a deterministic
/// id keyed by the administrator's subject id, so re-running on every silo /
/// region start is idempotent and converges (last-writer-wins) across the
/// replicated policy tree.
/// </para>
/// </remarks>
internal sealed class AdministratorAccessSeeder(
    IServiceProvider services,
    IConfiguration configuration,
    ILogger<AdministratorAccessSeeder> logger) : BackgroundService
{
    /// <summary>The configuration key holding the comma-separated administrator subject ids.</summary>
    internal const string AdministratorsKey = "Auth:BootstrapAdministrators";

    /// <summary>The stable rule-id prefix; the administrator subject id is appended for a deterministic, idempotent key.</summary>
    internal const string RuleIdPrefix = "ra-admin-full-access";

    /// <summary>
    /// The union of every operation the MCP facade groups require - the
    /// data-plane / lifecycle / backup / schema set
    /// (<see cref="LatticeAuthOperations.All"/>) plus the scopeless capability
    /// bits (<see cref="LatticeOperation.Telemetry"/> and
    /// <see cref="LatticeOperation.Replication"/>) and the irreversible
    /// whole-tree <see cref="LatticeOperation.TreeLifecycle"/> capability that
    /// <c>All</c> deliberately excludes. Granting this lights up state, data,
    /// backup, auth, telemetry, replication and tree-administration tools in MCP
    /// discovery, and gives the bootstrap administrator an explicit authored grant
    /// for the lifecycle-gated tree-administration operations (drop / reshard /
    /// resize) rather than relying on the bootstrap-admin call-time bypass alone.
    /// </summary>
    internal const LatticeOperation FullAccessOperations =
        LatticeAuthOperations.All | LatticeOperation.Telemetry | LatticeOperation.Replication | LatticeOperation.TreeLifecycle;

    private const int MaxAttempts = 12;
    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(5);

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var administrators = ParseAdministrators(configuration);
        if (administrators.Count == 0)
        {
            return;
        }

        var store = services.GetService<ILatticeAuthorizationPolicyStore>();
        if (store is null)
        {
            logger.LogWarning(
                "No ILatticeAuthorizationPolicyStore is registered; skipping administrator access seeding. "
                + "MCP tools will not be advertised to the security administrator(s) until a grant is authored.");
            return;
        }

        foreach (var administratorId in administrators)
        {
            await SeedAdministratorAsync(store, administratorId, stoppingToken).ConfigureAwait(false);
        }
    }

    private async Task SeedAdministratorAsync(
        ILatticeAuthorizationPolicyStore store,
        string administratorId,
        CancellationToken stoppingToken)
    {
        var rule = BuildFullAccessRule(administratorId);
        for (var attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            try
            {
                // The store enters system-origin internally, so this write bypasses
                // the enforcement gate it feeds; no administrator credential context
                // is needed. Idempotent on the deterministic rule id.
                await store.PutRuleAsync(rule, stoppingToken).ConfigureAwait(false);
                logger.LogInformation(
                    "Seeded cluster-wide full-access grant '{RuleId}' for security administrator '{AdministratorId}'.",
                    rule.RuleId,
                    administratorId);
                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex) when (attempt < MaxAttempts)
            {
                // The silo may not yet be active (grain calls fail until it is), so
                // retry with a fixed backoff before giving up.
                logger.LogDebug(
                    ex,
                    "Attempt {Attempt}/{MaxAttempts} to seed the administrator grant for '{AdministratorId}' failed; retrying.",
                    attempt,
                    MaxAttempts,
                    administratorId);
                try
                {
                    await Task.Delay(RetryDelay, stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
            catch (Exception ex)
            {
                logger.LogWarning(
                    ex,
                    "Failed to seed the cluster-wide full-access grant for security administrator '{AdministratorId}' "
                    + "after {MaxAttempts} attempts; the administrator will see no MCP tools until a grant is authored manually.",
                    administratorId,
                    MaxAttempts);
                return;
            }
        }
    }

    /// <summary>
    /// Parses the comma-separated <see cref="AdministratorsKey"/> configuration
    /// value into a de-duplicated, ordinal-ordered list of administrator subject
    /// ids (empty when unset).
    /// </summary>
    internal static IReadOnlyList<string> ParseAdministrators(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var raw = configuration[AdministratorsKey];
        if (string.IsNullOrWhiteSpace(raw))
        {
            return [];
        }

        var seen = new HashSet<string>(StringComparer.Ordinal);
        var result = new List<string>();
        foreach (var entry in raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (seen.Add(entry))
            {
                result.Add(entry);
            }
        }

        return result;
    }

    /// <summary>
    /// Builds the deterministic cluster-wide Allow rule that grants
    /// <paramref name="administratorId"/> every capability the MCP facade groups
    /// require.
    /// </summary>
    internal static LatticeAuthorizationRule BuildFullAccessRule(string administratorId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(administratorId);
        return new LatticeAuthorizationRule(
            $"{RuleIdPrefix}:{administratorId}",
            LatticeSubjectSelector.User(administratorId),
            LatticeScope.ClusterWide(),
            FullAccessOperations,
            LatticeEffect.Allow);
    }
}
