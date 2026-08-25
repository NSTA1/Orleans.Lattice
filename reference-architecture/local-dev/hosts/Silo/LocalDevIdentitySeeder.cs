using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.ReferenceArchitecture.Silo;

/// <summary>
/// Seeds the local-dev identity model into the cluster at silo startup: it writes
/// every declared group and membership edge into the durable membership directory
/// (the reserved <c>sys-membership-*</c> trees) and authors every group's
/// authorization grant into the policy store (the reserved <c>sys-auth-policy</c>
/// tree). Together with the deny-by-default effect this is what gives the harness
/// its differentiated, real-enforcement identities without an Entra tenant: an
/// operator can write but not back up, a reader can only read, an auditor sees only
/// telemetry, and everything else is denied.
/// </summary>
/// <remarks>
/// <para>
/// Group memberships are seeded into the directory even though the
/// <see cref="DevIdentityCredentialAuthenticator"/> also attaches them as asserted
/// groups on the principal. The asserted-group path makes grants resolve on the
/// very first call (before this seed lands); the directory path makes the same
/// identity model introspectable through the ordinary read / scan surface and the
/// Explorer Access tab, so an operator can SEE who is in which group and why a
/// call was allowed or denied. The two agree by construction because both derive
/// from the one mounted <c>identities.json</c>.
/// </para>
/// <para>
/// Writing the reserved directory and policy trees is authorization infrastructure
/// and runs system-origin inside the grains, so no ambient administrator credential
/// is required. Every write is idempotent (group upsert, member add, and rule put
/// all converge last-writer-wins on a deterministic key), so re-running on every
/// silo / region start is safe and the whole batch is retried with a fixed backoff
/// because grain calls fail until the silo is active. The bootstrap administrator's
/// own cluster-wide grant is authored separately by
/// <see cref="AdministratorAccessSeeder"/>; this seeder never touches it.
/// </para>
/// </remarks>
internal sealed class LocalDevIdentitySeeder(
    IServiceProvider services,
    LocalDevIdentityModel model,
    ILogger<LocalDevIdentitySeeder> logger) : BackgroundService
{
    /// <summary>The stable rule-id prefix for a seeded group grant; the group id and scope are appended for a deterministic, idempotent key.</summary>
    internal const string RuleIdPrefix = "ld-grant";

    private const int MaxAttempts = 12;
    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(5);

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (model.Groups.Count == 0 && model.Identities.Count == 0)
        {
            return;
        }

        var directory = services.GetService<ILatticeMembershipDirectory>();
        var store = services.GetService<ILatticeAuthorizationPolicyStore>();
        if (directory is null || store is null)
        {
            logger.LogWarning(
                "Membership directory or authorization policy store is not registered; "
                + "skipping local-dev identity seeding. Differentiated identities will have no effect.");
            return;
        }

        for (var attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            try
            {
                await SeedAsync(directory, store, stoppingToken).ConfigureAwait(false);
                logger.LogInformation(
                    "Seeded local-dev identity model: {GroupCount} group(s), {IdentityCount} identity(ies).",
                    model.Groups.Count,
                    model.Identities.Count);
                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex) when (attempt < MaxAttempts)
            {
                // The silo may not yet be active (grain calls fail until it is), so
                // retry the whole idempotent batch with a fixed backoff.
                logger.LogDebug(
                    ex,
                    "Attempt {Attempt}/{MaxAttempts} to seed the local-dev identity model failed; retrying.",
                    attempt,
                    MaxAttempts);
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
                    "Failed to seed the local-dev identity model after {MaxAttempts} attempts; "
                    + "differentiated identities may not be enforced until the model is seeded.",
                    MaxAttempts);
                return;
            }
        }
    }

    private async Task SeedAsync(
        ILatticeMembershipDirectory directory,
        ILatticeAuthorizationPolicyStore store,
        CancellationToken cancellationToken)
    {
        // 1. Groups and their grants.
        foreach (var group in model.Groups.Values)
        {
            await directory.UpsertGroupAsync(new MembershipGroup(group.Id, group.DisplayName), cancellationToken)
                .ConfigureAwait(false);

            foreach (var grant in group.Grants)
            {
                await store.PutRuleAsync(BuildGroupGrantRule(group.Id, grant), cancellationToken).ConfigureAwait(false);
            }
        }

        // 2. Membership edges.
        foreach (var identity in model.Identities.Values)
        {
            foreach (var groupId in identity.Groups)
            {
                await directory.AddMemberAsync(groupId, identity.Id, MembershipMemberKind.User, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Builds the deterministic group grant rule for <paramref name="grant"/>. The
    /// rule id is keyed by group id, scope, and effect so re-seeding converges
    /// rather than accumulating duplicates.
    /// </summary>
    internal static LatticeAuthorizationRule BuildGroupGrantRule(string groupId, LocalDevGrant grant)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        ArgumentNullException.ThrowIfNull(grant);

        var scopeToken = grant.ScopeKind == LocalDevScopeKind.Cluster ? "cluster" : $"tree-{grant.TreeId}";
        var ruleId = $"{RuleIdPrefix}:{groupId}:{scopeToken}:{grant.Effect}".ToLowerInvariant();
        return new LatticeAuthorizationRule(
            ruleId,
            LatticeSubjectSelector.Group(groupId),
            grant.ToScope(),
            grant.Operations,
            grant.Effect);
    }
}
