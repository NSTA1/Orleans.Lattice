using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// The seam the silo health check exercises on every probe: a single, trivial
/// grain call whose completion proves the local silo's membership is active and
/// the grain layer is answering. It is deliberately the narrowest thing that can
/// only succeed when the layer that actually failed is working - a co-located
/// grain activating and reading its durable state - rather than a check that the
/// web host is merely accepting sockets.
/// </summary>
/// <remarks>
/// A bare "is Kestrel listening" probe returns healthy through every failure mode
/// this container has exhibited: a dead or unreachable silo, a wedged grain layer,
/// an unreachable durable store. The one that motivated issue #2666 was a silo
/// that vanished while the process kept listening and every container-health
/// surface stayed green. This seam is the layer a probe must reach to go red for
/// that outage.
/// </remarks>
public interface IRepoContextSiloProbe
{
    /// <summary>
    /// Performs one trivial grain call, returning when it completes and throwing
    /// (or never returning, subject to the caller's timeout) when the silo or
    /// grain layer cannot answer. The value read is irrelevant - completion alone
    /// is the signal.
    /// </summary>
    /// <param name="cancellationToken">
    /// Cancels the call. The health check passes a token that fires after a bounded
    /// timeout so a wedged silo (a call that hangs rather than throws) surfaces as a
    /// fault rather than blocking the probe forever.
    /// </param>
    Task ProbeAsync(CancellationToken cancellationToken);
}

/// <summary>
/// The production <see cref="IRepoContextSiloProbe"/>: a single point-read of the
/// reserved authorization-policy tree, run under the bootstrap-administrator
/// credential the warmup uses. It reads the exact grant the warmup writes, so a
/// completed call proves the same grain-storage and WAL path the box depends on is
/// answering - the read counterpart of the warmup write that flips the host ready.
/// </summary>
/// <param name="policyStore">The durable authorization-policy store, itself grain-backed.</param>
public sealed class RepoContextSiloProbe(ILatticeAuthorizationPolicyStore policyStore) : IRepoContextSiloProbe
{
    // The structural tree's local-agent grant is one of the rules the warmup seeds
    // (RepoContextStartupService.SeedAccessAsync writes local-agent-{tree} for every
    // tree). Reading it back is a single-key point read: cheap enough to run on
    // every orchestrator poll, yet it still requires an active silo and a grain
    // activation, which is exactly the layer the probe must exercise.
    private const string ProbeTree = RepoContextHostTrees.Structural;
    private static readonly string ProbeRuleId = $"local-agent-{RepoContextHostTrees.Structural}";

    private readonly ILatticeAuthorizationPolicyStore _policyStore = policyStore
        ?? throw new ArgumentNullException(nameof(policyStore));

    /// <inheritdoc />
    public async Task ProbeAsync(CancellationToken cancellationToken)
    {
        // Read the reserved policy tree as the bootstrap administrator, exactly as
        // the warmup writes it: the box runs a default-deny access gate, so an
        // ungranted read of sys-auth-policy would fail closed and turn a healthy
        // silo into a false red. The value is discarded - a null (the rule not yet
        // seeded during startup) is still a completed grain call and a valid liveness
        // signal.
        using (LatticeCredentialContext.Use(
            LocalTrustedAgent.BootstrapAdministrator,
            scheme: LocalTrustedAgent.Scheme))
        {
            _ = await _policyStore
                .GetRuleAsync(ProbeTree, ProbeRuleId, cancellationToken)
                .ConfigureAwait(false);
        }
    }
}
