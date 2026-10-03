using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Auth;

/// <summary>
/// The default <see cref="ILatticeDecisionEngine"/>. Reads the current compiled
/// snapshot from the <see cref="CompiledPolicySnapshotMaintainer"/> and evaluates
/// requests against it with <see cref="PolicyEvaluator"/>. Holds no mutable state
/// of its own; the snapshot lifecycle (build, swap, epoch) lives on the
/// maintainer.
/// </summary>
/// <remarks>
/// The tenant layer is consulted through <see cref="ITenantRuleLayer"/>: each
/// decision reads its <see cref="ITenantRuleLayer.IsActive"/> once, and when it is
/// <c>false</c> (the null default, or tenancy's flag off) nothing else differs from
/// the operator-only engine. When it is <c>true</c> over a snapshot compiled while it
/// was off (<see cref="CompiledPolicy.TenantLayerIncluded"/> is <c>false</c>), the
/// engine asks the maintainer for a coalesced rebuild; until that lands the tenant
/// layer has no rules, so it adds no grant (fail closed).
/// </remarks>
/// <param name="maintainer">The snapshot maintainer.</param>
/// <param name="options">The authorization options.</param>
/// <param name="tenantLayer">
/// The tenant-layer switch. Optional so a host-built engine (tests, the
/// microbenchmark) defaults to the inactive layer; the container supplies the
/// registered seam.
/// </param>
internal sealed class LatticeDecisionEngine(
    CompiledPolicySnapshotMaintainer maintainer,
    IOptionsMonitor<LatticeAuthOptions> options,
    ITenantRuleLayer? tenantLayer = null) : ILatticeDecisionEngine
{
    private static readonly ITenantRuleLayer InactiveTenantLayer = new NullTenantRuleLayer();

    private readonly ITenantRuleLayer _tenantLayer = tenantLayer ?? InactiveTenantLayer;

    /// <inheritdoc />
    public long CurrentEpoch => maintainer.CurrentEpoch;

    /// <inheritdoc />
    public LatticeAccessDecision Evaluate(
        LatticeSubject subject,
        string treeId,
        LatticeOperation operation,
        string? key = null,
        string? rangeStart = null,
        string? rangeEnd = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        var policy = maintainer.Current;
        return PolicyEvaluator.Evaluate(
            policy,
            options.CurrentValue,
            subject,
            treeId,
            operation,
            key,
            rangeStart,
            rangeEnd,
            IsTenantLayerActive(policy),
            out _);
    }

    /// <summary>
    /// Evaluates a request exactly as <see cref="Evaluate(LatticeSubject, string, LatticeOperation, string, string, string)"/> does, additionally
    /// surfacing the winning <paramref name="match"/> so the enforcement gate can
    /// build an audit event without re-evaluating, and so the explain surfaces can
    /// report the deciding layer (<see cref="PolicyMatch.Layer"/>) and rule
    /// (<see cref="PolicyMatch.RuleId"/>). The decision itself is identical to the
    /// fast public evaluation path.
    /// </summary>
    /// <param name="subject">The requesting subject.</param>
    /// <param name="treeId">The target tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="operation">The requested operation.</param>
    /// <param name="key">The exact key for a point request, or <c>null</c> for a collection request.</param>
    /// <param name="rangeStart">The inclusive range start, or <c>null</c>.</param>
    /// <param name="rangeEnd">The exclusive range end, or <c>null</c>.</param>
    /// <param name="match">The winning rule match, or a default (unmatched) value.</param>
    /// <returns>The access decision.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    internal LatticeAccessDecision Evaluate(
        LatticeSubject subject,
        string treeId,
        LatticeOperation operation,
        string? key,
        string? rangeStart,
        string? rangeEnd,
        out PolicyMatch match)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        var policy = maintainer.Current;
        return PolicyEvaluator.Evaluate(
            policy,
            options.CurrentValue,
            subject,
            treeId,
            operation,
            key,
            rangeStart,
            rangeEnd,
            IsTenantLayerActive(policy),
            out match);
    }

    /// <summary>
    /// <c>true</c> when <paramref name="subject"/> can read at least one key of
    /// <paramref name="treeId"/> under <paramref name="operation"/>: it holds an
    /// allow grant whose effective decision at its own scope resolves to allow, or
    /// the tree's default effect is allow. The structural existence-hiding signal;
    /// see <see cref="PolicyEvaluator.HasAnyGrant(CompiledPolicy, LatticeAuthOptions, in LatticeSubject, string, LatticeOperation, bool)"/>.
    /// </summary>
    /// <param name="subject">The requesting subject.</param>
    /// <param name="treeId">The target tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="operation">The operation whose grant is probed.</param>
    /// <returns><see langword="true"/> when the subject can read at least one key.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    internal bool HasAnyGrant(
        LatticeSubject subject,
        string treeId,
        LatticeOperation operation)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        var policy = maintainer.Current;
        return PolicyEvaluator.HasAnyGrant(
            policy,
            options.CurrentValue,
            subject,
            treeId,
            operation,
            IsTenantLayerActive(policy));
    }

    /// <summary>
    /// Reads the tenant-layer switch once. When it is on over a snapshot compiled
    /// without the tenant partition, requests a coalesced rebuild so the partition
    /// appears; the request is a no-op while one is already queued.
    /// </summary>
    private bool IsTenantLayerActive(CompiledPolicy policy)
    {
        if (!_tenantLayer.IsActive)
        {
            return false;
        }

        if (!policy.TenantLayerIncluded)
        {
            maintainer.RequestRebuild();
        }

        return true;
    }
}
