using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// One policy-engine verdict over both layers, with the explain trace the tenant
/// policy facade reports: whether the request is allowed, and which layer and rule
/// decided it. An in-process value produced by
/// <see cref="ITenantPolicyDecisionSource"/>; it carries no serialization attributes.
/// </summary>
/// <param name="Allowed"><see langword="true"/> when the request is authorized, possibly subject to <paramref name="Filtered"/>.</param>
/// <param name="Filtered"><see langword="true"/> when only a subset of a whole-tree request's keys is authorized.</param>
/// <param name="Reason">The engine's human-readable reason, or <see langword="null"/> for a plain allow.</param>
/// <param name="DecidingLayer">The layer whose rule decided, or <see langword="null"/> when no rule matched.</param>
/// <param name="RuleId">The stored id of the deciding rule, or <see langword="null"/> when no rule matched.</param>
/// <param name="Effect">The deciding rule's effect. Meaningful only when <paramref name="RuleId"/> is not <see langword="null"/>.</param>
/// <param name="AllTrees"><see langword="true"/> when the deciding rule is a cluster-wide <c>Tree:*</c> rule.</param>
/// <param name="TenantWide"><see langword="true"/> when the deciding rule is a tenant-wide rule.</param>
internal readonly record struct TenantPolicyVerdict(
    bool Allowed,
    bool Filtered,
    string? Reason,
    TenantRuleLayer? DecidingLayer,
    string? RuleId,
    LatticeEffect Effect,
    bool AllTrees,
    bool TenantWide);
