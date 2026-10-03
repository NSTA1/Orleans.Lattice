using Microsoft.Extensions.Options;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The production <see cref="ITenantPolicyDecisionSource"/>: evaluates through the
/// auth add-on's decision engine - the same compiled-snapshot evaluator the data
/// plane's access gate consults - and maps its explain trace
/// (<see cref="PolicyMatch.Layer"/>, <see cref="PolicyMatch.RuleId"/>,
/// <see cref="PolicyMatch.AllTrees"/>, <see cref="PolicyMatch.TenantWide"/>) onto a
/// <see cref="TenantPolicyVerdict"/>.
/// </summary>
/// <remarks>
/// A host that replaced <see cref="ILatticeDecisionEngine"/> with its own engine
/// still gets a correct verdict through the public evaluation, without a deciding
/// layer or rule: the trace exists only on the shipped engine.
/// </remarks>
internal sealed class EngineTenantPolicyDecisionSource : ITenantPolicyDecisionSource
{
    private readonly ILatticeDecisionEngine _engine;
    private readonly LatticeDecisionEngine? _tracingEngine;
    private readonly IOptionsMonitor<LatticeAuthOptions> _options;

    /// <summary>Initializes a new <see cref="EngineTenantPolicyDecisionSource"/>.</summary>
    /// <param name="engine">The registered decision engine. Must not be <see langword="null"/>.</param>
    /// <param name="options">The authorization options supplying the default effect. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
    public EngineTenantPolicyDecisionSource(ILatticeDecisionEngine engine, IOptionsMonitor<LatticeAuthOptions> options)
    {
        ArgumentNullException.ThrowIfNull(engine);
        ArgumentNullException.ThrowIfNull(options);
        _engine = engine;
        _tracingEngine = engine as LatticeDecisionEngine;
        _options = options;
    }

    /// <inheritdoc />
    public LatticeEffect DefaultEffect => _options.CurrentValue.DefaultEffect;

    /// <inheritdoc />
    public TenantPolicyVerdict Evaluate(LatticeSubject subject, string treeId, LatticeOperation operation, string? key)
    {
        if (_tracingEngine is null)
        {
            var plain = _engine.Evaluate(subject, treeId, operation, key);
            return new TenantPolicyVerdict(
                plain.Allowed, plain.KeyFilter is not null, plain.Reason, null, null, default, false, false);
        }

        var decision = _tracingEngine.Evaluate(subject, treeId, operation, key, null, null, out var match);
        return new TenantPolicyVerdict(
            decision.Allowed,
            decision.KeyFilter is not null,
            decision.Reason,
            ToLayer(match),
            match.Matched ? match.RuleId : null,
            match.Effect,
            match.Matched && match.AllTrees,
            match.Matched && match.TenantWide);
    }

    private static TenantRuleLayer? ToLayer(in PolicyMatch match) =>
        !match.Matched
            ? null
            : match.Layer == PolicyDecisionLayer.Tenant ? TenantRuleLayer.Tenant : TenantRuleLayer.Platform;
}
