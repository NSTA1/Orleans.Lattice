using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The policy-decision seam the tenant policy facade explains through: one
/// evaluation of both policy layers that also reports the deciding layer and rule.
/// The production implementation, <see cref="EngineTenantPolicyDecisionSource"/>,
/// reads the auth add-on's compiled-snapshot decision engine and its explain trace;
/// tests substitute a scripted source.
/// </summary>
internal interface ITenantPolicyDecisionSource
{
    /// <summary>The effect applied when no rule of either layer matches.</summary>
    LatticeEffect DefaultEffect { get; }

    /// <summary>
    /// Evaluates whether <paramref name="subject"/> may perform
    /// <paramref name="operation"/> on <paramref name="treeId"/> (or one key of it),
    /// synchronously and in-memory, reporting the deciding layer and rule.
    /// </summary>
    /// <param name="subject">The subject, carrying its resolved transitive group closure.</param>
    /// <param name="treeId">The full tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="operation">The operation to evaluate.</param>
    /// <param name="key">The key for a point request, or <see langword="null"/> for a whole-tree request.</param>
    /// <returns>The verdict and its explain trace.</returns>
    TenantPolicyVerdict Evaluate(LatticeSubject subject, string treeId, LatticeOperation operation, string? key);
}
