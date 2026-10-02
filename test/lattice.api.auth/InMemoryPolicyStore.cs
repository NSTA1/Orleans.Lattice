using System.Runtime.CompilerServices;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// An in-memory <see cref="ILatticeAuthorizationPolicyStore"/> for the facade's unit
/// tests: rules are listed in the store's own <c>(tree id, rule id)</c> order, and
/// every write and delete records whether it ran under system origin, so a test can
/// prove the facade's break-glass removal runs there and that a refused write never
/// reached the store.
/// </summary>
internal sealed class InMemoryPolicyStore : ILatticeAuthorizationPolicyStore
{
    private readonly List<LatticeAuthorizationRule> _rules = new();

    /// <summary>Every <see cref="PutRuleAsync"/> call, in order.</summary>
    public List<LatticeAuthorizationRule> Puts { get; } = new();

    /// <summary>Every <see cref="RemoveRuleAsync"/> call, with whether it ran under system origin.</summary>
    public List<(string TreeId, string RuleId, bool SystemOrigin)> Removes { get; } = new();

    /// <summary>Seeds rules without recording a put.</summary>
    public InMemoryPolicyStore Seed(params LatticeAuthorizationRule[] rules)
    {
        _rules.AddRange(rules);
        return this;
    }

    public Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default)
    {
        Puts.Add(rule);
        _rules.RemoveAll(r => Same(r, rule.Scope.TreeId, rule.RuleId));
        _rules.Add(rule);
        return Task.CompletedTask;
    }

    public Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default) =>
        Task.FromResult(_rules.FirstOrDefault(r => Same(r, treeId, ruleId)));

    public Task<bool> RemoveRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)
    {
        Removes.Add((treeId, ruleId, LatticeAccessGateContext.IsSystemOrigin));
        return Task.FromResult(_rules.RemoveAll(r => Same(r, treeId, ruleId)) > 0);
    }

    public IAsyncEnumerable<LatticeAuthorizationRule> ListRulesForTreeAsync(string treeId, CancellationToken cancellationToken = default) =>
        Yield(Ordered().Where(r => string.Equals(r.Scope.TreeId, treeId, StringComparison.Ordinal)), cancellationToken);

    public IAsyncEnumerable<LatticeAuthorizationRule> ListRulesAsync(CancellationToken cancellationToken = default) =>
        Yield(Ordered(), cancellationToken);

    private IEnumerable<LatticeAuthorizationRule> Ordered() =>
        _rules
            .OrderBy(r => r.Scope.TreeId, StringComparer.Ordinal)
            .ThenBy(r => r.RuleId, StringComparer.Ordinal)
            .ToList();

    private static bool Same(LatticeAuthorizationRule rule, string treeId, string ruleId) =>
        string.Equals(rule.Scope.TreeId, treeId, StringComparison.Ordinal)
        && string.Equals(rule.RuleId, ruleId, StringComparison.Ordinal);

    private static async IAsyncEnumerable<LatticeAuthorizationRule> Yield(
        IEnumerable<LatticeAuthorizationRule> rules,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var rule in rules)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return rule;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }
}
