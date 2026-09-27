using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// An in-memory policy store that, like the real one, rejects app-owned rule writes made
/// outside system origin.
/// </summary>
internal sealed class InMemoryPolicyStore : ILatticeAuthorizationPolicyStore
{
    private readonly Dictionary<(string Tree, string Id), LatticeAuthorizationRule> _rules = new();

    public Func<LatticeAuthorizationRule, Exception?>? FailPut { get; set; }

    public int Puts { get; private set; }

    public int Removes { get; private set; }

    public IReadOnlyCollection<LatticeAuthorizationRule> Rules => _rules.Values;

    public void Seed(LatticeAuthorizationRule rule) => _rules[(rule.Scope.TreeId, rule.RuleId)] = rule;

    public Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default)
    {
        Guard(rule.RuleId);
        if (FailPut?.Invoke(rule) is { } failure)
            throw failure;
        Puts++;
        _rules[(rule.Scope.TreeId, rule.RuleId)] = rule;
        return Task.CompletedTask;
    }

    public Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default) =>
        Task.FromResult(_rules.TryGetValue((treeId, ruleId), out var rule) ? rule : null);

    public Task<bool> RemoveRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)
    {
        Guard(ruleId);
        Removes++;
        return Task.FromResult(_rules.Remove((treeId, ruleId)));
    }

    public async IAsyncEnumerable<LatticeAuthorizationRule> ListRulesForTreeAsync(string treeId, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var rule in _rules.Values.Where(r => r.Scope.TreeId == treeId).ToArray())
            yield return rule;
        await Task.CompletedTask;
    }

    public async IAsyncEnumerable<LatticeAuthorizationRule> ListRulesAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var rule in _rules.Values.ToArray())
            yield return rule;
        await Task.CompletedTask;
    }

    private static void Guard(string ruleId)
    {
        if (LatticeAppRuleIds.IsAppOwned(ruleId) && !LatticeSystemOrigin.IsActive)
            throw new InvalidOperationException($"App-owned rule '{ruleId}' written outside system origin.");
    }
}
