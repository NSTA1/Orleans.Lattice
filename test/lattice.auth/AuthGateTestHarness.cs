using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// A minimal <see cref="IOptionsMonitor{TOptions}"/> that returns a fixed value,
/// for unit tests that construct auth components directly (no DI container). Named
/// distinctly from the private stubs in sibling test files so it can be shared
/// across the coverage test fixtures without collision.
/// </summary>
internal sealed class CovOptionsMonitor<T>(T value) : IOptionsMonitor<T>
{
    public T CurrentValue { get; } = value;

    public T Get(string? name) => CurrentValue;

    public IDisposable? OnChange(Action<T, string?> listener) => null;
}

/// <summary>
/// An in-memory <see cref="ILatticeAuthorizationPolicyStore"/> whose rules can be
/// pre-seeded. Only <see cref="ListRulesAsync"/> is consulted by the snapshot
/// maintainer, but the whole interface is implemented so the store can stand in
/// anywhere one is required.
/// </summary>
internal sealed class CovPolicyStore : ILatticeAuthorizationPolicyStore
{
    public List<LatticeAuthorizationRule> Rules { get; } = new();

    public Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default)
    {
        Rules.Add(rule);
        return Task.CompletedTask;
    }

    public Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default) =>
        Task.FromResult<LatticeAuthorizationRule?>(null);

    public Task<bool> RemoveRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default) =>
        Task.FromResult(false);

    public async IAsyncEnumerable<LatticeAuthorizationRule> ListRulesForTreeAsync(
        string treeId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var rule in Rules.ToArray())
        {
            if (string.Equals(rule.Scope.TreeId, treeId, StringComparison.Ordinal))
            {
                yield return rule;
            }
        }

        await Task.CompletedTask;
    }

    public async IAsyncEnumerable<LatticeAuthorizationRule> ListRulesAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var rule in Rules.ToArray())
        {
            yield return rule;
        }

        await Task.CompletedTask;
    }
}

/// <summary>
/// Builds a warmed <see cref="PolicyAccessGate"/> (plus the engine and maintainer
/// it wraps) over an in-memory policy store, so the enforcement paths can be
/// exercised as direct in-process unit tests without standing up an Orleans
/// cluster.
/// </summary>
internal static class AuthGateHarness
{
    internal sealed record Harness(
        PolicyAccessGate Gate,
        LatticeDecisionEngine Engine,
        CompiledPolicySnapshotMaintainer Maintainer,
        LatticeAuthOptions Options);

    /// <summary>
    /// Creates a harness whose compiled snapshot has already been built once
    /// (epoch &gt; 0), so the gate takes its synchronous warm path.
    /// </summary>
    public static async Task<Harness> CreateAsync(
        LatticeAuthOptions options,
        params LatticeAuthorizationRule[] rules)
    {
        var store = new CovPolicyStore();
        store.Rules.AddRange(rules);
        var optionsMonitor = new CovOptionsMonitor<LatticeAuthOptions>(options);
        var maintainer = new CompiledPolicySnapshotMaintainer(
            store, NullLogger<CompiledPolicySnapshotMaintainer>.Instance);
        await maintainer.RebuildNowAsync();
        var engine = new LatticeDecisionEngine(maintainer, optionsMonitor);
        var observer = new LatticeAuthDecisionObserver(
            Array.Empty<ILatticeAuthAuditSink>(),
            optionsMonitor,
            NullLogger<LatticeAuthDecisionObserver>.Instance);
        var gate = new PolicyAccessGate(engine, maintainer, observer, optionsMonitor);
        return new Harness(gate, engine, maintainer, options);
    }
}
