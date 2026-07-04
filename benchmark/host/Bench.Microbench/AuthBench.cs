using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Wires the opt-in authorization layer into the microbench harness so the
/// per-operation enforcement cost can be measured against the disabled baseline.
///
/// The auth layer is opt-in: with Membership + Auth not registered (the default
/// harness config) the grain's service provider returns <c>null</c> for
/// <c>ILatticeAccessGate</c> and the enforcement path short-circuits with no
/// subject resolution, so "disabled" is byte-for-byte the pre-feature baseline.
/// When <c>BENCH_MICROBENCH_AUTH=enforcing</c> is set, every LatticeGrain the
/// harness constructs is handed a service provider that resolves a real
/// <see cref="PolicyAccessGate"/> (built from the internal decision engine,
/// snapshot maintainer and decision observer) plus a fixed-subject membership
/// context, so each measured operation pays the real gate cost: subject
/// resolution, compiled-snapshot lookup and rule evaluation under a default-deny
/// policy with a representative tree/key/prefix allow ruleset.
/// </summary>
internal static class AuthBench
{
    /// <summary>The subject id the benchmarked caller resolves to when enforcing.</summary>
    internal const string BenchSubjectId = "bench-subject";

    /// <summary>
    /// Every tree id the harness constructs a LatticeGrain for. The enforcing
    /// ruleset grants the bench subject a tree-scope allow on each so no measured
    /// operation is ever denied (a denial would throw and abort the run).
    /// </summary>
    private static readonly string[] TreeIds =
    [
        "microbench-tree",
        "microbench-crdt-writer-tree",
        "microbench-crdt-receiver-batch-tree",
        "microbench-fanout",
        "microbench-deep",
        "microbench-deeper",
        "microbench-atomic",
        "microbench-atomic-fanout",
        "microbench-xtree-a",
        "microbench-xtree-b",
    ];

    // Every operation the benchmarked subject may perform. Granting the full set
    // keeps every workload green while still exercising the gate's full
    // resolve -> lookup -> evaluate path per operation.
    private const LatticeOperation AllOperations =
        LatticeOperation.Read | LatticeOperation.Write | LatticeOperation.Delete |
        LatticeOperation.RangeRead | LatticeOperation.RangeDelete | LatticeOperation.CrdtApply |
        LatticeOperation.AtomicWrite | LatticeOperation.BulkLoad | LatticeOperation.Admin;

    private static readonly object Gate = new();
    private static ILatticeAccessGate? _gate;
    private static ILatticeMembershipContext? _membership;

    /// <summary>
    /// <c>true</c> when the harness was launched with
    /// <c>BENCH_MICROBENCH_AUTH=enforcing</c>, so the gate is enforced on every
    /// measured operation. Any other value (including unset) leaves the gate
    /// unregistered and measures the disabled baseline.
    /// </summary>
    internal static bool Enabled { get; } =
        string.Equals(
            Environment.GetEnvironmentVariable("BENCH_MICROBENCH_AUTH"),
            "enforcing",
            StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Builds the service provider handed to a LatticeGrain constructed by the
    /// harness. When the auth layer is disabled this is a bare substitute
    /// (unresolved services return <c>null</c>, exactly as the pre-feature
    /// baseline). When enforcing, it resolves the shared gate and membership
    /// context so the grain drives the real enforcement path.
    /// </summary>
    internal static IServiceProvider CreateServiceProvider()
    {
        var provider = Substitute.For<IServiceProvider>();
        if (!Enabled)
        {
            return provider;
        }

        EnsureBuilt();
        provider.GetService(typeof(ILatticeAccessGate)).Returns(_gate);
        provider.GetService(typeof(ILatticeMembershipContext)).Returns(_membership);
        return provider;
    }

    private static void EnsureBuilt()
    {
        if (_gate is not null)
        {
            return;
        }

        lock (Gate)
        {
            if (_gate is not null)
            {
                return;
            }

            var authOptions = new LatticeAuthOptions
            {
                // Default-deny: only the explicit allow rules below grant access.
                DefaultEffect = LatticeEffect.Deny,
            };
            var options = Substitute.For<IOptionsMonitor<LatticeAuthOptions>>();
            options.CurrentValue.Returns(authOptions);
            options.Get(Arg.Any<string>()).Returns(authOptions);

            var store = new BenchPolicyStore(BuildRules());
            var maintainer = new CompiledPolicySnapshotMaintainer(
                store,
                NullLogger<CompiledPolicySnapshotMaintainer>.Instance,
                TimeProvider.System);
            // Compile the ruleset into the snapshot before any benchmark runs so
            // the measured path only pays the read-side lookup + evaluation cost.
            maintainer.RebuildNowAsync().GetAwaiter().GetResult();

            var engine = new LatticeDecisionEngine(maintainer, options);
            var observer = new LatticeAuthDecisionObserver(
                Array.Empty<ILatticeAuthAuditSink>(),
                options,
                NullLogger<LatticeAuthDecisionObserver>.Instance);

            _membership = new FixedSubjectMembershipContext(
                new LatticeSubject(BenchSubjectId));
            _gate = new PolicyAccessGate(engine, maintainer, observer, options);
        }
    }

    /// <summary>
    /// A default-deny ruleset granting the bench subject access on every tree.
    /// Every tree carries a tree-scope allow so no operation is denied; the
    /// primary tree additionally carries a prefix-scope and an exact-key allow so
    /// the compiled snapshot is representative of the three scope tiers and the
    /// per-key range-filter path is exercised (both allow the full operation set,
    /// so the more-specific tiers never narrow the grant and nothing is denied).
    /// </summary>
    private static IReadOnlyList<LatticeAuthorizationRule> BuildRules()
    {
        var subject = LatticeSubjectSelector.User(BenchSubjectId);
        var rules = new List<LatticeAuthorizationRule>();
        foreach (var treeId in TreeIds)
        {
            rules.Add(new LatticeAuthorizationRule(
                $"{treeId}-tree-allow",
                subject,
                LatticeScope.Tree(treeId),
                AllOperations,
                LatticeEffect.Allow));
        }

        // Representative key/prefix grants on the primary tree. "k-" covers the
        // seeded keyspace so range reads flow through the per-key filter path.
        rules.Add(new LatticeAuthorizationRule(
            "microbench-tree-prefix-allow",
            subject,
            LatticeScope.Prefix("microbench-tree", "k-"),
            AllOperations,
            LatticeEffect.Allow));
        rules.Add(new LatticeAuthorizationRule(
            "microbench-tree-key-allow",
            subject,
            LatticeScope.Key("microbench-tree", "k-00000000"),
            AllOperations,
            LatticeEffect.Allow));

        return rules;
    }

    /// <summary>
    /// Membership context that resolves every caller to a single fixed subject,
    /// so the benchmarked operation always evaluates against the same identity.
    /// </summary>
    private sealed class FixedSubjectMembershipContext(LatticeSubject subject) : ILatticeMembershipContext
    {
        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default) =>
            new(subject);
    }

    /// <summary>
    /// Minimal in-memory policy store: only <see cref="ListRulesAsync"/> is
    /// exercised (by the snapshot maintainer's rebuild). The mutating members are
    /// never called by the benchmark and throw if they are.
    /// </summary>
    private sealed class BenchPolicyStore(IReadOnlyList<LatticeAuthorizationRule> rules)
        : ILatticeAuthorizationPolicyStore
    {
        public Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<bool> RemoveRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public async IAsyncEnumerable<LatticeAuthorizationRule> ListRulesForTreeAsync(
            string treeId,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var rule in rules)
            {
                if (string.Equals(rule.Scope.TreeId, treeId, StringComparison.Ordinal))
                {
                    yield return rule;
                }
            }

            await Task.CompletedTask.ConfigureAwait(false);
        }

        public async IAsyncEnumerable<LatticeAuthorizationRule> ListRulesAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var rule in rules)
            {
                yield return rule;
            }

            await Task.CompletedTask.ConfigureAwait(false);
        }
    }
}
