using BenchmarkDotNet.Attributes;
using Orleans.Lattice;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the warm authorization <b>decision</b> hot path - the pure,
/// synchronous, in-memory evaluation the enforcement gate pays on every gated
/// operation once the compiled snapshot is warm. It drives
/// <see cref="PolicyEvaluator.Evaluate(CompiledPolicy, LatticeAuthOptions, in LatticeSubject, string, LatticeOperation, string?, string?, string?)"/>
/// directly against a pre-compiled <see cref="CompiledPolicy"/> (reached via
/// <c>InternalsVisibleTo</c>), so the measured cost is exactly the per-decision
/// tree lookup plus tiered <see cref="CompiledTree.ResolvePoint"/> resolution -
/// no Orleans silo, no grain dispatch, no gate/observer noise.
/// <para>
/// The ruleset mirrors <see cref="AuthBench"/>: ten governed trees each carrying
/// a tree-wide allow, and the primary tree additionally carrying a prefix-scope
/// and an exact-key allow, so the three scope tiers (exact, prefix, tree-wide)
/// are all exercised. The three per-tier benchmarks each measure a single
/// <see cref="PolicyEvaluator.Evaluate"/> so the per-decision cost is attributed
/// to the tier it resolves at; the <see cref="Decide_Mixed"/> workload folds a
/// representative batch into one invocation so the aggregate cost is measured
/// with a tight relative confidence interval.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=authdecision</c> (see <c>Program.cs</c>).
/// This suite has no Orleans dependency, so it is fast to run at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c> for gold-standard timing rigour.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class AuthDecisionBenchmarks
{
    private const string PrimaryTree = "microbench-tree";
    private const string TreeWideTree = "microbench-fanout";
    private const string ExactKey = "k-00000000";
    private const string PrefixKey = "k-00000001";
    private const string TreeWideKey = "row-42";

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

    private const LatticeOperation AllOperations =
        LatticeOperation.Read | LatticeOperation.Write | LatticeOperation.Delete |
        LatticeOperation.RangeRead | LatticeOperation.RangeDelete | LatticeOperation.CrdtApply |
        LatticeOperation.AtomicWrite | LatticeOperation.BulkLoad | LatticeOperation.Admin;

    private CompiledPolicy _policy = null!;
    private LatticeAuthOptions _options = null!;
    private LatticeSubject _subject;

    [GlobalSetup]
    public void Setup()
    {
        _options = new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny };
        _subject = new LatticeSubject("bench-subject");

        var subject = LatticeSubjectSelector.User("bench-subject");
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

        rules.Add(new LatticeAuthorizationRule(
            "microbench-tree-prefix-allow",
            subject,
            LatticeScope.Prefix(PrimaryTree, "k-"),
            AllOperations,
            LatticeEffect.Allow));
        rules.Add(new LatticeAuthorizationRule(
            "microbench-tree-key-allow",
            subject,
            LatticeScope.Key(PrimaryTree, ExactKey),
            AllOperations,
            LatticeEffect.Allow));

        _policy = CompiledPolicy.Compile(rules);
    }

    /// <summary>Point decision resolving at the most specific (exact-key) tier.</summary>
    [Benchmark(Description = "Auth decide (exact-key tier)")]
    public LatticeAccessDecision Decide_ExactKey() =>
        PolicyEvaluator.Evaluate(_policy, _options, _subject, PrimaryTree, LatticeOperation.Read, ExactKey, null, null);

    /// <summary>Point decision that misses the exact tier and resolves at the prefix tier.</summary>
    [Benchmark(Description = "Auth decide (prefix tier)")]
    public LatticeAccessDecision Decide_PrefixKey() =>
        PolicyEvaluator.Evaluate(_policy, _options, _subject, PrimaryTree, LatticeOperation.Read, PrefixKey, null, null);

    /// <summary>Point decision on a tree with only a tree-wide rule (least specific tier).</summary>
    [Benchmark(Description = "Auth decide (tree-wide tier)")]
    public LatticeAccessDecision Decide_TreeWide() =>
        PolicyEvaluator.Evaluate(_policy, _options, _subject, TreeWideTree, LatticeOperation.Read, TreeWideKey, null, null);

    /// <summary>
    /// A representative batch of decisions folded into one invocation: the exact,
    /// prefix, and tree-wide tiers on the primary tree plus a tree-wide decision
    /// on every other governed tree, so the per-decision tree lookup is exercised
    /// across the full ten-tree snapshot. Amplifies the aggregate signal so the
    /// timing delta carries a tight relative confidence interval.
    /// </summary>
    [Benchmark(Description = "Auth decide (mixed batch)")]
    public int Decide_Mixed()
    {
        var allowed = 0;
        if (PolicyEvaluator.Evaluate(_policy, _options, _subject, PrimaryTree, LatticeOperation.Read, ExactKey, null, null).Allowed) allowed++;
        if (PolicyEvaluator.Evaluate(_policy, _options, _subject, PrimaryTree, LatticeOperation.Write, PrefixKey, null, null).Allowed) allowed++;
        var trees = TreeIds;
        for (var i = 0; i < trees.Length; i++)
        {
            if (PolicyEvaluator.Evaluate(_policy, _options, _subject, trees[i], LatticeOperation.Read, TreeWideKey, null, null).Allowed) allowed++;
        }

        return allowed;
    }
}
