using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three reductions made to the authorization <b>policy compile</b>
/// path - the whole-ruleset rebuild <c>CompiledPolicySnapshotMaintainer</c>
/// performs every time the policy tree changes and on its periodic refresh
/// cadence, over every rule in the store.
/// <para>
/// The complementary <see cref="AuthDecisionBenchmarks"/> suite measures the warm
/// per-request <i>decision</i>; this one measures the rebuild that produces the
/// snapshot that decision reads. Neither touches a silo, so both are cheap to run
/// at <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// <para>
/// The three edits under test sit on private members of <c>CompiledTree</c>
/// (<c>Append</c>, <c>Build</c>'s prefix materialisation, and <c>Freeze</c>)
/// reached only through <c>CompiledPolicy.Compile</c>, and <c>CompiledTree</c>'s
/// constructor is private, so neither lane can call the production method and
/// still expose the shape it built. Both lanes therefore reproduce the
/// <b>same</b> surrounding shell - identical inputs, identical per-rule
/// projection into <see cref="CompiledRule"/>, identical tree bucketing,
/// identical frozen output - and differ only in the bodies under test. That
/// symmetry is the point: a baseline arm that skips part of the optimized arm's
/// shell fabricates a regression. <see cref="Compile_Production"/> pins the lanes
/// to reality by running the real shipped <see cref="CompiledPolicy.Compile"/>
/// over the same ruleset.
/// </para>
/// <para>
/// The pairs mirror the production edits:
/// (1) <c>Append</c> bucketed into <see cref="List{T}"/> values, probing with
/// <c>TryGetValue</c> and storing through the indexer on a miss. That cost three
/// allocations for every scope key - the list, the four-slot backing array its
/// first <c>Add</c> grows, and the <c>ToArray</c> copy taken at freeze time -
/// where the overwhelmingly common scope carries exactly one rule, plus a second
/// hash of the scope string per rule. The replacement holds buckets as arrays and
/// folds the probe pair onto a single
/// <see cref="CollectionsMarshal.GetValueRefOrAddDefault{TKey, TValue}"/> slot;
/// (2) the prefix materialisation in <c>Build</c> sorted the key array alone and
/// then hashed every key back through the builder to fetch its bucket - a full
/// dictionary lookup per prefix that the enumeration already had in hand. The
/// replacement fills the key and rule arrays in one pass and sorts them together
/// as parallel arrays;
/// (3) <c>Freeze</c> filled an intermediate <see cref="Dictionary{TKey, TValue}"/>
/// purely to change the bucket type from list to array, then threw it away the
/// instant <c>ToFrozenDictionary</c> copied the entries back out. With buckets
/// already in array form the builder freezes directly, which also keeps the
/// <see cref="Dictionary{TKey, TValue}"/>-source fast path.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=authcompiletrims</c> (or
/// <c>--suite authcompiletrims</c>); see <c>Program.cs</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class AuthPolicyCompileTrimBenchmarks
{
    // A policy shaped like a real multi-tenant estate: every governed tree
    // carries a tree-wide rule plus a fan of exact-key and prefix rules, so all
    // three CompiledTree buckets are populated and the per-key bucket overhead is
    // paid across a realistic key cardinality rather than a toy one.
    private const int TreeCount = 24;
    private const int ExactRulesPerTree = 64;
    private const int PrefixRulesPerTree = 32;

    private const LatticeOperation AllOperations =
        LatticeOperation.Read | LatticeOperation.Write | LatticeOperation.Delete |
        LatticeOperation.RangeRead | LatticeOperation.RangeDelete | LatticeOperation.CrdtApply |
        LatticeOperation.AtomicWrite | LatticeOperation.BulkLoad | LatticeOperation.Admin;

    private List<LatticeAuthorizationRule> _rules = null!;

    // The bucket builders the (2) and (3) lanes consume, in the form each design
    // actually produces, prepared once so those lanes measure only the
    // materialisation / freeze body and not the bucketing that precedes it.
    private Dictionary<string, List<CompiledRule>> _exactListBuilder = null!;
    private Dictionary<string, List<CompiledRule>> _prefixListBuilder = null!;
    private Dictionary<string, CompiledRule[]> _exactArrayBuilder = null!;
    private Dictionary<string, CompiledRule[]> _prefixArrayBuilder = null!;

    [GlobalSetup]
    public void Setup()
    {
        _rules = new List<LatticeAuthorizationRule>(TreeCount * (1 + ExactRulesPerTree + PrefixRulesPerTree));

        for (var t = 0; t < TreeCount; t++)
        {
            var treeId = $"tree-{t:D3}";

            // A tree-wide grant per tree, subject-shared across trees so the
            // distinct-subject fold is exercised with real duplication.
            _rules.Add(new LatticeAuthorizationRule(
                $"{treeId}-tree-allow",
                LatticeSubjectSelector.User($"user-{t % 8}"),
                LatticeScope.Tree(treeId),
                AllOperations,
                LatticeEffect.Allow));

            for (var k = 0; k < ExactRulesPerTree; k++)
            {
                _rules.Add(new LatticeAuthorizationRule(
                    $"{treeId}-key-{k:D3}",
                    LatticeSubjectSelector.User($"user-{k % 16}"),
                    LatticeScope.Key(treeId, $"row/{k:D6}/payload"),
                    AllOperations,
                    k % 7 == 0 ? LatticeEffect.Deny : LatticeEffect.Allow));
            }

            for (var p = 0; p < PrefixRulesPerTree; p++)
            {
                _rules.Add(new LatticeAuthorizationRule(
                    $"{treeId}-prefix-{p:D3}",
                    LatticeSubjectSelector.Group($"group-{p % 12}"),
                    LatticeScope.Prefix(treeId, $"row/{p:D6}/"),
                    AllOperations,
                    LatticeEffect.Allow));
            }
        }

        // The same buckets in both forms, so the materialise and freeze lanes
        // start from input that is entry-for-entry equivalent.
        _exactListBuilder = new Dictionary<string, List<CompiledRule>>(StringComparer.Ordinal);
        _prefixListBuilder = new Dictionary<string, List<CompiledRule>>(StringComparer.Ordinal);
        _exactArrayBuilder = new Dictionary<string, CompiledRule[]>(StringComparer.Ordinal);
        _prefixArrayBuilder = new Dictionary<string, CompiledRule[]>(StringComparer.Ordinal);

        foreach (var rule in _rules)
        {
            var compiled = Project(rule);
            var key = BucketKey(rule);
            switch (rule.Scope.Kind)
            {
                case LatticeScopeKind.Key:
                    AppendListBaseline(_exactListBuilder, key, compiled);
                    AppendArrayOptimized(_exactArrayBuilder, key, compiled);
                    break;
                case LatticeScopeKind.Prefix:
                    AppendListBaseline(_prefixListBuilder, key, compiled);
                    AppendArrayOptimized(_prefixArrayBuilder, key, compiled);
                    break;
                default:
                    break;
            }
        }
    }

    // ---- whole-compile A/B: the aggregate of all three edits ----------------

    /// <summary>The full rebuild as it stood before the change.</summary>
    [Benchmark(Baseline = true, Description = "Policy compile (before: list buckets, re-lookup, intermediate map)")]
    public int Compile_Baseline() => CompileShell(_rules, optimized: false).Count;

    /// <summary>The full rebuild with all three bodies replaced.</summary>
    [Benchmark(Description = "Policy compile (after: array buckets, paired sort, direct freeze)")]
    public int Compile_Optimized() => CompileShell(_rules, optimized: true).Count;

    /// <summary>
    /// The real shipped <see cref="CompiledPolicy.Compile"/> over the same
    /// ruleset. Not an A/B arm - it pins the copied shell above to the production
    /// path so a drift between them is visible rather than silent.
    /// </summary>
    [Benchmark(Description = "Policy compile (production CompiledPolicy.Compile)")]
    public int Compile_Production() => CompiledPolicy.Compile(_rules).TreeCount;

    // ---- (1) per-key bucketing: list bucket + double probe vs array slot ----

    /// <summary>Bucketing into lists, probing with TryGetValue then storing through the indexer.</summary>
    [Benchmark(Description = "Bucket rules (before: List bucket, TryGetValue + indexer store)")]
    public int Bucket_Baseline()
    {
        var builder = new Dictionary<string, List<CompiledRule>>(StringComparer.Ordinal);
        var rules = _rules;
        for (var i = 0; i < rules.Count; i++)
        {
            AppendListBaseline(builder, BucketKey(rules[i]), Project(rules[i]));
        }

        return builder.Count;
    }

    /// <summary>Bucketing into arrays through one GetValueRefOrAddDefault slot.</summary>
    [Benchmark(Description = "Bucket rules (after: array bucket, GetValueRefOrAddDefault fold)")]
    public int Bucket_Optimized()
    {
        var builder = new Dictionary<string, CompiledRule[]>(StringComparer.Ordinal);
        var rules = _rules;
        for (var i = 0; i < rules.Count; i++)
        {
            AppendArrayOptimized(builder, BucketKey(rules[i]), Project(rules[i]));
        }

        return builder.Count;
    }

    // ---- (2) prefix materialisation: re-lookup vs paired sort ---------------

    /// <summary>Sorting the keys alone, then hashing each one back through the builder.</summary>
    [Benchmark(Description = "Prefix index (before: sort keys, re-lookup each bucket)")]
    public int PrefixIndex_Baseline() => MaterialisePrefixesBaseline(_prefixListBuilder).Prefixes.Length;

    /// <summary>Filling both arrays in one pass and sorting them together.</summary>
    [Benchmark(Description = "Prefix index (after: one pass, paired sort)")]
    public int PrefixIndex_Optimized() => MaterialisePrefixesOptimized(_prefixArrayBuilder).Prefixes.Length;

    // ---- (3) freeze: intermediate dictionary vs direct freeze ---------------

    /// <summary>Freezing through a throwaway intermediate Dictionary and a per-bucket copy.</summary>
    [Benchmark(Description = "Freeze exact map (before: intermediate Dictionary + per-bucket ToArray)")]
    public int Freeze_Baseline() => FreezeBaseline(_exactListBuilder).Count;

    /// <summary>Freezing the array-valued builder directly.</summary>
    [Benchmark(Description = "Freeze exact map (after: direct freeze of array buckets)")]
    public int Freeze_Optimized() => FreezeOptimized(_exactArrayBuilder).Count;

    // ---- the shared shell both compile lanes run ---------------------------

    /// <summary>
    /// A faithful copy of <c>CompiledPolicy.Compile</c> plus
    /// <c>CompiledTree.Build</c>, parameterised by which set of bodies to run.
    /// Everything outside the three bodies under test is identical between the
    /// lanes, so the delta is exactly the work the production change removes.
    /// </summary>
    private static Dictionary<string, TreeShape> CompileShell(
        IEnumerable<LatticeAuthorizationRule> rules,
        bool optimized)
    {
        var byTree = new Dictionary<string, List<LatticeAuthorizationRule>>(StringComparer.Ordinal);
        var distinctSubjects = new HashSet<(LatticeSubjectSelectorKind Kind, string Id)>();

        foreach (var rule in rules)
        {
            distinctSubjects.Add((rule.Subject.Kind, rule.Subject.Id));

            var treeId = rule.Scope.TreeId;
            if (!byTree.TryGetValue(treeId, out var list))
            {
                list = new List<LatticeAuthorizationRule>();
                byTree[treeId] = list;
            }

            list.Add(rule);
        }

        var trees = new Dictionary<string, TreeShape>(byTree.Count, StringComparer.Ordinal);
        foreach (var (treeId, treeRules) in byTree)
        {
            trees[treeId] = BuildShape(treeRules, optimized);
        }

        // Consumed exactly as production consumes it, so neither lane can have
        // the set folded away by the JIT while the other pays for it.
        if (distinctSubjects.Count < 0)
        {
            throw new InvalidOperationException("unreachable");
        }

        return trees;
    }

    private static TreeShape BuildShape(IReadOnlyList<LatticeAuthorizationRule> rules, bool optimized) =>
        optimized ? BuildShapeOptimized(rules) : BuildShapeBaseline(rules);

    private static TreeShape BuildShapeBaseline(IReadOnlyList<LatticeAuthorizationRule> rules)
    {
        Dictionary<string, List<CompiledRule>>? exactBuilder = null;
        Dictionary<string, List<CompiledRule>>? prefixBuilder = null;
        List<CompiledRule>? treeBuilder = null;

        foreach (var rule in rules)
        {
            var compiled = Project(rule);
            switch (rule.Scope.Kind)
            {
                case LatticeScopeKind.Key:
                    exactBuilder ??= new Dictionary<string, List<CompiledRule>>(StringComparer.Ordinal);
                    AppendListBaseline(exactBuilder, rule.Scope.KeyOrPrefix!, compiled);
                    break;
                case LatticeScopeKind.Prefix:
                    prefixBuilder ??= new Dictionary<string, List<CompiledRule>>(StringComparer.Ordinal);
                    AppendListBaseline(prefixBuilder, rule.Scope.KeyOrPrefix!, compiled);
                    break;
                default:
                    treeBuilder ??= new List<CompiledRule>();
                    treeBuilder.Add(compiled);
                    break;
            }
        }

        var exact = exactBuilder is null
            ? FrozenDictionary<string, CompiledRule[]>.Empty
            : FreezeBaseline(exactBuilder);

        var (prefixes, prefixRules) = prefixBuilder is null
            ? (Array.Empty<string>(), Array.Empty<CompiledRule[]>())
            : MaterialisePrefixesBaseline(prefixBuilder);

        var treeRules = treeBuilder is null ? Array.Empty<CompiledRule>() : treeBuilder.ToArray();
        return new TreeShape(exact, prefixes, prefixRules, treeRules);
    }

    private static TreeShape BuildShapeOptimized(IReadOnlyList<LatticeAuthorizationRule> rules)
    {
        Dictionary<string, CompiledRule[]>? exactBuilder = null;
        Dictionary<string, CompiledRule[]>? prefixBuilder = null;
        List<CompiledRule>? treeBuilder = null;

        foreach (var rule in rules)
        {
            var compiled = Project(rule);
            switch (rule.Scope.Kind)
            {
                case LatticeScopeKind.Key:
                    exactBuilder ??= new Dictionary<string, CompiledRule[]>(StringComparer.Ordinal);
                    AppendArrayOptimized(exactBuilder, rule.Scope.KeyOrPrefix!, compiled);
                    break;
                case LatticeScopeKind.Prefix:
                    prefixBuilder ??= new Dictionary<string, CompiledRule[]>(StringComparer.Ordinal);
                    AppendArrayOptimized(prefixBuilder, rule.Scope.KeyOrPrefix!, compiled);
                    break;
                default:
                    treeBuilder ??= new List<CompiledRule>();
                    treeBuilder.Add(compiled);
                    break;
            }
        }

        var exact = exactBuilder is null
            ? FrozenDictionary<string, CompiledRule[]>.Empty
            : FreezeOptimized(exactBuilder);

        var (prefixes, prefixRules) = prefixBuilder is null
            ? (Array.Empty<string>(), Array.Empty<CompiledRule[]>())
            : MaterialisePrefixesOptimized(prefixBuilder);

        var treeRules = treeBuilder is null ? Array.Empty<CompiledRule>() : treeBuilder.ToArray();
        return new TreeShape(exact, prefixes, prefixRules, treeRules);
    }

    private static CompiledRule Project(LatticeAuthorizationRule rule) =>
        new(rule.RuleId, rule.Subject.Kind, rule.Subject.Id, rule.Operations, rule.Effect);

    // Production buckets per tree, so a scope key is unique within its builder.
    // The standalone bucketing lanes share one builder across every tree, so they
    // qualify the key with the tree id to keep that same one-rule-per-key shape.
    private static string BucketKey(LatticeAuthorizationRule rule) =>
        string.Concat(rule.Scope.TreeId, "|", rule.Scope.KeyOrPrefix ?? string.Empty);

    // ---- the bodies under test, before and after ---------------------------

    private static void AppendListBaseline(Dictionary<string, List<CompiledRule>> builder, string key, CompiledRule rule)
    {
        if (!builder.TryGetValue(key, out var list))
        {
            list = new List<CompiledRule>();
            builder[key] = list;
        }

        list.Add(rule);
    }

    private static void AppendArrayOptimized(Dictionary<string, CompiledRule[]> builder, string key, CompiledRule rule)
    {
        ref var bucket = ref CollectionsMarshal.GetValueRefOrAddDefault(builder, key, out var existed);
        if (!existed)
        {
            bucket = [rule];
            return;
        }

        Array.Resize(ref bucket, bucket!.Length + 1);
        bucket[^1] = rule;
    }

    private static (string[] Prefixes, CompiledRule[][] Rules) MaterialisePrefixesBaseline(
        Dictionary<string, List<CompiledRule>> builder)
    {
        var prefixes = builder.Keys.ToArray();
        Array.Sort(prefixes, StringComparer.Ordinal);
        var rules = new CompiledRule[prefixes.Length][];
        for (var i = 0; i < prefixes.Length; i++)
        {
            rules[i] = builder[prefixes[i]].ToArray();
        }

        return (prefixes, rules);
    }

    private static (string[] Prefixes, CompiledRule[][] Rules) MaterialisePrefixesOptimized(
        Dictionary<string, CompiledRule[]> builder)
    {
        var prefixes = new string[builder.Count];
        var rules = new CompiledRule[prefixes.Length][];
        var next = 0;
        foreach (var (prefix, bucket) in builder)
        {
            prefixes[next] = prefix;
            rules[next] = bucket;
            next++;
        }

        Array.Sort(prefixes, rules, StringComparer.Ordinal);
        return (prefixes, rules);
    }

    private static FrozenDictionary<string, CompiledRule[]> FreezeBaseline(
        Dictionary<string, List<CompiledRule>> builder)
    {
        var frozen = new Dictionary<string, CompiledRule[]>(builder.Count, StringComparer.Ordinal);
        foreach (var (key, list) in builder)
        {
            frozen[key] = list.ToArray();
        }

        return frozen.ToFrozenDictionary(StringComparer.Ordinal);
    }

    private static FrozenDictionary<string, CompiledRule[]> FreezeOptimized(
        Dictionary<string, CompiledRule[]> builder) =>
        builder.ToFrozenDictionary(StringComparer.Ordinal);

    /// <summary>
    /// The shape <c>CompiledTree</c> builds, carried as a plain record so both
    /// lanes can materialise it. <c>CompiledTree</c>'s own constructor is
    /// private, so neither lane could otherwise expose what it produced.
    /// </summary>
    private sealed record TreeShape(
        FrozenDictionary<string, CompiledRule[]> Exact,
        string[] Prefixes,
        CompiledRule[][] PrefixRules,
        CompiledRule[] TreeRules);
}
