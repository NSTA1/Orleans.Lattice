using System.Collections.Frozen;
using System.Runtime.InteropServices;

namespace Orleans.Lattice.Auth;

/// <summary>
/// The compiled authorization rules governing a single tree, indexed for
/// deterministic, allocation-light point resolution. Rules are split into three
/// scope tiers: an exact-key map (most specific), a prefix index kept as a
/// sorted array for longest-prefix lookup, and a tree-wide list (least specific).
/// Built once by <see cref="CompiledPolicy.Compile"/> and thereafter immutable.
/// </summary>
/// <remarks>
/// In-process snapshot state: never serialized, never crosses a grain boundary.
/// Resolution walks the tiers from most to least specific and stops at the most
/// specific tier that has a matching rule, so a hit at a more specific scope can
/// never be overridden by a less specific one - the "most-specific scope wins"
/// rule of the decision algorithm.
/// </remarks>
internal sealed class CompiledTree
{
    private readonly FrozenDictionary<string, CompiledRule[]> _exact;

    // Parallel arrays: _prefixes is sorted ascending (ordinal); _prefixRules[i]
    // holds the rules scoped at _prefixes[i]. Sorting lets us binary-search for
    // the longest prefix of a key (the largest stored prefix that the key starts
    // with) and then walk to shorter prefixes only if the longer tiers have no
    // matching rule.
    private readonly string[] _prefixes;
    private readonly CompiledRule[][] _prefixRules;
    private readonly CompiledRule[] _treeRules;

    private CompiledTree(
        FrozenDictionary<string, CompiledRule[]> exact,
        string[] prefixes,
        CompiledRule[][] prefixRules,
        CompiledRule[] treeRules)
    {
        _exact = exact;
        _prefixes = prefixes;
        _prefixRules = prefixRules;
        _treeRules = treeRules;
    }

    /// <summary>
    /// <c>true</c> when the tree carries any exact-key or prefix rule, so a
    /// collection (range) request can vary its decision key-by-key. When
    /// <c>false</c>, only tree-wide rules apply and a collection decision is
    /// uniform.
    /// </summary>
    public bool HasPerKeyRules => _exact.Count > 0 || _prefixes.Length > 0;

    /// <summary>
    /// Resolves the winning rule for a point request, or a non-matched result
    /// when no rule applies.
    /// </summary>
    /// <param name="subject">The requesting subject (its group closure is treated as a flat set).</param>
    /// <param name="operation">The requested operation.</param>
    /// <param name="key">
    /// The exact key, or <c>null</c> to consult only tree-wide rules (a
    /// whole-tree / collection decision).
    /// </param>
    /// <param name="userRuleBeatsGroupRule">
    /// When <c>true</c>, a user-subject rule outranks a group-subject rule at the
    /// same scope tier.
    /// </param>
    /// <returns>The winning <see cref="PolicyMatch"/>, or a non-matched result.</returns>
    public PolicyMatch ResolvePoint(
        in LatticeSubject subject,
        LatticeOperation operation,
        string? key,
        bool userRuleBeatsGroupRule)
    {
        if (key is not null)
        {
            // Tier 1: exact key (most specific).
            if (_exact.TryGetValue(key, out var exactRules)
                && TryBestInBucket(exactRules, subject, operation, userRuleBeatsGroupRule, out var exactWinner))
            {
                return new PolicyMatch(exactWinner.Effect, exactWinner.RuleId, LatticeScopeKind.Key, key);
            }

            // Tier 2: prefixes of the key, longest first. The prefixes of a given
            // key are strictly nested and therefore sort in length order, so the
            // largest stored prefix the key starts with is the longest; walking
            // left from the binary-search position yields prefixes in descending
            // length, skipping stored prefixes the key does not start with.
            for (var i = LargestPrefixAtOrBelow(key); i >= 0; i--)
            {
                var prefix = _prefixes[i];
                if (!key.StartsWith(prefix, StringComparison.Ordinal))
                {
                    continue;
                }

                if (TryBestInBucket(_prefixRules[i], subject, operation, userRuleBeatsGroupRule, out var prefixWinner))
                {
                    return new PolicyMatch(prefixWinner.Effect, prefixWinner.RuleId, LatticeScopeKind.Prefix, prefix);
                }
            }
        }

        // Tier 3: tree-wide (least specific).
        if (TryBestInBucket(_treeRules, subject, operation, userRuleBeatsGroupRule, out var treeWinner))
        {
            return new PolicyMatch(treeWinner.Effect, treeWinner.RuleId, LatticeScopeKind.Tree, null);
        }

        return default;
    }

    /// <summary>
    /// Picks the most specific matching rule in a single scope tier. Within a
    /// tier the tie-break order is: a user rule outranks a group rule (when
    /// <paramref name="userRuleBeatsGroupRule"/>), then a deny outranks an allow.
    /// </summary>
    private static bool TryBestInBucket(
        CompiledRule[] rules,
        in LatticeSubject subject,
        LatticeOperation operation,
        bool userRuleBeatsGroupRule,
        out CompiledRule winner)
    {
        winner = default;
        var found = false;
        var bestScore = -1;

        // Walk by readonly reference so each ~40-byte CompiledRule struct is read
        // in place rather than copied into a loop variable every iteration; the
        // single copy into `winner` happens only when a better rule is found.
        var span = rules.AsSpan();
        for (var i = 0; i < span.Length; i++)
        {
            ref readonly var rule = ref span[i];
            if (!OperationMatches(rule.Operations, operation))
            {
                continue;
            }

            if (!SubjectMatches(subject, rule))
            {
                continue;
            }

            var subjectRank = userRuleBeatsGroupRule && rule.SubjectKind == LatticeSubjectSelectorKind.User ? 1 : 0;
            var effectRank = rule.Effect == LatticeEffect.Deny ? 1 : 0;
            var score = (subjectRank << 1) | effectRank;

            if (!found || score > bestScore)
            {
                winner = rule;
                bestScore = score;
                found = true;
            }
        }

        return found;
    }

    /// <summary>
    /// <c>true</c> when a rule's operation bitset includes the requested
    /// operation. The empty request (<see cref="LatticeOperation.None"/>) matches
    /// nothing.
    /// </summary>
    private static bool OperationMatches(LatticeOperation ruleOperations, LatticeOperation requested) =>
        requested != LatticeOperation.None && (ruleOperations & requested) == requested;

    /// <summary>
    /// <c>true</c> when a rule's subject selector matches the subject: a user
    /// selector matches the subject id; a group selector matches when the group
    /// is in the subject's (already transitively-expanded) group closure.
    /// </summary>
    private static bool SubjectMatches(in LatticeSubject subject, in CompiledRule rule)
    {
        if (rule.SubjectKind == LatticeSubjectSelectorKind.User)
        {
            return string.Equals(subject.SubjectId, rule.SubjectId, StringComparison.Ordinal);
        }

        // Group selector: membership test against the flat closure. Prefer the
        // O(1) set path when the closure is a set (the common case) and fall back
        // to an allocation-free linear scan otherwise.
        var groups = subject.GroupIds;
        if (groups is IReadOnlySet<string> set)
        {
            return set.Contains(rule.SubjectId);
        }

        foreach (var group in groups)
        {
            if (string.Equals(group, rule.SubjectId, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Returns the index of the largest stored prefix that is less than or equal
    /// to <paramref name="key"/> (ordinal), or <c>-1</c> when none. The longest
    /// prefix of the key, if stored, is at or below this index.
    /// </summary>
    private int LargestPrefixAtOrBelow(string key)
    {
        var lo = 0;
        var hi = _prefixes.Length - 1;
        var result = -1;
        while (lo <= hi)
        {
            var mid = lo + ((hi - lo) >> 1);
            if (string.CompareOrdinal(_prefixes[mid], key) <= 0)
            {
                result = mid;
                lo = mid + 1;
            }
            else
            {
                hi = mid - 1;
            }
        }

        return result;
    }

    /// <summary>
    /// <c>true</c> when <paramref name="subject"/> has at least one allow grant on
    /// this tree for <paramref name="operation"/> whose effective decision at its
    /// own scope resolves to allow - a whole-tree rule, or an exact / prefix rule
    /// that is not shadowed by a deny at the same or a more specific scope. This is
    /// the structural "can read at least one key" signal that existence-hiding
    /// needs: unlike a collection decision (which is allow-with-filter for any
    /// subject once the tree carries per-key rules), this distinguishes a subject
    /// that holds a partial grant from one that holds none. Walks every scope tier
    /// once; used only off the hot path (a visibility probe), never on a data-plane
    /// read.
    /// </summary>
    public bool HasAnyResolvedAllow(
        in LatticeSubject subject,
        LatticeOperation operation,
        bool userRuleBeatsGroupRule)
    {
        // Whole-tree (least specific) grant covers every key.
        var treeWide = ResolvePoint(subject, operation, key: null, userRuleBeatsGroupRule);
        if (treeWide.Matched && treeWide.Effect == LatticeEffect.Allow)
        {
            return true;
        }

        // Any exact-key grant whose decision at that key resolves to allow.
        foreach (var key in _exact.Keys)
        {
            var m = ResolvePoint(subject, operation, key, userRuleBeatsGroupRule);
            if (m.Matched && m.Effect == LatticeEffect.Allow)
            {
                return true;
            }
        }

        // Any prefix grant whose decision at the prefix boundary resolves to allow.
        foreach (var prefix in _prefixes)
        {
            var m = ResolvePoint(subject, operation, prefix, userRuleBeatsGroupRule);
            if (m.Matched && m.Effect == LatticeEffect.Allow)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Compiles the rules governing one tree into the tiered index. All supplied
    /// rules must share the same governed tree id.
    /// </summary>
    /// <param name="rules">The rules governing a single tree.</param>
    /// <returns>The compiled tree index.</returns>
    public static CompiledTree Build(IReadOnlyList<LatticeAuthorizationRule> rules)
    {
        // Buckets are held as arrays rather than List<CompiledRule> because the
        // overwhelmingly common scope carries exactly one rule. A list bucket
        // cost three allocations per scope key - the List, the four-slot backing
        // array its first Add grows, and the ToArray copy at freeze time - where
        // an array bucket costs exactly one. Appending to an array is O(k) per
        // rule, but k is the number of rules sharing one exact key or prefix,
        // which is one or two in any real policy.
        Dictionary<string, CompiledRule[]>? exactBuilder = null;
        Dictionary<string, CompiledRule[]>? prefixBuilder = null;
        List<CompiledRule>? treeBuilder = null;

        foreach (var rule in rules)
        {
            var compiled = new CompiledRule(rule.RuleId, rule.Subject.Kind, rule.Subject.Id, rule.Operations, rule.Effect);
            switch (rule.Scope.Kind)
            {
                case LatticeScopeKind.Key:
                    exactBuilder ??= new Dictionary<string, CompiledRule[]>(StringComparer.Ordinal);
                    Append(exactBuilder, rule.Scope.KeyOrPrefix!, compiled);
                    break;
                case LatticeScopeKind.Prefix:
                    prefixBuilder ??= new Dictionary<string, CompiledRule[]>(StringComparer.Ordinal);
                    Append(prefixBuilder, rule.Scope.KeyOrPrefix!, compiled);
                    break;
                default:
                    treeBuilder ??= new List<CompiledRule>();
                    treeBuilder.Add(compiled);
                    break;
            }
        }

        var exact = Freeze(exactBuilder);

        string[] prefixes;
        CompiledRule[][] prefixRules;
        if (prefixBuilder is null)
        {
            prefixes = Array.Empty<string>();
            prefixRules = Array.Empty<CompiledRule[]>();
        }
        else
        {
            // Materialise the prefix key and its rule array in the same pass and
            // sort the two arrays together. The previous shape sorted the keys
            // alone and then hashed every one of them back through the builder
            // to fetch its bucket - a full dictionary lookup per prefix that the
            // enumeration had already handed us for free.
            prefixes = new string[prefixBuilder.Count];
            prefixRules = new CompiledRule[prefixes.Length][];
            var next = 0;
            foreach (var (prefix, bucket) in prefixBuilder)
            {
                prefixes[next] = prefix;
                prefixRules[next] = bucket;
                next++;
            }

            // Keys are unique, so the ordinal ordering the longest-prefix binary
            // search depends on is total and the paired sort is deterministic.
            Array.Sort(prefixes, prefixRules, StringComparer.Ordinal);
        }

        var treeRules = treeBuilder is null ? Array.Empty<CompiledRule>() : treeBuilder.ToArray();
        return new CompiledTree(exact, prefixes, prefixRules, treeRules);
    }

    private static void Append(Dictionary<string, CompiledRule[]> builder, string key, CompiledRule rule)
    {
        // One hash per rule instead of two: `GetValueRefOrAddDefault` hands back
        // the slot it just probed, so the miss branch fills that slot directly
        // rather than re-hashing the key through the indexer. Nothing mutates
        // `builder` while the ref is live, so the ref stays valid.
        ref var bucket = ref CollectionsMarshal.GetValueRefOrAddDefault(builder, key, out var existed);
        if (!existed)
        {
            bucket = [rule];
            return;
        }

        // Append order is the rule declaration order the previous list bucket
        // preserved, which decides ties in TryBestInBucket.
        Array.Resize(ref bucket, bucket!.Length + 1);
        bucket[^1] = rule;
    }

    private static FrozenDictionary<string, CompiledRule[]> Freeze(Dictionary<string, CompiledRule[]>? builder)
    {
        // Buckets are already in their final array form, so the frozen map is
        // built straight from the builder. That both skips the per-bucket
        // ToArray copy this used to make and takes the Dictionary-source fast
        // path, where the selector overload would have re-projected every entry.
        return builder is null ? EmptyExact : builder.ToFrozenDictionary(StringComparer.Ordinal);
    }

    private static readonly FrozenDictionary<string, CompiledRule[]> EmptyExact =
        FrozenDictionary<string, CompiledRule[]>.Empty;
}
