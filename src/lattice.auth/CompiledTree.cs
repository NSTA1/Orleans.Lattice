using System.Collections.Frozen;
using System.Runtime.InteropServices;

namespace Orleans.Lattice.Auth;

/// <summary>
/// The compiled authorization rules governing a single tree, indexed for
/// deterministic, allocation-light point resolution. Rules are split into three
/// scope tiers: an exact-key map (most specific), a prefix index kept as a
/// sorted array for longest-prefix lookup, and a tree-wide list (least specific).
/// Built once by <see cref="CompiledPolicy.Compile(IEnumerable{LatticeAuthorizationRule}, bool)"/> and thereafter immutable.
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

    // The exact-key map's keys sorted ascending (ordinal), so a range request can
    // find whether any exact-key rule falls inside it with one binary search.
    private readonly string[] _exactKeys;
    // Parallel arrays: _prefixes is sorted ascending (ordinal); _prefixRules[i]
    // holds the rules scoped at _prefixes[i]. Sorting lets us binary-search for
    // the longest prefix of a key (the largest stored prefix that the key starts
    // with) and then walk to shorter prefixes only if the longer tiers have no
    // matching rule.
    private readonly string[] _prefixes;
    private readonly CompiledRule[][] _prefixRules;
    private readonly CompiledRule[] _treeRules;

    // Subject-relative (templated) key and prefix rules (issue #6061). They cannot
    // be indexed by their text, because what they cover depends on the caller, so
    // each is matched against the key at decision time and joins the exact-key or
    // prefix tier at its resolved position. Both are empty on a tree with no
    // templated rule, which then resolves exactly as before.
    private readonly TemplatedRule[] _templatedKeys;
    private readonly TemplatedRule[] _templatedPrefixes;

    private CompiledTree(
        FrozenDictionary<string, CompiledRule[]> exact,
        string[] exactKeys,
        string[] prefixes,
        CompiledRule[][] prefixRules,
        CompiledRule[] treeRules,
        TemplatedRule[] templatedKeys,
        TemplatedRule[] templatedPrefixes)
    {
        _exact = exact;
        _exactKeys = exactKeys;
        _prefixes = prefixes;
        _prefixRules = prefixRules;
        _treeRules = treeRules;
        _templatedKeys = templatedKeys;
        _templatedPrefixes = templatedPrefixes;
    }

    /// <summary>
    /// <c>true</c> when the tree carries any exact-key or prefix rule (literal or
    /// templated), so a collection (range) request can vary its decision
    /// key-by-key. When <c>false</c>, only tree-wide rules apply and a collection
    /// decision is uniform.
    /// </summary>
    public bool HasPerKeyRules =>
        _exact.Count > 0 || _prefixes.Length > 0 || _templatedKeys.Length > 0 || _templatedPrefixes.Length > 0;

    /// <summary><c>true</c> when the tree carries any subject-relative (templated) key or prefix rule.</summary>
    internal bool HasTemplatedRules => _templatedKeys.Length > 0 || _templatedPrefixes.Length > 0;

    /// <summary>
    /// The exact keys that carry at least one rule. Used off the hot path by the
    /// layered existence probe to enumerate the positions a decision can change at.
    /// </summary>
    internal IReadOnlyList<string> ExactKeys => _exact.Keys;

    /// <summary>
    /// The prefixes that carry at least one rule, in ascending ordinal order. Used off
    /// the hot path by the layered existence probe.
    /// </summary>
    internal IReadOnlyList<string> Prefixes => _prefixes;

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
            if (_templatedKeys.Length == 0)
            {
                if (_exact.TryGetValue(key, out var exactRules)
                    && TryBestInBucket(exactRules, subject, operation, userRuleBeatsGroupRule, out var exactWinner))
                {
                    return new PolicyMatch(exactWinner.Effect, exactWinner.RuleId, LatticeScopeKind.Key, key);
                }
            }
            else
            {
                // A templated key rule whose resolved key equals this key sits in the
                // same tier as a literal rule on it, so both compete under the same
                // user-over-group and deny-over-allow tie-break.
                var best = new TierWinner();
                if (_exact.TryGetValue(key, out var exactRules))
                {
                    foreach (ref readonly var rule in exactRules.AsSpan())
                    {
                        best.Consider(rule, key, subject, operation, userRuleBeatsGroupRule);
                    }
                }

                foreach (ref readonly var templated in _templatedKeys.AsSpan())
                {
                    if (RuleApplies(templated.Rule, subject, operation) && templated.Template.MatchesKey(key, subject))
                    {
                        best.ConsiderApplicable(templated.Rule, templated.Template.Text, userRuleBeatsGroupRule);
                    }
                }

                if (best.Found)
                {
                    return new PolicyMatch(best.Rule.Effect, best.Rule.RuleId, LatticeScopeKind.Key, best.Scope);
                }
            }
        }

        return ResolveBelowExactTier(subject, operation, key, userRuleBeatsGroupRule);
    }

    /// <summary>
    /// Resolves tiers 2 and 3 of <see cref="ResolvePoint"/> - the prefixes of
    /// <paramref name="key"/>, longest first, then the tree-wide rules - skipping the
    /// exact-key tier. For a stored prefix this is the decision every key that
    /// extends the prefix and names no more specific rule resolves to.
    /// </summary>
    internal PolicyMatch ResolveBelowExactTier(
        in LatticeSubject subject,
        LatticeOperation operation,
        string? key,
        bool userRuleBeatsGroupRule)
    {
        if (key is not null && _templatedPrefixes.Length > 0)
        {
            var prefixMatch = ResolvePrefixTierWithTemplates(subject, operation, key, userRuleBeatsGroupRule);
            if (prefixMatch.Matched)
            {
                return prefixMatch;
            }
        }
        else if (key is not null)
        {
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
    /// Tier 2 when the tree carries templated prefix rules: the literal prefixes of
    /// <paramref name="key"/> and the templated prefixes that resolve to a prefix of
    /// it, walked together longest resolved prefix first. A templated prefix ranks by
    /// its length after substitution, and one that resolves to the same prefix as a
    /// literal rule competes in that rule's tier, so the usual tie-break (user over
    /// group, then deny over allow) decides between them. Allocation-free while the
    /// tree has at most 128 templated prefix rules.
    /// </summary>
    private PolicyMatch ResolvePrefixTierWithTemplates(
        in LatticeSubject subject,
        LatticeOperation operation,
        string key,
        bool userRuleBeatsGroupRule)
    {
        var count = _templatedPrefixes.Length;
        Span<int> lengths = count <= 128 ? stackalloc int[count] : new int[count];
        for (var t = 0; t < count; t++)
        {
            ref readonly var templated = ref _templatedPrefixes[t];
            lengths[t] = RuleApplies(templated.Rule, subject, operation)
                && templated.Template.TryMatchPrefix(key, subject, out var resolved)
                    ? resolved
                    : -1;
        }

        var literal = LargestPrefixAtOrBelow(key);
        var ceiling = int.MaxValue;
        while (true)
        {
            while (literal >= 0 && !key.StartsWith(_prefixes[literal], StringComparison.Ordinal))
            {
                literal--;
            }

            var literalLength = literal >= 0 ? _prefixes[literal].Length : -1;
            var templatedLength = -1;
            foreach (var length in lengths)
            {
                if (length < ceiling && length > templatedLength)
                {
                    templatedLength = length;
                }
            }

            var tier = Math.Max(literalLength, templatedLength);
            if (tier < 0)
            {
                return default;
            }

            var best = new TierWinner();
            if (literalLength == tier)
            {
                var prefix = _prefixes[literal];
                foreach (ref readonly var rule in _prefixRules[literal].AsSpan())
                {
                    best.Consider(rule, prefix, subject, operation, userRuleBeatsGroupRule);
                }

                literal--;
            }

            for (var t = 0; t < count; t++)
            {
                if (lengths[t] == tier)
                {
                    ref readonly var templated = ref _templatedPrefixes[t];
                    best.ConsiderApplicable(templated.Rule, templated.Template.Text, userRuleBeatsGroupRule);
                }
            }

            if (best.Found)
            {
                return new PolicyMatch(best.Rule.Effect, best.Rule.RuleId, LatticeScopeKind.Prefix, best.Scope);
            }

            ceiling = tier;
        }
    }

    /// <summary>
    /// Resolves a collection request over the half-open range
    /// <c>[<paramref name="rangeStart"/>, <paramref name="rangeEnd"/>)</c> to the
    /// single rule match every key in the range resolves to, when there is one.
    /// The range is uniform when no exact-key rule applying to the subject and
    /// operation lies inside it, and every stored prefix with such a rule that
    /// governs any key of the range governs every key of it - which is true only of
    /// a prefix of <paramref name="rangeStart"/> whose own prefix range contains the
    /// whole request range. A rule that does not apply to the subject and operation
    /// is skipped by every resolution, so it cannot vary the decision. Every key
    /// then resolves through the same applicable prefix and tree tiers, so
    /// <see cref="ResolvePoint"/> agrees across the range and the shared answer is
    /// the below-exact-tier resolution of the range start.
    /// </summary>
    /// <param name="subject">The requesting subject.</param>
    /// <param name="operation">The requested operation.</param>
    /// <param name="rangeStart">The inclusive range start, or <c>null</c> for the start of the keyspace.</param>
    /// <param name="rangeEnd">The exclusive range end, or <c>null</c> for the end of the keyspace.</param>
    /// <param name="userRuleBeatsGroupRule">The user-over-group tie-break toggle.</param>
    /// <param name="match">The shared match (possibly unmatched) when the range is uniform.</param>
    /// <returns><c>true</c> when every key in a non-empty range resolves to <paramref name="match"/>.</returns>
    public bool TryResolveUniformRange(
        in LatticeSubject subject,
        LatticeOperation operation,
        string? rangeStart,
        string? rangeEnd,
        bool userRuleBeatsGroupRule,
        out PolicyMatch match)
    {
        match = default;
        var start = rangeStart ?? string.Empty;
        if (rangeEnd is not null && string.CompareOrdinal(start, rangeEnd) >= 0)
        {
            // An empty range is left to the per-key filter path unchanged.
            return false;
        }

        // An exact-key rule inside the range that applies to this subject and
        // operation decides that one key differently.
        for (var i = FirstAtOrAbove(_exactKeys, start); i < _exactKeys.Length && IsBelowEnd(_exactKeys[i], rangeEnd); i++)
        {
            if (Applies(_exact[_exactKeys[i]], subject, operation, userRuleBeatsGroupRule))
            {
                return false;
            }
        }

        // A stored prefix sorting strictly above the start but inside the range
        // governs the keys that extend it, and not the range start.
        var below = LargestPrefixAtOrBelow(start);
        for (var i = below + 1; i < _prefixes.Length && IsBelowEnd(_prefixes[i], rangeEnd); i++)
        {
            if (Applies(_prefixRules[i], subject, operation, userRuleBeatsGroupRule))
            {
                return false;
            }
        }

        // A stored prefix at or below the start governs the range only if it is a
        // prefix of the start (otherwise every key extending it sorts below the
        // start), and then it must govern the whole range, not just its head.
        for (var i = below; i >= 0; i--)
        {
            var prefix = _prefixes[i];
            if (start.StartsWith(prefix, StringComparison.Ordinal)
                && !RangeEndWithinPrefix(rangeEnd, prefix)
                && Applies(_prefixRules[i], subject, operation, userRuleBeatsGroupRule))
            {
                return false;
            }
        }

        // A templated rule stands for the keys and prefixes it resolves to for this
        // subject, so each of them is held to the same two tests as a literal one.
        if (HasTemplatedRules && !TemplatedRulesUniformOver(subject, operation, start, rangeEnd))
        {
            return false;
        }

        match = ResolveBelowExactTier(subject, operation, start, userRuleBeatsGroupRule);
        return true;
    }

    /// <summary>
    /// <c>true</c> when no templated rule that applies to <paramref name="subject"/>
    /// and <paramref name="operation"/> can vary the decision inside
    /// <c>[<paramref name="start"/>, <paramref name="rangeEnd"/>)</c>: none of its
    /// resolved keys lies inside the range, and none of its resolved prefixes governs
    /// part of the range without governing all of it. Fails closed (reports a
    /// non-uniform range, so the caller filters key by key) when a template has more
    /// instantiations than <see cref="ScopeTemplate.MaxInstantiations"/>.
    /// Allocation-free: instantiations are visited as spans over a stack buffer. One
    /// budget is shared by every templated rule of the tree, so the work per range
    /// decision is bounded however many templated rules the tree carries.
    /// </summary>
    private bool TemplatedRulesUniformOver(
        in LatticeSubject subject,
        LatticeOperation operation,
        string start,
        string? rangeEnd)
    {
        var budget = new ScopeTemplate.WalkBudget(ScopeTemplate.MaxInstantiations, ScopeTemplate.MaxWalkSteps);
        foreach (ref readonly var templated in _templatedKeys.AsSpan())
        {
            if (!RuleApplies(templated.Rule, subject, operation))
            {
                continue;
            }

            var check = new RangeCheck(start, rangeEnd, isPrefix: false);
            if (!templated.Template.VisitInstantiations(subject, ref check, ref budget) || check.Varies)
            {
                return false;
            }
        }

        foreach (ref readonly var templated in _templatedPrefixes.AsSpan())
        {
            if (!RuleApplies(templated.Rule, subject, operation))
            {
                continue;
            }

            var check = new RangeCheck(start, rangeEnd, isPrefix: true);
            if (!templated.Template.VisitInstantiations(subject, ref check, ref budget) || check.Varies)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Applies the literal-rule uniformity tests of <see cref="TryResolveUniformRange"/>
    /// to one resolved key or prefix, recording whether it varies the decision inside
    /// the range.
    /// </summary>
    private struct RangeCheck(string start, string? rangeEnd, bool isPrefix) : ScopeTemplate.IInstantiationVisitor
    {
        public bool Varies { get; private set; }

        public bool Visit(ReadOnlySpan<char> value)
        {
            if (!isPrefix)
            {
                // A resolved exact key inside the range decides that key differently.
                Varies = value.SequenceCompareTo(start) >= 0 && IsBelowEnd(value, rangeEnd);
            }
            else if (value.SequenceCompareTo(start) > 0)
            {
                // A prefix above the start but inside the range governs only the keys extending it.
                Varies = IsBelowEnd(value, rangeEnd);
            }
            else
            {
                // A prefix at or below the start governs the range only if it is a
                // prefix of the start, and then it must govern the whole range.
                Varies = start.AsSpan().StartsWith(value, StringComparison.Ordinal) && !RangeEndWithinPrefix(rangeEnd, value);
            }

            return !Varies;
        }
    }

    /// <summary>
    /// <c>true</c> when <paramref name="allowsAt"/> holds at some concrete key or
    /// prefix this tree's templated rules resolve to for <paramref name="subject"/>,
    /// considering only rules that apply to the subject and <paramref name="operation"/>.
    /// These are the subject's extra positions at which a decision can change,
    /// alongside <see cref="ExactKeys"/> and <see cref="Prefixes"/>. Used off the hot
    /// path by the existence probes. At most <see cref="ScopeTemplate.MaxProbePositions"/>
    /// positions are tried across all the tree's templated rules; when that bound
    /// stops the search the answer is <c>false</c> (no grant found), which fails
    /// closed for an existence probe.
    /// </summary>
    /// <param name="subject">The subject.</param>
    /// <param name="operation">The operation.</param>
    /// <param name="allowsAt">Given a position and whether it is an exact key (otherwise a prefix), whether the decision there allows.</param>
    /// <returns><c>true</c> when a position allows.</returns>
    internal bool AnyTemplatedPositionAllows(
        in LatticeSubject subject,
        LatticeOperation operation,
        Func<string, bool, bool> allowsAt)
    {
        var budget = new ScopeTemplate.WalkBudget(ScopeTemplate.MaxProbePositions, ScopeTemplate.MaxWalkSteps);
        return AnyAllows(_templatedKeys, exact: true, subject, operation, allowsAt, ref budget)
            || AnyAllows(_templatedPrefixes, exact: false, subject, operation, allowsAt, ref budget);

        static bool AnyAllows(
            TemplatedRule[] rules,
            bool exact,
            in LatticeSubject subject,
            LatticeOperation operation,
            Func<string, bool, bool> allowsAt,
            ref ScopeTemplate.WalkBudget budget)
        {
            foreach (ref readonly var templated in rules.AsSpan())
            {
                if (!RuleApplies(templated.Rule, subject, operation))
                {
                    continue;
                }

                var probe = new PositionProbe(allowsAt, exact);
                templated.Template.VisitInstantiations(subject, ref probe, ref budget);
                if (probe.Found)
                {
                    return true;
                }

                if (budget.Exhausted)
                {
                    return false;
                }
            }

            return false;
        }
    }

    /// <summary>Resolves each visited position through a caller predicate, stopping at the first that allows.</summary>
    private struct PositionProbe(Func<string, bool, bool> allowsAt, bool exact) : ScopeTemplate.IInstantiationVisitor
    {
        public bool Found { get; private set; }

        public bool Visit(ReadOnlySpan<char> value)
        {
            Found = allowsAt(new string(value), exact);
            return !Found;
        }
    }

    private static bool Applies(
        CompiledRule[] rules,
        in LatticeSubject subject,
        LatticeOperation operation,
        bool userRuleBeatsGroupRule) =>
        TryBestInBucket(rules, subject, operation, userRuleBeatsGroupRule, out _);

    private static bool IsBelowEnd(ReadOnlySpan<char> value, string? rangeEnd) =>
        rangeEnd is null || value.SequenceCompareTo(rangeEnd) < 0;

    /// <summary>
    /// <c>true</c> when every key below <paramref name="rangeEnd"/> that is at or
    /// above <paramref name="prefix"/> starts with it: the exclusive end is at or
    /// below <see cref="LatticeKeyRange.PrefixUpperBound(string)"/> of the prefix.
    /// Compares against that bound without materializing it.
    /// </summary>
    private static bool RangeEndWithinPrefix(string? rangeEnd, ReadOnlySpan<char> prefix)
    {
        var last = prefix.Length - 1;
        while (last >= 0 && prefix[last] == char.MaxValue)
        {
            last--;
        }

        if (last < 0)
        {
            // No finite upper bound: every key at or above the prefix extends it.
            return true;
        }

        if (rangeEnd is null)
        {
            return false;
        }

        // The bound is prefix[0..last) followed by prefix[last] + 1.
        for (var j = 0; j < last; j++)
        {
            if (j >= rangeEnd.Length)
            {
                return true;
            }

            if (rangeEnd[j] != prefix[j])
            {
                return rangeEnd[j] < prefix[j];
            }
        }

        if (last >= rangeEnd.Length)
        {
            return true;
        }

        var boundUnit = (char)(prefix[last] + 1);
        if (rangeEnd[last] != boundUnit)
        {
            return rangeEnd[last] < boundUnit;
        }

        return rangeEnd.Length == last + 1;
    }

    /// <summary>
    /// Returns the index of the first element of the ordinal-sorted
    /// <paramref name="sorted"/> that is at or above <paramref name="value"/>, or
    /// its length when none is.
    /// </summary>
    private static int FirstAtOrAbove(string[] sorted, string value)
    {
        var lo = 0;
        var hi = sorted.Length;
        while (lo < hi)
        {
            var mid = lo + ((hi - lo) >> 1);
            if (string.CompareOrdinal(sorted[mid], value) < 0)
            {
                lo = mid + 1;
            }
            else
            {
                hi = mid;
            }
        }

        return lo;
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

    /// <summary><c>true</c> when <paramref name="rule"/> covers <paramref name="operation"/> and its subject selector matches <paramref name="subject"/>.</summary>
    private static bool RuleApplies(in CompiledRule rule, in LatticeSubject subject, LatticeOperation operation) =>
        OperationMatches(rule.Operations, operation) && SubjectMatches(subject, rule);

    /// <summary>
    /// The running winner of one scope tier when literal and templated rules compete
    /// in it: the same score as <see cref="TryBestInBucket"/> (user over group when
    /// enabled, then deny over allow), plus the winning rule's scope text.
    /// </summary>
    private struct TierWinner
    {
        private int _score;

        public bool Found { get; private set; }

        public CompiledRule Rule { get; private set; }

        public string? Scope { get; private set; }

        public void Consider(
            in CompiledRule rule,
            string scope,
            in LatticeSubject subject,
            LatticeOperation operation,
            bool userRuleBeatsGroupRule)
        {
            if (RuleApplies(rule, subject, operation))
            {
                ConsiderApplicable(rule, scope, userRuleBeatsGroupRule);
            }
        }

        /// <summary>Considers a rule the caller has already found applies to the subject and operation.</summary>
        public void ConsiderApplicable(in CompiledRule rule, string scope, bool userRuleBeatsGroupRule)
        {
            var subjectRank = userRuleBeatsGroupRule && rule.SubjectKind == LatticeSubjectSelectorKind.User ? 1 : 0;
            var effectRank = rule.Effect == LatticeEffect.Deny ? 1 : 0;
            var score = (subjectRank << 1) | effectRank;
            if (!Found || score > _score)
            {
                Found = true;
                _score = score;
                Rule = rule;
                Scope = scope;
            }
        }
    }

    /// <summary>A compiled rule whose key or prefix is a subject-relative template.</summary>
    private readonly record struct TemplatedRule(ScopeTemplate Template, CompiledRule Rule);

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
        // O(1) set path when the closure is a set (the common case), then an
        // allocation-free span scan for an array or list, and fall back to the
        // interface enumerator otherwise.
        var groups = subject.GroupIds;
        if (groups is IReadOnlySet<string> set)
        {
            return set.Contains(rule.SubjectId);
        }

        ReadOnlySpan<string> span;
        if (groups is string[] array)
        {
            span = array;
        }
        else if (groups is List<string> list)
        {
            span = CollectionsMarshal.AsSpan(list);
        }
        else
        {
            foreach (var group in groups)
            {
                if (string.Equals(group, rule.SubjectId, StringComparison.Ordinal))
                {
                    return true;
                }
            }

            return false;
        }

        foreach (var group in span)
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

        // Any prefix grant whose decision for the keys it governs resolves to allow.
        // Those keys are the ones that extend the prefix without naming a more
        // specific rule, so the exact-key tier is skipped: an exact-key deny on the
        // key that spells the prefix denies that one key, not the prefix's others,
        // and evaluating the prefix string as an ordinary key would let it hide a
        // tree the subject can still read.
        foreach (var prefix in _prefixes)
        {
            var m = ResolveBelowExactTier(subject, operation, prefix, userRuleBeatsGroupRule);
            if (m.Matched && m.Effect == LatticeEffect.Allow)
            {
                return true;
            }
        }

        // A templated grant is judged at the keys and prefixes it resolves to for
        // this subject, exactly as a literal grant at that position would be.
        if (HasTemplatedRules)
        {
            var probeSubject = subject;
            return AnyTemplatedPositionAllows(subject, operation, (position, exact) =>
            {
                var m = exact
                    ? ResolvePoint(probeSubject, operation, position, userRuleBeatsGroupRule)
                    : ResolveBelowExactTier(probeSubject, operation, position, userRuleBeatsGroupRule);
                return m.Matched && m.Effect == LatticeEffect.Allow;
            });
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
        List<TemplatedRule>? templatedKeys = null;
        List<TemplatedRule>? templatedPrefixes = null;

        foreach (var rule in rules)
        {
            var compiled = new CompiledRule(rule.RuleId, rule.Subject.Kind, rule.Subject.Id, rule.Operations, rule.Effect);
            if (rule.Scope.Kind != LatticeScopeKind.Tree && ScopeTemplate.ForCompile(rule) is { } template)
            {
                var bucket = rule.Scope.Kind == LatticeScopeKind.Key
                    ? templatedKeys ??= new List<TemplatedRule>()
                    : templatedPrefixes ??= new List<TemplatedRule>();
                bucket.Add(new TemplatedRule(template, compiled));
                continue;
            }

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
        string[] exactKeys;
        if (exactBuilder is null)
        {
            exactKeys = Array.Empty<string>();
        }
        else
        {
            exactKeys = new string[exactBuilder.Count];
            exactBuilder.Keys.CopyTo(exactKeys, 0);
            Array.Sort(exactKeys, StringComparer.Ordinal);
        }

        return new CompiledTree(
            exact,
            exactKeys,
            prefixes,
            prefixRules,
            treeRules,
            templatedKeys is null ? Array.Empty<TemplatedRule>() : templatedKeys.ToArray(),
            templatedPrefixes is null ? Array.Empty<TemplatedRule>() : templatedPrefixes.ToArray());
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
