using System.Collections.Frozen;
using System.Runtime.InteropServices;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Auth;

/// <summary>
/// A parsed subject-relative key or prefix: the literal text of a
/// <see cref="LatticeScope.KeyOrPrefix"/> with one or more placeholders that are
/// filled in from the caller's resolved <see cref="LatticeSubject"/> at decision
/// time. Two placeholders are recognized: <see cref="LatticeScope.SubjectPlaceholder"/>
/// (the caller's subject id) and <c>{group:&lt;prefix&gt;}</c> (the id of any group
/// in the caller's expanded group set that starts with <c>&lt;prefix&gt;</c>, with
/// the prefix stripped).
/// </summary>
/// <remarks>
/// <para>
/// Every placeholder must be followed by a literal delimiter character, which
/// bounds the substituted value: the value runs from the placeholder's position
/// up to the first occurrence of the delimiter. A subject or group id that is
/// empty, contains the delimiter, or contains U+001F never matches, so <c>u1</c>
/// cannot match a key belonging to <c>u10</c> and nothing about an id can be
/// probed by provoking an error. The anonymous subject never matches.
/// </para>
/// <para>
/// Only the exact tokens <c>{subject}</c> and <c>{group:</c> are recognized; any
/// other brace is literal text, so a scope written before templated scopes
/// existed keeps its meaning unless it spells one of those tokens.
/// </para>
/// <para>
/// Matching (<see cref="TryMatchPrefix"/>, <see cref="MatchesKey"/>) and
/// <see cref="VisitInstantiations{TVisitor}"/> are allocation-free for the group
/// closures the membership package produces (an ordinal
/// <see cref="HashSet{T}"/>), as well as arrays, lists and frozen sets.
/// </para>
/// <para>In-process value only: never serialized and never crosses a grain boundary.</para>
/// </remarks>
internal sealed class ScopeTemplate
{
    /// <summary>The opening of a group placeholder, <c>{group:</c>.</summary>
    internal const string GroupPlaceholderOpen = "{group:";

    /// <summary>
    /// The most instantiations <see cref="VisitInstantiations{TVisitor}"/> visits for
    /// one template before reporting itself incomplete. Bounds the work a template
    /// with several group placeholders can cause for a subject in many groups.
    /// </summary>
    internal const int MaxInstantiations = 4096;

    /// <summary>
    /// The most steps - placeholder expansions and group-set iterations together -
    /// <see cref="VisitInstantiations{TVisitor}"/> takes for one template before
    /// reporting itself incomplete. <see cref="MaxInstantiations"/> counts only
    /// completed instantiations, so this bounds the work of branches that never
    /// complete as well; callers treat an incomplete walk as fail closed.
    /// </summary>
    internal const int MaxWalkSteps = 65_536;

    /// <summary>
    /// The most positions an existence probe resolves across ALL the templated rules
    /// of one tree. Each position is resolved against every rule of the tree, so a
    /// per-template cap alone would let the number of templated rules square the work.
    /// </summary>
    internal const int MaxProbePositions = 1024;

    /// <summary>The most instantiations one rule contributes to an (advisory) explanation.</summary>
    internal const int MaxExplainInstantiations = 64;

    /// <summary>
    /// The most placeholders one template may carry. Bounds the recursion depth of
    /// <see cref="VisitInstantiations{TVisitor}"/> and the per-decision match cost.
    /// </summary>
    internal const int MaxPlaceholders = 8;

    private const int StackBufferChars = 256;

    private readonly Segment[] _segments;
    private readonly int _literalLength;

    private ScopeTemplate(string text, Segment[] segments)
    {
        Text = text;
        _segments = segments;
        foreach (var segment in segments)
        {
            if (segment.Kind == SegmentKind.Literal)
            {
                _literalLength += segment.Text!.Length;
            }
        }
    }

    /// <summary>The template as written, which is also the rule's stored key or prefix.</summary>
    public string Text { get; }

    /// <summary>
    /// <c>true</c> when <paramref name="text"/> spells a placeholder token
    /// (<c>{subject}</c> or <c>{group:</c>), so it is meant as a template whether or
    /// not it is a valid one. Allocation-free.
    /// </summary>
    /// <param name="text">The key or prefix text, or <c>null</c>.</param>
    /// <returns><c>true</c> when the text contains a placeholder token.</returns>
    public static bool ContainsPlaceholder(string? text) =>
        text is not null
        && (text.Contains(LatticeScope.SubjectPlaceholder, StringComparison.Ordinal)
            || text.Contains(GroupPlaceholderOpen, StringComparison.Ordinal));

    /// <summary>
    /// <c>true</c> when a templated scope may not be authored on
    /// <paramref name="treeId"/>: a control-plane namespace - any <c>sys-*</c> tree,
    /// the internal <c>_lattice_*</c> system namespace, or any other control-plane
    /// data tree (<see cref="AuthConstants.IsControlPlaneDataTree"/>, which adds the
    /// shared tag indexes).
    /// </summary>
    /// <param name="treeId">The governed tree id.</param>
    /// <returns><c>true</c> when the tree cannot hold a templated scope.</returns>
    public static bool IsForbiddenTree(string treeId) =>
        treeId.StartsWith("sys-", StringComparison.Ordinal)
        || treeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal)
        || AuthConstants.IsControlPlaneDataTree(treeId);

    /// <summary>
    /// Parses <paramref name="text"/> as a template.
    /// </summary>
    /// <param name="text">The key or prefix text.</param>
    /// <param name="template">The parsed template when this returns <c>true</c>; otherwise <c>null</c>.</param>
    /// <param name="error">
    /// Why the text is not a valid template when it contains a placeholder token
    /// but this returns <c>false</c>; <c>null</c> when the text carries no
    /// placeholder token at all (it is a plain literal).
    /// </param>
    /// <returns><c>true</c> when the text is a valid template.</returns>
    public static bool TryParse(string? text, out ScopeTemplate? template, out string? error)
    {
        template = null;
        error = null;
        if (!ContainsPlaceholder(text))
        {
            return false;
        }

        var segments = new List<Segment>();
        var literalStart = 0;
        var placeholders = 0;
        var i = 0;
        while (i < text!.Length)
        {
            if (text[i] != '{')
            {
                i++;
                continue;
            }

            Segment placeholder;
            int next;
            if (StartsWithAt(text, i, LatticeScope.SubjectPlaceholder))
            {
                placeholder = new Segment(SegmentKind.Subject, null);
                next = i + LatticeScope.SubjectPlaceholder.Length;
            }
            else if (StartsWithAt(text, i, GroupPlaceholderOpen))
            {
                var close = text.IndexOf('}', i + GroupPlaceholderOpen.Length);
                if (close < 0)
                {
                    error = $"The group placeholder at position {i} is not closed with '}}'.";
                    return false;
                }

                var groupPrefix = text.Substring(i + GroupPlaceholderOpen.Length, close - i - GroupPlaceholderOpen.Length);
                if (groupPrefix.Length == 0)
                {
                    error = "A group placeholder must name a non-empty group id prefix, for example '{group:grp:}'.";
                    return false;
                }

                if (groupPrefix.AsSpan().IndexOfAny('{', AuthConstants.RuleKeySeparator) >= 0)
                {
                    error = $"The group id prefix '{groupPrefix}' must not contain '{{' or U+001F.";
                    return false;
                }

                placeholder = new Segment(SegmentKind.Group, groupPrefix);
                next = close + 1;
            }
            else
            {
                // Any other brace is literal text.
                i++;
                continue;
            }

            if (++placeholders > MaxPlaceholders)
            {
                error = $"A templated scope may carry at most {MaxPlaceholders} placeholders.";
                return false;
            }

            if (i > literalStart)
            {
                segments.Add(new Segment(SegmentKind.Literal, text[literalStart..i]));
            }

            if (next >= text.Length)
            {
                error = "Every placeholder must be followed by a literal delimiter (for example '/'), so that a "
                    + "substituted id cannot run into the next key segment.";
                return false;
            }

            if (StartsWithAt(text, next, LatticeScope.SubjectPlaceholder) || StartsWithAt(text, next, GroupPlaceholderOpen))
            {
                error = "Two placeholders must be separated by a literal delimiter (for example '/').";
                return false;
            }

            var delimiter = text[next];
            if (delimiter == AuthConstants.RuleKeySeparator)
            {
                error = "A placeholder delimiter must not be U+001F.";
                return false;
            }

            segments.Add(placeholder);
            literalStart = next;
            i = next;
        }

        if (literalStart < text.Length)
        {
            segments.Add(new Segment(SegmentKind.Literal, text[literalStart..]));
        }

        template = new ScopeTemplate(text, segments.ToArray());
        return true;
    }

    /// <summary>
    /// The reason <paramref name="rule"/> may not carry its templated key or prefix,
    /// or <c>null</c> when its scope is not templated or is a valid, permitted
    /// template. The policy store refuses a rule this reports on, and the compiler
    /// treats such a rule's scope as a literal (see <see cref="ForCompile"/>).
    /// </summary>
    /// <remarks>
    /// A template is refused when it does not parse; on a control-plane tree
    /// (<see cref="IsForbiddenTree"/>); and when a group placeholder can reach the
    /// reserved tenant-group namespace (<c>t/</c>) other than that of the tenant
    /// owning the scoped tree - the placeholder names tenant groups as subjects, so it
    /// is held to the same confinement as a tenant group subject. An app-owned
    /// (<c>app:</c>) rule is never treated as a template, because an app's consent
    /// ceiling is checked against its scope text and a template's text says nothing
    /// about the keys it resolves to; its scope is not validated and always matches
    /// literally.
    /// </remarks>
    /// <param name="rule">The candidate rule.</param>
    /// <returns>The refusal reason, or <c>null</c>.</returns>
    public static string? Validate(LatticeAuthorizationRule rule)
    {
        var scope = rule.Scope;
        if (scope is null || scope.Kind == LatticeScopeKind.Tree || !ContainsPlaceholder(scope.KeyOrPrefix))
        {
            return null;
        }

        // An app-owned rule always matches literally (see ForCompile), so its scope is
        // not validated as a template unless it sits on a control-plane tree: an app
        // manifest written before templated scopes existed installs exactly as it did.
        if (LatticeAppRuleIds.IsAppOwned(rule.RuleId) && !IsForbiddenTree(scope.TreeId))
        {
            return null;
        }

        return ValidateScope(scope);
    }

    /// <summary>
    /// The reason <paramref name="scope"/>'s templated key or prefix is refused,
    /// whichever rule carries it, or <c>null</c> when the scope is not templated or is
    /// a valid, permitted template. See <see cref="Validate"/>.
    /// </summary>
    /// <param name="scope">The candidate scope.</param>
    /// <returns>The refusal reason, or <c>null</c>.</returns>
    public static string? ValidateScope(LatticeScope scope)
    {
        if (scope.Kind == LatticeScopeKind.Tree || !ContainsPlaceholder(scope.KeyOrPrefix))
        {
            return null;
        }

        if (IsForbiddenTree(scope.TreeId))
        {
            return $"Tree '{scope.TreeId}' is a control-plane namespace and cannot hold a templated (subject-relative) "
                + "scope.";
        }

        if (!TryParse(scope.KeyOrPrefix, out var template, out var error))
        {
            return error;
        }

        foreach (var segment in template!._segments)
        {
            if (segment.Kind == SegmentKind.Group && !IsConfinedGroupPrefix(segment.Text!, scope.TreeId))
            {
                return $"The group placeholder prefix '{segment.Text}' can match tenant groups "
                    + $"('{LatticeTenantTrees.SegmentPrefix}...'). In a templated scope it must start with "
                    + $"'{LatticeTenantTrees.SegmentPrefix}{{tenant}}/' for the tenant owning tree '{scope.TreeId}'.";
            }
        }

        return null;
    }

    /// <summary>
    /// Returns the template a rule's scope compiles to, or <c>null</c> when the scope
    /// compiles as a literal: a whole-tree scope, a key or prefix with no placeholder,
    /// an app-owned rule, or one <see cref="Validate"/> refuses. A rule the store
    /// would refuse but which reached the policy tree anyway (by replication or a
    /// restore) therefore keeps the literal meaning it had before templated scopes
    /// existed.
    /// </summary>
    /// <param name="rule">The rule.</param>
    /// <returns>The compiled template, or <c>null</c> for a literal scope.</returns>
    public static ScopeTemplate? ForCompile(LatticeAuthorizationRule rule) =>
        rule.Scope.Kind != LatticeScopeKind.Tree
        && ContainsPlaceholder(rule.Scope.KeyOrPrefix)
        && !LatticeAppRuleIds.IsAppOwned(rule.RuleId)
        && Validate(rule) is null
        && TryParse(rule.Scope.KeyOrPrefix, out var template, out _)
            ? template
            : null;

    /// <summary>
    /// <c>true</c> when <paramref name="scope"/> on its own (regardless of which rule
    /// carries it) is a valid template on a tree that may hold one.
    /// </summary>
    /// <param name="scope">The scope.</param>
    /// <returns><c>true</c> when the scope is a permitted template.</returns>
    public static bool IsTemplatedScope(LatticeScope scope) =>
        scope.Kind != LatticeScopeKind.Tree
        && ContainsPlaceholder(scope.KeyOrPrefix)
        && !IsForbiddenTree(scope.TreeId)
        && TryParse(scope.KeyOrPrefix, out _, out _);

    /// <summary>
    /// The concrete scopes <paramref name="rule"/>'s scope stands for when evaluated
    /// for <paramref name="subject"/>: the scope itself when it compiles as a literal,
    /// otherwise one scope per instantiation (empty when the template cannot match the
    /// subject). Used off the hot path to cite templated rules in explanations.
    /// </summary>
    /// <param name="rule">The rule.</param>
    /// <param name="subject">The subject the scope is resolved for.</param>
    /// <returns>The concrete scopes.</returns>
    public static IReadOnlyList<LatticeScope> ResolveFor(LatticeAuthorizationRule rule, in LatticeSubject subject)
    {
        if (ForCompile(rule) is not { } template)
        {
            return new[] { rule.Scope };
        }

        var values = new List<string>();
        var budget = new WalkBudget(MaxExplainInstantiations, MaxWalkSteps);
        var collector = new ListCollector(values);
        template.VisitInstantiations(subject, ref collector, ref budget);
        var scopes = new LatticeScope[values.Count];
        for (var i = 0; i < values.Count; i++)
        {
            scopes[i] = new LatticeScope(rule.Scope.Kind, rule.Scope.TreeId, values[i]);
        }

        return scopes;
    }

    /// <summary>
    /// Matches <paramref name="key"/> against this template as a prefix: <c>true</c>
    /// when the key begins with the template resolved for <paramref name="subject"/>,
    /// reporting the resolved prefix's length. Allocation-free.
    /// </summary>
    /// <param name="key">The candidate key.</param>
    /// <param name="subject">The requesting subject.</param>
    /// <param name="resolvedLength">The length of the resolved prefix when this returns <c>true</c>.</param>
    /// <returns><c>true</c> when the key falls under the resolved prefix.</returns>
    public bool TryMatchPrefix(string key, in LatticeSubject subject, out int resolvedLength)
    {
        resolvedLength = Match(key, subject);
        return resolvedLength >= 0;
    }

    /// <summary>
    /// Matches <paramref name="key"/> against this template as an exact key:
    /// <c>true</c> when the key equals the template resolved for
    /// <paramref name="subject"/>. Allocation-free.
    /// </summary>
    /// <param name="key">The candidate key.</param>
    /// <param name="subject">The requesting subject.</param>
    /// <returns><c>true</c> when the key is the resolved key.</returns>
    public bool MatchesKey(string key, in LatticeSubject subject) => Match(key, subject) == key.Length;

    /// <summary>
    /// Visits every concrete key or prefix this template resolves to for
    /// <paramref name="subject"/> - the subject id substituted for each
    /// <c>{subject}</c>, and every qualifying group of the subject for each
    /// <c>{group:...}</c> - as a span over a reused buffer, stopping when the visitor
    /// returns <c>false</c>, after <see cref="MaxInstantiations"/> values, or after
    /// <see cref="MaxWalkSteps"/> steps. A template with a placeholder the subject
    /// cannot fill resolves to nothing and is rejected before any expansion. The
    /// buffer is on the stack for instantiations up to 256 characters, so the walk
    /// allocates nothing in the common case.
    /// </summary>
    /// <typeparam name="TVisitor">The visitor; a struct so the call is not boxed.</typeparam>
    /// <param name="subject">The subject.</param>
    /// <param name="visitor">The visitor, passed by reference so it can record a result.</param>
    /// <returns><c>false</c> when a bound stopped the walk before every instantiation was visited; otherwise <c>true</c>.</returns>
    public bool VisitInstantiations<TVisitor>(in LatticeSubject subject, ref TVisitor visitor)
        where TVisitor : struct, IInstantiationVisitor
    {
        var budget = new WalkBudget(MaxInstantiations, MaxWalkSteps);
        return VisitInstantiations(subject, ref visitor, ref budget);
    }

    /// <summary>
    /// Visits this template's instantiations for <paramref name="subject"/> as
    /// <see cref="VisitInstantiations{TVisitor}(in LatticeSubject, ref TVisitor)"/> does,
    /// but spends <paramref name="budget"/>, which the caller shares across every
    /// template one decision or probe walks, so the total work of that decision is
    /// bounded and not only the work of each template.
    /// </summary>
    /// <typeparam name="TVisitor">The visitor; a struct so the call is not boxed.</typeparam>
    /// <param name="subject">The subject.</param>
    /// <param name="visitor">The visitor, passed by reference so it can record a result.</param>
    /// <param name="budget">The shared budget; marked exhausted when a bound stops the walk.</param>
    /// <returns><c>false</c> when the budget is exhausted; otherwise <c>true</c>.</returns>
    public bool VisitInstantiations<TVisitor>(in LatticeSubject subject, ref TVisitor visitor, ref WalkBudget budget)
        where TVisitor : struct, IInstantiationVisitor
    {
        if (budget.Exhausted)
        {
            return false;
        }

        if (subject.IsAnonymous)
        {
            return true;
        }

        var capacity = _literalLength;
        var longestGroup = -1;
        for (var s = 0; s < _segments.Length; s++)
        {
            var segment = _segments[s];
            if (segment.Kind == SegmentKind.Literal)
            {
                continue;
            }

            // A placeholder no value can fill makes every instantiation impossible;
            // stop before expanding the placeholders ahead of it (linear work).
            if (!CanFill(segment, _segments[s + 1].Text![0], subject, ref budget))
            {
                return !budget.Exhausted;
            }

            if (segment.Kind == SegmentKind.Subject)
            {
                capacity += subject.SubjectId.Length;
            }
            else
            {
                if (longestGroup < 0)
                {
                    longestGroup = LongestGroup(subject.GroupIds, ref budget);
                    if (budget.Exhausted)
                    {
                        return false;
                    }
                }

                capacity += longestGroup;
            }
        }

        Span<char> buffer = capacity <= StackBufferChars ? stackalloc char[capacity] : new char[capacity];
        var stop = false;
        Walk(0, buffer, 0, subject, ref visitor, ref budget, ref stop);
        return !budget.Exhausted;
    }

    /// <summary>
    /// Whether at least one value of <paramref name="subject"/> can fill the placeholder
    /// <paramref name="segment"/>. Each group scanned is charged to
    /// <paramref name="budget"/>; when it runs out the answer is <c>false</c> and the
    /// budget is exhausted.
    /// </summary>
    private static bool CanFill(Segment segment, char delimiter, in LatticeSubject subject, ref WalkBudget budget)
    {
        if (segment.Kind == SegmentKind.Subject)
        {
            return IsSubstitutable(subject.SubjectId, delimiter);
        }

        var prefix = segment.Text!;
        var cursor = new GroupCursor(subject.GroupIds);
        try
        {
            while (cursor.MoveNext())
            {
                if (!budget.TakeStep())
                {
                    return false;
                }

                var group = cursor.Current;
                if (group.Length > prefix.Length
                    && group.StartsWith(prefix, StringComparison.Ordinal)
                    && IsSubstitutable(group.AsSpan(prefix.Length), delimiter))
                {
                    return true;
                }
            }
        }
        finally
        {
            cursor.Dispose();
        }

        return false;
    }

    /// <summary>
    /// Appends every concrete key or prefix this template resolves to for
    /// <paramref name="subject"/> to <paramref name="into"/>. Allocates one string per
    /// instantiation; used only off the hot path (existence probes and explanations).
    /// </summary>
    /// <param name="subject">The subject.</param>
    /// <param name="into">The list to append to.</param>
    /// <returns><c>false</c> when the cap stopped the walk; otherwise <c>true</c>.</returns>
    public bool AppendInstantiations(in LatticeSubject subject, List<string> into)
    {
        var collector = new ListCollector(into);
        return VisitInstantiations(subject, ref collector);
    }

    /// <summary>
    /// Walks the template over <paramref name="key"/> and returns the length of the
    /// resolved template when the key begins with it, or <c>-1</c>.
    /// </summary>
    private int Match(string key, in LatticeSubject subject)
    {
        if (subject.IsAnonymous)
        {
            return -1;
        }

        var position = 0;
        for (var s = 0; s < _segments.Length; s++)
        {
            var segment = _segments[s];
            if (segment.Kind == SegmentKind.Literal)
            {
                if (!key.AsSpan(position).StartsWith(segment.Text, StringComparison.Ordinal))
                {
                    return -1;
                }

                position += segment.Text!.Length;
                continue;
            }

            // A placeholder is always followed by a non-empty literal whose first
            // character is the delimiter that bounds the substituted value, so the
            // value cannot contain the delimiter and an id that does never matches.
            var delimiter = _segments[s + 1].Text![0];
            var end = key.IndexOf(delimiter, position);
            if (end <= position)
            {
                return -1;
            }

            var value = key.AsSpan(position, end - position);
            if (value.Contains(AuthConstants.RuleKeySeparator))
            {
                return -1;
            }

            if (segment.Kind == SegmentKind.Subject)
            {
                if (!value.SequenceEqual(subject.SubjectId))
                {
                    return -1;
                }
            }
            else if (!HasGroup(subject.GroupIds, segment.Text!, value))
            {
                return -1;
            }

            position = end;
        }

        return position;
    }

    private void Walk<TVisitor>(
        int index,
        Span<char> buffer,
        int length,
        in LatticeSubject subject,
        ref TVisitor visitor,
        ref WalkBudget budget,
        ref bool stop)
        where TVisitor : struct, IInstantiationVisitor
    {
        if (stop || !budget.TakeStep())
        {
            return;
        }

        if (index == _segments.Length)
        {
            if (!budget.TakeInstantiation())
            {
                return;
            }

            if (!visitor.Visit(buffer[..length]))
            {
                stop = true;
            }

            return;
        }

        var segment = _segments[index];
        if (segment.Kind == SegmentKind.Literal)
        {
            segment.Text.AsSpan().CopyTo(buffer[length..]);
            Walk(index + 1, buffer, length + segment.Text!.Length, subject, ref visitor, ref budget, ref stop);
            return;
        }

        var delimiter = _segments[index + 1].Text![0];
        if (segment.Kind == SegmentKind.Subject)
        {
            if (IsSubstitutable(subject.SubjectId, delimiter))
            {
                subject.SubjectId.AsSpan().CopyTo(buffer[length..]);
                Walk(index + 1, buffer, length + subject.SubjectId.Length, subject, ref visitor, ref budget, ref stop);
            }

            return;
        }

        var prefix = segment.Text!;
        // Not a using declaration: that would make the cursor read-only, so each
        // MoveNext would advance a defensive copy and never the cursor itself.
        var groups = new GroupCursor(subject.GroupIds);
        try
        {
            while (!stop && groups.MoveNext())
            {
                if (!budget.TakeStep())
                {
                    return;
                }

                var group = groups.Current;
                if (group.Length > prefix.Length
                    && group.StartsWith(prefix, StringComparison.Ordinal)
                    && IsSubstitutable(group.AsSpan(prefix.Length), delimiter))
                {
                    var suffix = group.AsSpan(prefix.Length);
                    suffix.CopyTo(buffer[length..]);
                    Walk(index + 1, buffer, length + suffix.Length, subject, ref visitor, ref budget, ref stop);
                }
            }
        }
        finally
        {
            groups.Dispose();
        }
    }

    /// <summary>
    /// <c>true</c> when a group placeholder prefix either cannot match a tenant group
    /// (<c>t/...</c>) or is confined to the tenant that owns <paramref name="treeId"/>
    /// (it starts with <c>t/{tenant}/</c>).
    /// </summary>
    private static bool IsConfinedGroupPrefix(string groupPrefix, string treeId)
    {
        var tenantNamespace = LatticeTenantTrees.SegmentPrefix;
        if (!groupPrefix.StartsWith(tenantNamespace, StringComparison.Ordinal)
            && !tenantNamespace.StartsWith(groupPrefix, StringComparison.Ordinal))
        {
            return true;
        }

        if (groupPrefix.Length <= tenantNamespace.Length
            || !TenantRuleConfinement.TryGetTenantLayerTree(treeId, out var tenant))
        {
            return false;
        }

        var rest = groupPrefix.AsSpan(tenantNamespace.Length);
        return rest.Length > tenant.Length
            && rest.StartsWith(tenant, StringComparison.Ordinal)
            && rest[tenant.Length] == '/';
    }

    private static bool StartsWithAt(string text, int index, string token) =>
        string.CompareOrdinal(text, index, token, 0, token.Length) == 0;

    /// <summary>
    /// Whether <paramref name="value"/> can be substituted for a placeholder
    /// delimited by <paramref name="delimiter"/>: non-empty and free of both the
    /// delimiter and U+001F.
    /// </summary>
    private static bool IsSubstitutable(ReadOnlySpan<char> value, char delimiter) =>
        !value.IsEmpty && value.IndexOfAny(delimiter, AuthConstants.RuleKeySeparator) < 0;

    private static int LongestGroup(IReadOnlyCollection<string> groups, ref WalkBudget budget)
    {
        var longest = 0;
        var cursor = new GroupCursor(groups);
        try
        {
            while (cursor.MoveNext() && budget.TakeStep())
            {
                longest = Math.Max(longest, cursor.Current.Length);
            }
        }
        finally
        {
            cursor.Dispose();
        }

        return longest;
    }

    /// <summary>
    /// Whether the group <c><paramref name="prefix"/> + <paramref name="value"/></c>
    /// is in <paramref name="groups"/>. A hash or frozen set is probed with one
    /// lookup that composes the id on the stack; any other closure is scanned.
    /// Allocation-free for every closure shape <see cref="GroupCursor"/> handles.
    /// </summary>
    private static bool HasGroup(IReadOnlyCollection<string> groups, string prefix, ReadOnlySpan<char> value)
    {
        if (groups.Count == 0)
        {
            return false;
        }

        var length = prefix.Length + value.Length;
        if (length <= StackBufferChars)
        {
            if (groups is HashSet<string> hashSet && hashSet.TryGetAlternateLookup<ReadOnlySpan<char>>(out var hashLookup))
            {
                Span<char> buffer = stackalloc char[length];
                return hashLookup.Contains(Compose(buffer, prefix, value));
            }

            if (groups is FrozenSet<string> frozenSet && frozenSet.TryGetAlternateLookup<ReadOnlySpan<char>>(out var frozenLookup))
            {
                Span<char> buffer = stackalloc char[length];
                return frozenLookup.Contains(Compose(buffer, prefix, value));
            }
        }

        var cursor = new GroupCursor(groups);
        try
        {
            while (cursor.MoveNext())
            {
                var group = cursor.Current;
                if (group.Length == length
                    && group.StartsWith(prefix, StringComparison.Ordinal)
                    && group.AsSpan(prefix.Length).SequenceEqual(value))
                {
                    return true;
                }
            }
        }
        finally
        {
            cursor.Dispose();
        }

        return false;
    }

    private static ReadOnlySpan<char> Compose(Span<char> buffer, string prefix, ReadOnlySpan<char> value)
    {
        prefix.AsSpan().CopyTo(buffer);
        value.CopyTo(buffer[prefix.Length..]);
        return buffer;
    }

    /// <summary>Receives each instantiation of a template.</summary>
    internal interface IInstantiationVisitor
    {
        /// <summary>Receives one instantiation; valid only for the duration of the call.</summary>
        /// <param name="value">The resolved key or prefix.</param>
        /// <returns><c>true</c> to continue; <c>false</c> to stop the walk.</returns>
        bool Visit(ReadOnlySpan<char> value);
    }

    private struct ListCollector(List<string> into) : IInstantiationVisitor
    {
        public readonly bool Visit(ReadOnlySpan<char> value)
        {
            into.Add(new string(value));
            return true;
        }
    }

    /// <summary>
    /// The work a template walk may still do: instantiations to visit and steps to
    /// take. Shared by reference across every template one decision walks, so the
    /// decision's total work is bounded. Once exhausted it stays exhausted, and every
    /// caller treats an exhausted walk as incomplete (fail closed).
    /// </summary>
    /// <param name="instantiations">The instantiations that may still be visited.</param>
    /// <param name="steps">The steps that may still be taken.</param>
    internal struct WalkBudget(int instantiations, int steps)
    {
        private int _instantiations = instantiations;
        private int _steps = steps;

        /// <summary><c>true</c> once a bound stopped a walk.</summary>
        public bool Exhausted { get; private set; }

        /// <summary>Spends one step; when none is left, marks the budget exhausted.</summary>
        /// <returns><c>false</c> when the budget is exhausted.</returns>
        public bool TakeStep()
        {
            if (!Exhausted && _steps > 0)
            {
                _steps--;
                return true;
            }

            Exhausted = true;
            return false;
        }

        /// <summary>Spends one instantiation; when none is left, marks the budget exhausted.</summary>
        /// <returns><c>false</c> when the budget is exhausted.</returns>
        public bool TakeInstantiation()
        {
            if (!Exhausted && _instantiations > 0)
            {
                _instantiations--;
                return true;
            }

            Exhausted = true;
            return false;
        }
    }

    /// <summary>
    /// Enumerates a group closure without boxing an enumerator for the shapes the
    /// platform produces: arrays, lists and frozen sets as spans, and hash sets by
    /// their struct enumerator. Any other collection falls back to its interface
    /// enumerator.
    /// </summary>
    private ref struct GroupCursor
    {
        private readonly ReadOnlySpan<string> _span;
        private readonly IEnumerator<string>? _other;
        private readonly bool _isHash;
        private HashSet<string>.Enumerator _hash;
        private int _index;

        public GroupCursor(IReadOnlyCollection<string> groups)
        {
            _index = -1;
            switch (groups)
            {
                case string[] array:
                    _span = array;
                    break;
                case List<string> list:
                    _span = CollectionsMarshal.AsSpan(list);
                    break;
                case FrozenSet<string> frozen:
                    _span = frozen.Items.AsSpan();
                    break;
                case HashSet<string> hash:
                    _hash = hash.GetEnumerator();
                    _isHash = true;
                    break;
                default:
                    _other = groups.GetEnumerator();
                    break;
            }
        }

        public string Current { get; private set; } = string.Empty;

        public bool MoveNext()
        {
            if (_isHash)
            {
                if (_hash.MoveNext())
                {
                    Current = _hash.Current;
                    return true;
                }

                return false;
            }

            if (_other is not null)
            {
                if (_other.MoveNext())
                {
                    Current = _other.Current;
                    return true;
                }

                return false;
            }

            if (++_index < _span.Length)
            {
                Current = _span[_index];
                return true;
            }

            return false;
        }

        public readonly void Dispose() => _other?.Dispose();
    }

    private enum SegmentKind
    {
        Literal,
        Subject,
        Group,
    }

    /// <summary>One piece of a template: literal text, or a placeholder (a group placeholder carries its id prefix).</summary>
    private readonly record struct Segment(SegmentKind Kind, string? Text);
}
