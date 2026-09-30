using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Suggestions;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// The member paths a tree's schema policy already names, suggested where a
/// member is typed: a new rule's member and a remediation step's member.
/// </summary>
/// <remarks>
/// Suggestions only: a value's members are not known to the cluster until a rule
/// names them, so any member path is accepted. Read once per tree and caller
/// (sign-in, endpoint and asserted tenant); a policy that cannot be read answers unavailable, with a note.
/// </remarks>
/// <param name="facades">The Schema area's facades.</param>
/// <param name="tree">The tree whose policy is read, read at query time.</param>
internal sealed class SchemaMemberSuggestionSource(SchemaFacades facades, Func<string?> tree) : ILtSuggestionSource
{
    /// <summary>The note shown when the policy cannot be read.</summary>
    public const string UnavailableReason = "The tree's policy could not be read, so no member is suggested.";

    private (string Tree, ShellCallerKey Caller, IReadOnlyList<LtSuggestion>? Members)? _remembered;

    /// <inheritdoc />
    public async ValueTask<LtSuggestionSet> SuggestAsync(string text, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(text);
        var treeId = tree();
        if (string.IsNullOrEmpty(treeId))
        {
            return LtSuggestionSet.Empty;
        }

        var caller = facades.Caller;
        if (_remembered is not { } remembered
            || !string.Equals(remembered.Tree, treeId, StringComparison.Ordinal)
            || remembered.Caller != caller)
        {
            IReadOnlyList<LtSuggestion>? members;
            try
            {
                var policy = await facades.RequireSchema().GetPolicyAsync(treeId, cancellationToken).ConfigureAwait(false);
                members = Members(policy?.Rules);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception)
            {
                members = null;
            }

            remembered = (treeId, caller, members);
            if (facades.Caller == caller)
            {
                _remembered = remembered;
            }
        }

        return remembered.Members is { } list
            ? SuggestionMatcher.Match(list, text, limit)
            : LtSuggestionSet.Unavailable(UnavailableReason);
    }

    /// <summary>Forgets what was read, so a saved policy's new members are suggested.</summary>
    public void Invalidate() => _remembered = null;

    private static IReadOnlyList<LtSuggestion> Members(IEnumerable<Orleans.Lattice.Schema.LatticeSchemaRule>? rules)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var members = new List<LtSuggestion>();
        foreach (var rule in rules ?? [])
        {
            if (!string.IsNullOrWhiteSpace(rule.MemberPath) && seen.Add(rule.MemberPath))
            {
                members.Add(new LtSuggestion(rule.MemberPath, "Named by the policy"));
            }
        }

        members.Sort(static (left, right) => string.CompareOrdinal(left.Value, right.Value));
        return members;
    }
}
