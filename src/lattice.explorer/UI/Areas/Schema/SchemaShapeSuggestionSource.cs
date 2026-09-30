using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Suggestions;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// Suggestions drawn from an inferred shape, read at query time: the member
/// paths below a scope (the whole value, or a list's items), or the distinct
/// values seen at one member. Local and instant; nothing is read from the cluster.
/// </summary>
/// <param name="read">Reads the suggestions to match against, or <see langword="null"/> when none are known.</param>
internal sealed class SchemaShapeSuggestionSource(Func<IReadOnlyList<LtSuggestion>?> read) : ILtSuggestionSource
{
    /// <summary>The member paths below <paramref name="scope"/>, outside any list it holds.</summary>
    /// <param name="scope">Reads the scope.</param>
    /// <returns>The source.</returns>
    public static SchemaShapeSuggestionSource Members(Func<SchemaShapeNode?> scope) => new(() => scope() is { } node ? MembersOf(node) : null);

    /// <summary>The distinct values seen at a member.</summary>
    /// <param name="member">Reads the member.</param>
    /// <returns>The source.</returns>
    public static SchemaShapeSuggestionSource Values(Func<SchemaShapeNode?> member) =>
        new(() => member() is { Unseen: false } node ? [.. node.Distinct.Select(value => new LtSuggestion(value, "Seen in the sample"))] : null);

    /// <inheritdoc />
    public ValueTask<LtSuggestionSet> SuggestAsync(string text, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(text);
        return ValueTask.FromResult(read() is { } values ? SuggestionMatcher.Match(values, text, limit) : LtSuggestionSet.Empty);
    }

    /// <summary>Every member path below <paramref name="scope"/>, stopping at lists (their items are another scope).</summary>
    /// <param name="scope">The scope.</param>
    /// <returns>The suggestions, by path.</returns>
    internal static IReadOnlyList<LtSuggestion> MembersOf(SchemaShapeNode scope)
    {
        var members = new List<LtSuggestion>();
        var pending = new Stack<SchemaShapeNode>(scope.Children.Values.Reverse());
        while (pending.Count > 0)
        {
            var node = pending.Pop();
            members.Add(new LtSuggestion(node.Path, Detail(node)));
            foreach (var child in node.Children.Values.Reverse())
            {
                pending.Push(child);
            }
        }

        members.Sort(static (left, right) => string.CompareOrdinal(left.Value, right.Value));
        return members;
    }

    private static string Detail(SchemaShapeNode node) =>
        node.Dominant is { } type
            ? SchemaCardText.TypePhrase(type)
            : node.NamedByPolicy ? "Named by the policy" : "Not seen";
}
