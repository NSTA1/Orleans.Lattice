using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Reads the protocol actions out of <c>spec/AtomicCommit.tla</c>: the names
/// the <c>Next</c> relation disjoins, and the text of each one's definition.
/// <para>
/// String work over the module, deliberately. A TLA+ parser would be more
/// general and much harder to review, and the module is written in one
/// consistent layout that these few patterns follow exactly. Every caller
/// asserts its result is non-empty, so a layout change that defeats a pattern
/// fails loudly rather than leaving a gate checking nothing.
/// </para>
/// </summary>
internal static class SpecActions
{
    private static readonly Regex NextHeader = new(@"^Next\s*==\s*$", RegexOptions.Multiline);

    private static readonly Regex Identifier = new(@"^([A-Za-z][A-Za-z0-9_]*)");

    /// <summary>
    /// The action names <c>Next</c> disjoins, in declaration order, with any
    /// quantifier prefix (<c>\E t \in Txns :</c>) and argument list stripped.
    /// </summary>
    public static IReadOnlyList<string> ReadNextActions(string specification)
    {
        ArgumentNullException.ThrowIfNull(specification);

        var text = specification.ReplaceLineEndings("\n");
        var header = NextHeader.Match(text);
        if (!header.Success)
        {
            throw new InvalidOperationException("the specification declares no 'Next ==' relation.");
        }

        var actions = new List<string>();
        foreach (var line in text[(header.Index + header.Length)..].Split('\n').Skip(1))
        {
            var trimmed = line.Trim();
            if (trimmed.Length == 0)
            {
                break;
            }

            if (!trimmed.StartsWith(@"\/", StringComparison.Ordinal))
            {
                continue;
            }

            var disjunct = trimmed[2..];
            var colon = disjunct.LastIndexOf(':');
            var name = Identifier.Match((colon >= 0 ? disjunct[(colon + 1)..] : disjunct).Trim());
            if (name.Success)
            {
                actions.Add(name.Groups[1].Value);
            }
        }

        if (actions.Count == 0)
        {
            throw new InvalidOperationException("parsed no disjuncts out of the specification's Next relation.");
        }

        return actions;
    }

    /// <summary>
    /// The text of an action's definition: its header line through to the next
    /// blank line. Throws when the action is not defined exactly once, which is
    /// what makes a containment check against it meaningful.
    /// </summary>
    public static string ReadDefinition(string specification, string action)
    {
        ArgumentNullException.ThrowIfNull(specification);
        ArgumentException.ThrowIfNullOrEmpty(action);

        var text = specification.ReplaceLineEndings("\n");
        var headers = Regex.Matches(
            text,
            $@"^{Regex.Escape(action)}(\([^)]*\))?\s*==",
            RegexOptions.Multiline);

        if (headers.Count != 1)
        {
            throw new InvalidOperationException(
                $"expected exactly one definition of '{action}' in the specification, found {headers.Count}.");
        }

        var start = headers[0].Index;
        var end = text.IndexOf("\n\n", start, StringComparison.Ordinal);
        return end < 0 ? text[start..] : text[start..end];
    }

    /// <summary>
    /// The spec construct a mapping-table row is about, from its first cell:
    /// <c>`BroadcastStep(t,k)`</c> becomes <c>BroadcastStep</c>.
    /// </summary>
    public static string ActionNameOf(RefinementRow row)
    {
        ArgumentNullException.ThrowIfNull(row);

        var match = Identifier.Match(row.Label.Trim().Trim('`'));
        return match.Success ? match.Groups[1].Value : string.Empty;
    }
}
