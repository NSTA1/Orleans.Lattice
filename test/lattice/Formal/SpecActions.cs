using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Reads the protocol actions out of a module under <c>spec/</c>: the names
/// the <c>Next</c> relation disjoins, and the text of each one's definition.
/// <para>
/// String work over the module, deliberately. A TLA+ parser would be more
/// general and much harder to review, and the modules are written in one
/// consistent layout that these few patterns follow exactly (see spec/README.md). Every caller
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

    /// <summary>
    /// Whether <paramref name="mutation"/> really perturbs
    /// <paramref name="action"/>: at least one of its edits anchors inside the
    /// action's definition AND changes something other than TLA+ comments
    /// there. This is the check behind every <c>PERTURBS:</c> header.
    /// </summary>
    public static bool MutationPerturbs(string specification, string action, SpecMutation mutation)
    {
        ArgumentNullException.ThrowIfNull(mutation);

        var definition = ReadDefinition(specification, action);
        return mutation.Edits.Any(edit => EditPerturbs(definition, edit));
    }

    /// <summary>
    /// Whether one edit perturbs the action whose definition text is
    /// <paramref name="definition"/>.
    /// <para>
    /// Containment alone is not enough, and was once all this checked. An edit
    /// whose anchor lies inside the action but whose replacement only adds a
    /// comment there satisfies containment while changing nothing, so a
    /// mutation could pair that edit with its real change somewhere else and
    /// still claim the action. Comparing the two sides with comments stripped
    /// closes that: the edit has to change what TLC actually reads.
    /// </para>
    /// </summary>
    public static bool EditPerturbs(string definition, SpecEdit edit)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(edit);

        var normalisedDefinition = definition.ReplaceLineEndings("\n");
        var find = edit.Find.ReplaceLineEndings("\n");
        return normalisedDefinition.Contains(find, StringComparison.Ordinal)
            && !string.Equals(StripComments(find), StripComments(edit.Replace), StringComparison.Ordinal);
    }

    /// <summary>
    /// TLA+ text with its comments removed - <c>\*</c> to end of line and
    /// <c>(* ... *)</c> blocks, which TLA+ allows to nest - and then with
    /// trailing whitespace and blank lines dropped, so that only what TLC
    /// parses is compared. String literals are copied verbatim, so a comment
    /// marker inside one is not mistaken for a comment.
    /// </summary>
    public static string StripComments(string tla)
    {
        ArgumentNullException.ThrowIfNull(tla);

        var text = tla.ReplaceLineEndings("\n");
        var output = new System.Text.StringBuilder(text.Length);
        var depth = 0;
        var i = 0;
        while (i < text.Length)
        {
            var current = text[i];
            var next = i + 1 < text.Length ? text[i + 1] : '\0';

            if (depth > 0)
            {
                if (current == '(' && next == '*')
                {
                    depth++;
                    i += 2;
                }
                else if (current == '*' && next == ')')
                {
                    depth--;
                    i += 2;
                }
                else
                {
                    // A newline inside a block comment is kept so line
                    // structure, which TLA+ conjunction lists depend on,
                    // survives the strip.
                    if (current == '\n')
                    {
                        output.Append('\n');
                    }

                    i++;
                }

                continue;
            }

            if (current == '"')
            {
                var end = i + 1;
                while (end < text.Length && text[end] != '"' && text[end] != '\n')
                {
                    end += text[end] == '\\' ? 2 : 1;
                }

                end = Math.Min(end + 1, text.Length);
                output.Append(text, i, end - i);
                i = end;
            }
            else if (current == '\\' && next == '*')
            {
                while (i < text.Length && text[i] != '\n')
                {
                    i++;
                }
            }
            else if (current == '(' && next == '*')
            {
                depth = 1;
                i += 2;
            }
            else
            {
                output.Append(current);
                i++;
            }
        }

        return string.Join(
            "\n",
            output.ToString().Split('\n').Select(line => line.TrimEnd()).Where(line => line.Length > 0));
    }
}
