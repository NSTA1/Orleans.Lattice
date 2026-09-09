using System.Text;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Detects MCP tool-call framing that has leaked into a memory <c>body</c>, so the
/// write seam can refuse a malformed call instead of storing it and reporting
/// success.
/// </summary>
/// <remarks>
/// <para>
/// A malformed tool call serialises part of its own framing into the <c>body</c>
/// argument. Two shapes occur. The body may simply acquire trailing framing text,
/// which is untidy but loses nothing; or a <em>later argument is absorbed into the
/// body</em>, in which case the field that argument should have populated is left
/// empty and nothing anywhere records that it was ever supplied. The second shape
/// is the damaging one, because the entry reads as complete while its metadata is
/// silently missing.
/// </para>
/// <para>
/// This completes a rule the package already applies rather than introducing a new
/// one. The same <c>switch</c> that patches a memory record ends in
/// <c>default: throw UnknownField(...)</c>, and the link surface rejects a
/// malformed link target, so the seam already fails closed on an unknown field
/// <em>name</em> and on a malformed link <em>value</em>. It simply never inspected
/// a scalar field <em>value</em>.
/// </para>
/// <para>
/// <strong>Precision is the whole design.</strong> An entry that documents this
/// defect legitimately needs to discuss these tokens, and so does any note that
/// quotes HTML or XML, so a detector that merely looked for delimiter-like
/// characters would block exactly the documentation that explains the bug.
/// Detection therefore requires the full shape and anchors it to the tail: at
/// least two framing markers, one of them a closing tag, forming an unbroken run
/// that reaches the end of the body. A single token mentioned in prose does not
/// match, and neither does a markup sample that is followed by more prose.
/// </para>
/// <para>
/// This type recognises tags <em>structurally</em>, by walking characters, and so
/// contains no literal framing token of its own. That is deliberate: a detector
/// spelling out the sequences it detects would corrupt any entry that quoted it,
/// which is the very trap this guard exists to close.
/// </para>
/// </remarks>
internal static class RepoContextBodyFraming
{
    /// <summary>Names how a <c>remember</c> call supplied the offending body.</summary>
    internal const string RememberBodyLocation = "The 'body' argument";

    /// <summary>Names how an <c>update</c> call supplied the offending body.</summary>
    internal const string UpdateBodyLocation = "The 'body' entry of the 'fields' argument";

    /// <summary>
    /// The argument names of the <c>repocontext_remember</c> and
    /// <c>repocontext_update</c> tools. A tag naming one of these is framing;
    /// a tag naming anything else (an HTML or XML element, say) is not.
    /// </summary>
    private static readonly HashSet<string> ArgumentNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "repoId", "topic", "id", "kind", "title", "body", "author", "provenance",
        "tags", "addLinks", "removeLinks", "ttlSeconds", "fencingToken",
        "key", "fields", "addTags", "removeTags", "region",
    };

    /// <summary>
    /// Structural framing element names. These carry no argument name of their
    /// own but are unambiguous evidence of a serialized tool call.
    /// </summary>
    private static readonly HashSet<string> StructuralNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "parameter", "invoke", "function_calls",
    };

    /// <summary>
    /// The longest run of non-framing text tolerated between two framing markers,
    /// or after the last one, while still counting as part of a trailing framing
    /// run. A displaced argument value is short; ordinary prose is not.
    /// </summary>
    private const int MaxInterleavedValueLength = 2048;

    /// <summary>
    /// Inspects <paramref name="body"/> for a trailing run of tool-call framing.
    /// </summary>
    /// <param name="body">The candidate body. May be <see langword="null"/>.</param>
    /// <returns>
    /// The inspection outcome. A <see langword="null"/>, empty, or clean body is
    /// reported as not contaminated with no displaced arguments.
    /// </returns>
    internal static RepoContextBodyFramingInspection Inspect(string? body)
    {
        if (string.IsNullOrEmpty(body))
        {
            return new RepoContextBodyFramingInspection(false, []);
        }

        var markers = ScanMarkers(body);
        if (markers.Count < 2)
        {
            return new RepoContextBodyFramingInspection(false, []);
        }

        // The framing must reach the end of the body. Prose after the last marker
        // means the markers were quoted mid-body, not emitted by a malformed call.
        if (!IsFramingCompatible(body, markers[^1].End, body.Length))
        {
            return new RepoContextBodyFramingInspection(false, []);
        }

        // Walk backwards while consecutive markers stay separated only by short,
        // paragraph-free text. The earliest marker still reachable starts the run.
        var runStart = markers.Count - 1;
        while (runStart > 0
               && IsFramingCompatible(body, markers[runStart - 1].End, markers[runStart].Start))
        {
            runStart--;
        }

        var run = markers.GetRange(runStart, markers.Count - runStart);
        if (run.Count < 2 || !run.Exists(static marker => marker.IsClosing))
        {
            return new RepoContextBodyFramingInspection(false, []);
        }

        return new RepoContextBodyFramingInspection(true, CollectDisplaced(run));
    }

    /// <summary>
    /// Builds the rejection message for a contaminated body, naming the displaced
    /// arguments so the caller can see precisely what its malformed call dropped.
    /// </summary>
    /// <param name="location">
    /// How the offending body was supplied, so the caller can find it: the
    /// <c>'body'</c> argument of <c>remember</c>, or the <c>'body'</c> entry of the
    /// <c>'fields'</c> map of <c>update</c>.
    /// </param>
    /// <param name="displacedArguments">The displaced argument names, possibly empty.</param>
    /// <returns>A self-contained, actionable message.</returns>
    internal static string DescribeRejection(string location, IReadOnlyList<string> displacedArguments)
    {
        var message = new StringBuilder(location)
            .Append(" ends in MCP tool-call framing, which means the call was malformed");

        if (displacedArguments.Count > 0)
        {
            message.Append(" and the following argument(s) were absorbed into the body instead of being supplied separately: ")
                .Append(string.Join(", ", displacedArguments))
                .Append(". Those argument(s) were therefore NOT applied");
        }

        return message
            .Append(". Re-issue the call with each argument supplied separately. If the body is meant to "
                + "document this framing, describe its shape instead of reproducing it - a verbatim example "
                + "is indistinguishable from an instance of the defect.")
            .ToString();
    }

    /// <summary>
    /// The distinct argument names the run names, excluding the leading closing tag
    /// (which closes the body itself and so displaces nothing).
    /// </summary>
    private static List<string> CollectDisplaced(List<FramingMarker> run)
    {
        var displaced = new List<string>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var first = run[0].IsClosing ? 1 : 0;

        for (var i = first; i < run.Count; i++)
        {
            var name = run[i].ArgumentName;
            if (name is not null && seen.Add(name))
            {
                displaced.Add(name);
            }
        }

        return displaced;
    }

    /// <summary>
    /// Reports whether the text in <c>[start, end)</c> is short enough and free
    /// enough of paragraph breaks to be a displaced argument value rather than
    /// ordinary prose.
    /// </summary>
    private static bool IsFramingCompatible(string body, int start, int end)
    {
        if (end - start > MaxInterleavedValueLength)
        {
            return false;
        }

        var newlines = 0;
        for (var i = start; i < end; i++)
        {
            var c = body[i];
            if (c == '\n')
            {
                if (++newlines > 1)
                {
                    return false;
                }
            }
            else if (!char.IsWhiteSpace(c))
            {
                newlines = 0;
            }
        }

        return true;
    }

    /// <summary>Finds every framing marker in <paramref name="body"/>, in order.</summary>
    private static List<FramingMarker> ScanMarkers(string body)
    {
        var markers = new List<FramingMarker>();
        for (var i = 0; i < body.Length; i++)
        {
            if (body[i] != '<')
            {
                continue;
            }

            var close = body.IndexOf('>', i + 1);
            if (close < 0)
            {
                break;
            }

            // A tag cannot span a line; an unterminated '<' in prose must not
            // swallow the remainder of the body looking for a '>'.
            if (body.AsSpan(i, close - i).Contains('\n'))
            {
                continue;
            }

            if (TryClassify(body.AsSpan(i + 1, close - i - 1), out var isClosing, out var argumentName))
            {
                markers.Add(new FramingMarker(i, close + 1, isClosing, argumentName));
                i = close;
            }
        }

        return markers;
    }

    /// <summary>
    /// Classifies the inside of a tag as framing or not, and names the argument it
    /// refers to when it names one.
    /// </summary>
    private static bool TryClassify(ReadOnlySpan<char> inner, out bool isClosing, out string? argumentName)
    {
        isClosing = false;
        argumentName = null;

        inner = inner.Trim();
        if (inner.IsEmpty)
        {
            return false;
        }

        if (inner[0] == '/')
        {
            isClosing = true;
            inner = inner[1..].Trim();
        }

        // Split the element name from any attributes.
        var nameEnd = 0;
        while (nameEnd < inner.Length && !char.IsWhiteSpace(inner[nameEnd]))
        {
            nameEnd++;
        }

        var element = StripNamespace(inner[..nameEnd].Trim("\"'/"));
        if (element.IsEmpty)
        {
            return false;
        }

        if (ArgumentNames.Contains(element.ToString()))
        {
            argumentName = Canonical(element);
            return true;
        }

        if (!StructuralNames.Contains(element.ToString()))
        {
            return false;
        }

        // A structural element may name its argument in a name= attribute.
        argumentName = TryReadNameAttribute(inner[nameEnd..]);
        return true;
    }

    /// <summary>Reads the argument named by a <c>name=</c> attribute, when present and known.</summary>
    private static string? TryReadNameAttribute(ReadOnlySpan<char> attributes)
    {
        var at = attributes.IndexOf("name=", StringComparison.OrdinalIgnoreCase);
        if (at < 0)
        {
            return null;
        }

        var value = attributes[(at + "name=".Length)..].Trim();
        if (!value.IsEmpty && (value[0] == '"' || value[0] == '\''))
        {
            var quote = value[0];
            value = value[1..];
            var end = value.IndexOf(quote);
            value = end >= 0 ? value[..end] : value;
        }
        else
        {
            var end = 0;
            while (end < value.Length && !char.IsWhiteSpace(value[end]))
            {
                end++;
            }

            value = value[..end];
        }

        value = value.Trim("/\"'");
        return ArgumentNames.Contains(value.ToString()) ? Canonical(value) : null;
    }

    /// <summary>Drops any namespace prefix, so a namespaced framing tag still matches.</summary>
    private static ReadOnlySpan<char> StripNamespace(ReadOnlySpan<char> element)
    {
        var colon = element.LastIndexOf(':');
        return colon >= 0 ? element[(colon + 1)..] : element;
    }

    /// <summary>Returns the argument name in its canonical declared casing.</summary>
    private static string Canonical(ReadOnlySpan<char> element)
    {
        var candidate = element.ToString();
        foreach (var name in ArgumentNames)
        {
            if (string.Equals(name, candidate, StringComparison.OrdinalIgnoreCase))
            {
                return name;
            }
        }

        return candidate;
    }

    /// <summary>One recognised framing tag and where it sits in the body.</summary>
    /// <param name="Start">The index of the opening angle bracket.</param>
    /// <param name="End">The index just past the closing angle bracket.</param>
    /// <param name="IsClosing">Whether the tag is a closing tag.</param>
    /// <param name="ArgumentName">The argument the tag names, when it names one.</param>
    private readonly record struct FramingMarker(int Start, int End, bool IsClosing, string? ArgumentName);
}
