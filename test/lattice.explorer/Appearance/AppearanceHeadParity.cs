using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests.Appearance;

/// <summary>
/// Reads the two heads and the appearance assets off disk, and answers the one
/// structural question the no-flash requirement reduces to: does the palette
/// reach the document before anything can be painted?
/// </summary>
/// <remarks>
/// <para>
/// The question is genuinely structural, which is why it can be answered without
/// a browser. A classic <c>&lt;script&gt;</c> in <c>&lt;head&gt;</c> with neither
/// <c>defer</c> nor <c>async</c> blocks the parser, and the parser has not reached
/// <c>&lt;body&gt;</c>, so no box has been laid out and nothing has been painted
/// when it runs. Every way of losing that guarantee is a change to the tag or its
/// position: adding <c>defer</c>, adding <c>async</c>, moving it into
/// <c>&lt;body&gt;</c>, or dropping it from a head. Each is detected here.
/// </para>
/// <para>
/// The alternative - proving it in a real browser - can only ever demonstrate the
/// absence of a flash on the machine that ran it, and would live in the advisory
/// browser lane rather than in the required check.
/// </para>
/// </remarks>
internal static class AppearanceHeadParity
{
    /// <summary>The published path of the first-paint bootstrap script.</summary>
    public const string ScriptAsset = "_content/Orleans.Lattice.Explorer.UI/lattice-appearance.js";

    /// <summary>The published path of the appearance stylesheet.</summary>
    public const string StylesheetAsset = "_content/Orleans.Lattice.Explorer.UI/lattice-appearance.css";

    /// <summary>The call each head makes at the top of its body to stamp the chosen density.</summary>
    public const string BodyStampCall = "latticeAppearance.stamp()";

    private const string WebHeadPath = "src/lattice.explorer/WebHosting/Components/App.razor";
    private const string DesktopHeadPath = "src/lattice.explorer/Maui/wwwroot/index.html";
    private const string ScriptPath = "src/lattice.explorer/UI/wwwroot/lattice-appearance.js";
    private const string StylesheetPath = "src/lattice.explorer/UI/wwwroot/lattice-appearance.css";
    private const string TokenStylesheetPath = "src/lattice.explorer/DesignSystem/wwwroot/lattice-tokens.css";

    /// <summary>The web head's document.</summary>
    public static string WebHead() => Read(WebHeadPath);

    /// <summary>The desktop head's document.</summary>
    public static string DesktopHead() => Read(DesktopHeadPath);

    /// <summary>Every head that must honour the appearance choice, by name.</summary>
    public static IEnumerable<(string Name, string Source)> Heads()
    {
        yield return ("the web head (" + WebHeadPath + ")", WebHead());
        yield return ("the desktop head (" + DesktopHeadPath + ")", DesktopHead());
    }

    /// <summary>The first-paint bootstrap script.</summary>
    public static string Script() => Read(ScriptPath);

    /// <summary>The appearance stylesheet.</summary>
    public static string Stylesheet() => Read(StylesheetPath);

    /// <summary>The token layer, whose density presets the deferral rule must out-rank.</summary>
    public static string TokenStylesheet() => Read(TokenStylesheetPath);

    /// <summary>
    /// Whether <paramref name="head"/> applies the appearance before its first
    /// paint, and if not, why not.
    /// </summary>
    /// <param name="head">The head document to inspect.</param>
    /// <param name="reason">The specific defect, when the answer is no.</param>
    /// <returns><see langword="true"/> when the head cannot flash the wrong palette.</returns>
    public static bool AppliesBeforeFirstPaint(string head, out string reason)
    {
        // Comments are dropped first: both heads explain in prose beside the tag
        // exactly what must not be done to it, and prose naming `defer` or
        // `<body>` is not a defect.
        head = WithoutComments(head);

        var asset = head.IndexOf(ScriptAsset, StringComparison.Ordinal);
        if (asset < 0)
        {
            reason = "it does not load " + ScriptAsset + " at all, so the palette is only applied after hydration";
            return false;
        }

        var headEnd = head.IndexOf("</head", StringComparison.OrdinalIgnoreCase);
        if (headEnd < 0 || asset > headEnd)
        {
            reason = "the bootstrap script is outside <head>, so the document has already begun painting when it runs";
            return false;
        }

        var tag = ScriptTag(head, asset);

        if (tag.Contains("defer", StringComparison.OrdinalIgnoreCase))
        {
            reason = "the bootstrap script is deferred, so it runs after the document has been parsed and painted";
            return false;
        }

        if (tag.Contains("async", StringComparison.OrdinalIgnoreCase))
        {
            reason = "the bootstrap script is async, so whether it beats the first paint is a race";
            return false;
        }

        if (tag.Contains("type=\"module\"", StringComparison.OrdinalIgnoreCase))
        {
            reason = "the bootstrap script is a module, and a module is deferred by definition";
            return false;
        }

        reason = string.Empty;
        return true;
    }

    /// <summary>
    /// Whether <paramref name="head"/> stamps the chosen density at the top of its
    /// body, before any shell content is parsed.
    /// </summary>
    /// <param name="head">The head document to inspect.</param>
    /// <returns><see langword="true"/> when the density stamp precedes the shell content.</returns>
    public static bool StampsDensityBeforeContent(string head)
    {
        head = WithoutComments(head);

        var bodyStart = head.IndexOf("<body", StringComparison.OrdinalIgnoreCase);
        var stamp = head.IndexOf(BodyStampCall, StringComparison.Ordinal);

        if (bodyStart < 0 || stamp < bodyStart)
        {
            return false;
        }

        // Everything between the body tag and the stamp's own element must be
        // whitespace: markup in between is content whose layout the stamp exists
        // to precede.
        var between = head[(head.IndexOf('>', bodyStart) + 1)..head.LastIndexOf('<', stamp)];
        return between.Trim().Length == 0;
    }

    private static string ScriptTag(string head, int assetIndex)
    {
        var start = head.LastIndexOf("<script", assetIndex, StringComparison.OrdinalIgnoreCase);
        var end = head.IndexOf('>', assetIndex);

        return start < 0 || end < 0 ? string.Empty : head[start..(end + 1)];
    }

    private static string WithoutComments(string markup)
    {
        // Both comment syntaxes the two heads use, folded onto one pair of
        // markers so a single pass can drop what lies between them.
        var marked = markup
            .Replace("<!--", "\u0001", StringComparison.Ordinal)
            .Replace("-->", "\u0002", StringComparison.Ordinal)
            .Replace("@*", "\u0001", StringComparison.Ordinal)
            .Replace("*@", "\u0002", StringComparison.Ordinal);

        var builder = new System.Text.StringBuilder(marked.Length);
        var depth = 0;

        foreach (var c in marked)
        {
            if (c == '\u0001')
            {
                depth++;
            }
            else if (c == '\u0002')
            {
                depth--;
            }
            else if (depth == 0)
            {
                builder.Append(c);
            }
        }

        return builder.ToString();
    }

    private static string Read(string relativePath)
    {
        var path = Path.Combine(
            HygieneRepository.FindRepoRoot(),
            relativePath.Replace('/', Path.DirectorySeparatorChar));

        Assert.That(File.Exists(path), Is.True, relativePath + " must exist");

        return File.ReadAllText(path);
    }
}
