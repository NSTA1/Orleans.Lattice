using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Reads and checks the <c>## Deliberate abstraction gaps</c> section of a
/// refinement note: the register of what a module does NOT model (#5156).
/// <para>
/// WHY A REGISTER AND NOT PROSE. A gap is a claim about production code - "this
/// code is not covered by the model". Free prose cannot be connected to a code
/// change: <c>spec/shard-ownership/Refinement.md</c> listed the shard fold as
/// unmodelled while real defects were fixed in
/// <c>TreeShardConsolidationGrain</c> (#5126, #5131), and nothing linked the
/// two. So every gap names the production symbols it is about, in a clause the
/// gate can resolve, or says why there is none.
/// </para>
/// <para>
/// THE FORM. The section may open with introductory prose. After that it is a
/// list of top-level bullets, one per gap, each a single paragraph (continuation
/// lines indented, no blank line inside a bullet):
/// </para>
/// <code>
/// - **&lt;Gap title&gt;.** &lt;prose&gt;
///   Production symbols: `Type`, `Type.Member`.
/// </code>
/// <para>
/// or, when the gap is about the model rather than about a production mechanism,
/// a last line <c>No production symbol: &lt;reason&gt;.</c> The clause starts its
/// own line and runs to the end of the bullet. This is the shape the refinement
/// index (#5155) reads: one gap per top-level bullet, the bold lead-in as the
/// title, and the backticked symbols that resolve under <c>src/</c>.
/// </para>
/// <para>
/// WHAT IS CHECKED. Every bullet parses; titles are unique within a note; the
/// clause is present and well formed; every symbol it names resolves through
/// <see cref="RefinementSymbolResolver"/>; and every production symbol the
/// prose names in backticks (a <c>Type.Member</c> or a bare type declared under
/// <c>src/</c>) is listed in the clause, so the clause is the whole register and
/// a <c>No production symbol</c> gap cannot quietly name one. A dotted name
/// whose type is not declared under <c>src/</c> (a test, a Coyote model, a file
/// name) is not a production symbol and is left alone.
/// </para>
/// </summary>
internal static class RefinementGapRegister
{
    /// <summary>The heading of the section, without its <c>##</c>.</summary>
    public const string SectionHeading = "Deliberate abstraction gaps";

    /// <summary>The marker that opens a clause naming production symbols.</summary>
    public const string SymbolsMarker = "Production symbols:";

    /// <summary>The marker that opens a clause stating why no symbol is named.</summary>
    public const string NoSymbolMarker = "No production symbol:";

    /// <summary>The shortest reason accepted after <see cref="NoSymbolMarker"/>.</summary>
    public const int MinimumReasonLength = 20;

    private static readonly Regex Heading = new(@"^#{1,6}\s", RegexOptions.Compiled);

    private static readonly Regex NearMissHeading = new(
        @"^#{1,6}\s.*abstraction\s+gap",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex BulletHead = new(
        @"^- \*\*(?<title>(?:(?!\*\*).)+?)\.\*\*(?:\s|$)",
        RegexOptions.Compiled);

    private static readonly Regex BacktickedSpan = new("`([^`]+)`", RegexOptions.Compiled);

    private static readonly Regex SymbolList = new(
        @"^(?:`(?<s>[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)`)"
        + @"(?:,\s*`(?<s>[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)`)*\.$",
        RegexOptions.Compiled);

    // The production-column shape of RefinementCodeSymbols: both halves start
    // upper-case, so file names such as `WalMove.tla` are never read as code.
    private static readonly Regex DottedReference = new(
        @"(?<![A-Za-z0-9_.])([A-Z][A-Za-z0-9_]*)\.([A-Z_][A-Za-z0-9_]*)(?![A-Za-z0-9_.])",
        RegexOptions.Compiled);

    private static readonly Regex BareType = new(@"^[A-Z][A-Za-z0-9_]*$", RegexOptions.Compiled);

    /// <summary>
    /// Parses the gap section of <paramref name="markdown"/>. Returns
    /// <see langword="null"/> when the note has no such section; otherwise the
    /// gaps it holds, with every format problem in <paramref name="problems"/>.
    /// Symbol resolution is <see cref="Check"/>'s job, not this one's.
    /// </summary>
    public static IReadOnlyList<RefinementGapEntry>? Parse(
        string markdown,
        string noteLabel,
        out IReadOnlyList<string> problems)
    {
        ArgumentNullException.ThrowIfNull(markdown);
        ArgumentException.ThrowIfNullOrEmpty(noteLabel);

        var found = new List<string>();
        problems = found;

        var lines = markdown.ReplaceLineEndings("\n").Split('\n');
        var start = -1;
        for (var i = 0; i < lines.Length; i++)
        {
            var line = lines[i].TrimEnd();
            if (line == "## " + SectionHeading)
            {
                if (start >= 0)
                {
                    found.Add($"{noteLabel}:{i + 1}: a second '## {SectionHeading}' section; a note has at most one.");
                }
                else
                {
                    start = i;
                }
            }
            else if (NearMissHeading.IsMatch(line))
            {
                found.Add($"{noteLabel}:{i + 1}: heading '{line}' is not the canonical '## {SectionHeading}', so no gate reads it.");
            }
        }

        if (start < 0)
        {
            return null;
        }

        var bullets = new List<List<(int Line, string Text)>>();
        List<(int Line, string Text)>? current = null;
        var afterBlank = false;

        for (var i = start + 1; i < lines.Length; i++)
        {
            var raw = lines[i].TrimEnd();
            if (Heading.IsMatch(raw))
            {
                break;
            }

            if (raw.Length == 0)
            {
                afterBlank = true;
                continue;
            }

            if (raw.StartsWith("- ", StringComparison.Ordinal))
            {
                current = new List<(int, string)> { (i + 1, raw) };
                bullets.Add(current);
                afterBlank = false;
                continue;
            }

            if (current is null)
            {
                if (raw.StartsWith("* ", StringComparison.Ordinal) || raw.StartsWith("+ ", StringComparison.Ordinal))
                {
                    found.Add($"{noteLabel}:{i + 1}: a gap bullet must start with '- '.");
                }

                // Introductory prose before the first gap.
                continue;
            }

            if (!char.IsWhiteSpace(raw[0]))
            {
                found.Add($"{noteLabel}:{i + 1}: prose after the first gap bullet that belongs to no bullet; "
                    + "make it a gap of its own, indent it into the gap it qualifies, or move it above the list.");
                continue;
            }

            if (afterBlank)
            {
                found.Add($"{noteLabel}:{i + 1}: a gap bullet must be one paragraph; remove the blank line before this continuation.");
            }

            current.Add((i + 1, raw.Trim()));
            afterBlank = false;
        }

        if (bullets.Count == 0)
        {
            found.Add($"{noteLabel}: '## {SectionHeading}' holds no '- **Title.** ...' gap bullet.");
            return Array.Empty<RefinementGapEntry>();
        }

        var entries = new List<RefinementGapEntry>();
        var titles = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        foreach (var bullet in bullets)
        {
            var entry = ParseBullet(bullet, noteLabel, found);
            if (entry is null)
            {
                continue;
            }

            if (titles.TryGetValue(entry.Title, out var first))
            {
                found.Add($"{noteLabel}:{entry.LineNumber}: gap title '{entry.Title}' repeats the gap on line {first}; titles are unique within a note.");
            }
            else
            {
                titles[entry.Title] = entry.LineNumber;
            }

            entries.Add(entry);
        }

        return entries;
    }

    /// <summary>
    /// Every problem with the gap section of <paramref name="markdown"/>: the
    /// format problems <see cref="Parse"/> reports, then every symbol that does
    /// not resolve and every production symbol the prose names outside the
    /// clause. Empty for a note with no gap section.
    /// </summary>
    public static IReadOnlyList<string> Check(string markdown, string noteLabel, RefinementSymbolResolver resolver)
    {
        ArgumentNullException.ThrowIfNull(resolver);

        var entries = Parse(markdown, noteLabel, out var formatProblems);
        var problems = new List<string>(formatProblems);
        if (entries is null)
        {
            return problems;
        }

        foreach (var entry in entries)
        {
            var where = $"{noteLabel}:{entry.LineNumber}: gap '{entry.Title}'";

            foreach (var symbol in entry.Symbols)
            {
                var dot = symbol.IndexOf('.', StringComparison.Ordinal);
                if (dot < 0)
                {
                    if (!resolver.TypeExists(symbol))
                    {
                        problems.Add($"{where} names `{symbol}`, but no type of that name is declared under src/.");
                    }

                    continue;
                }

                var resolution = resolver.Resolve(symbol[..dot], symbol[(dot + 1)..]);
                if (!RefinementSymbolResolver.IsResolved(resolution))
                {
                    problems.Add(resolution == RefinementSymbolResolution.UnknownType
                        ? $"{where} names `{symbol}`, but no type `{symbol[..dot]}` is declared under src/."
                        : $"{where} names `{symbol}`, but `{symbol[..dot]}` declares nothing called `{symbol[(dot + 1)..]}`.");
                }
            }

            var named = new HashSet<string>(entry.Symbols, StringComparer.Ordinal);
            var text = entry.NoSymbolReason is null ? entry.Prose : entry.Prose + " " + entry.NoSymbolReason;
            foreach (var symbol in ProseSymbols(text, resolver))
            {
                if (!named.Contains(symbol))
                {
                    problems.Add(entry.NoSymbolReason is null
                        ? $"{where} names production symbol `{symbol}` in its prose but not in its '{SymbolsMarker}' clause."
                        : $"{where} names production symbol `{symbol}` but says '{NoSymbolMarker}'; list it in a '{SymbolsMarker}' clause instead.");
                }
            }
        }

        return problems;
    }

    /// <summary>
    /// The production symbols <paramref name="text"/> names in backticks: every
    /// <c>Type.Member</c> whose type is declared under <c>src/</c>, and every
    /// bare backticked name that is such a type. Distinct, in document order.
    /// </summary>
    public static IReadOnlyList<string> ProseSymbols(string text, RefinementSymbolResolver resolver)
    {
        ArgumentNullException.ThrowIfNull(text);
        ArgumentNullException.ThrowIfNull(resolver);

        var symbols = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);

        foreach (Match span in BacktickedSpan.Matches(text))
        {
            var content = span.Groups[1].Value;
            if (BareType.IsMatch(content))
            {
                if (resolver.TypeExists(content) && seen.Add(content))
                {
                    symbols.Add(content);
                }

                continue;
            }

            foreach (Match reference in DottedReference.Matches(content))
            {
                if (resolver.TypeExists(reference.Groups[1].Value) && seen.Add(reference.Value))
                {
                    symbols.Add(reference.Value);
                }
            }
        }

        return symbols;
    }

    private static RefinementGapEntry? ParseBullet(List<(int Line, string Text)> bullet, string noteLabel, List<string> problems)
    {
        var lineNumber = bullet[0].Line;
        var joined = string.Join(" ", bullet.Select(l => l.Text));

        var head = BulletHead.Match(joined);
        if (!head.Success)
        {
            problems.Add($"{noteLabel}:{lineNumber}: a gap bullet must open with a bold title ending in a full stop, '- **Title.** ...'.");
            return null;
        }

        var title = head.Groups["title"].Value.Trim();

        if (MentionsClause(bullet[0].Text))
        {
            problems.Add($"{noteLabel}:{lineNumber}: gap '{title}' opens its symbol clause on the title line; start it on a line of its own.");
        }

        var clauseLine = -1;
        for (var i = 1; i < bullet.Count; i++)
        {
            var text = bullet[i].Text;
            if (clauseLine < 0
                && (text.StartsWith(SymbolsMarker, StringComparison.Ordinal)
                    || text.StartsWith(NoSymbolMarker, StringComparison.Ordinal)))
            {
                clauseLine = i;
                continue;
            }

            if (MentionsClause(text))
            {
                problems.Add($"{noteLabel}:{bullet[i].Line}: gap '{title}' has a second or misplaced symbol clause; "
                    + "a gap has one, starting its own line, as its last part.");
            }
        }

        if (clauseLine < 0)
        {
            problems.Add($"{noteLabel}:{lineNumber}: gap '{title}' has no symbol clause: end it with a line "
                + $"'{SymbolsMarker} `Type`, `Type.Member`.' or '{NoSymbolMarker} <reason>.'");
            return new RefinementGapEntry(lineNumber, title, joined, Array.Empty<string>(), null);
        }

        var prose = string.Join(" ", bullet.Take(clauseLine).Select(l => l.Text));
        var clause = string.Join(" ", bullet.Skip(clauseLine).Select(l => l.Text));

        if (clause.StartsWith(NoSymbolMarker, StringComparison.Ordinal))
        {
            var reason = clause[NoSymbolMarker.Length..].Trim();
            if (reason.Length < MinimumReasonLength || !reason.EndsWith('.'))
            {
                problems.Add($"{noteLabel}:{bullet[clauseLine].Line}: gap '{title}' says '{NoSymbolMarker}' "
                    + $"without a reason: state why, in at least {MinimumReasonLength} characters ending with a full stop.");
            }

            return new RefinementGapEntry(lineNumber, title, prose, Array.Empty<string>(), reason);
        }

        var list = SymbolList.Match(clause[SymbolsMarker.Length..].Trim());
        if (!list.Success)
        {
            problems.Add($"{noteLabel}:{bullet[clauseLine].Line}: gap '{title}' has a malformed '{SymbolsMarker}' clause; "
                + "write the symbols as backticked `Type` or `Type.Member`, separated by commas, ending with a full stop.");
            return new RefinementGapEntry(lineNumber, title, prose, Array.Empty<string>(), null);
        }

        var symbols = list.Groups["s"].Captures.Select(c => c.Value).ToArray();
        foreach (var duplicate in symbols.GroupBy(s => s, StringComparer.Ordinal).Where(g => g.Count() > 1))
        {
            problems.Add($"{noteLabel}:{bullet[clauseLine].Line}: gap '{title}' lists `{duplicate.Key}` more than once.");
        }

        return new RefinementGapEntry(lineNumber, title, prose, symbols, null);
    }

    private static bool MentionsClause(string text) =>
        text.Contains(SymbolsMarker, StringComparison.Ordinal)
        || text.Contains(NoSymbolMarker, StringComparison.Ordinal);
}
