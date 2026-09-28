using System.Diagnostics.Metrics;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Testing.Metrics;
/// <summary>
/// The source-derived registry of every instrument declared anywhere under
/// <c>src/</c>, keyed by its canonical dotted name and carrying the factory kind
/// that declares it.
/// </summary>
/// <remarks>
/// <para>
/// The registry is built by parsing source rather than by reflecting over live
/// instruments, because many instruments - observable gauges in particular - are
/// created only when the host starts the subsystem that owns them, so a snapshot
/// <see cref="MeterListener"/> at test time never sees them. Parsing the
/// declaration covers the lazily-created and the eagerly-created alike, and does
/// so uniformly.
/// </para>
/// <para>
/// <b>The mapping is built forward, never reverse.</b> The exporter translates
/// both <c>'.'</c> and any underscore already present in the .NET name into
/// <c>'_'</c>, so a Prometheus token cannot be parsed back into a dotted name:
/// <c>orleans_lattice_wal_gc_passes_total</c> is consistent with several distinct
/// dotted names and the mangling is not injective. Every lookup here therefore
/// generates the candidate Prometheus forms from a known dotted name and matches
/// tokens against that generated set.
/// </para>
/// </remarks>
public static class DeclaredInstruments
{
    private static readonly Regex CreateRegex = new(
        @"Create(?<kind>Histogram|UpDownCounter|Counter|ObservableGauge|ObservableCounter|ObservableUpDownCounter)"
        + @"\s*(?:<[^>()]*>)?\s*(?<open>\()\s*(?<arg>@?""(?:[^""\\]|\\.)*""|[A-Za-z_][A-Za-z0-9_.]*)",
        RegexOptions.Compiled);

    private static readonly Regex NamedUnitRegex = new(
        @"^unit\s*:\s*""(?<u>[^""\\]*)""$",
        RegexOptions.Compiled);

    private static readonly Regex ConstRegex = new(
        @"const\s+string\s+(?<id>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*""(?<val>[^""\\]*)""\s*;",
        RegexOptions.Compiled);

    private static readonly Lazy<Registry> RegistryLazy = new(Build, isThreadSafe: true);

    /// <summary>Every declared instrument, keyed by canonical dotted name.</summary>
    public static IReadOnlyDictionary<string, DeclaredInstrumentKind> ByDottedName => RegistryLazy.Value.ByDottedName;

    /// <summary>
    /// Declarations whose name argument could not be resolved to a literal. This
    /// must stay empty: an unresolved declaration is an instrument the gates
    /// silently stop covering, so it is asserted rather than tolerated.
    /// </summary>
    public static IReadOnlyList<string> Unresolved => RegistryLazy.Value.Unresolved;

    /// <summary>The number of <c>Create*</c> declarations the scan matched.</summary>
    public static int DeclarationCount => RegistryLazy.Value.DeclarationCount;

    /// <summary>
    /// The unit string each instrument declares, keyed by canonical dotted name.
    /// An instrument that declares no unit maps to the empty string.
    /// </summary>
    /// <remarks>
    /// The unit is read from the declaration's argument list, which is located by
    /// balancing parentheses from the factory call rather than by matching a
    /// trailing anchor such as <c>description:</c>. That distinction is
    /// load-bearing: an anchored pattern reads only the declarations shaped the
    /// way its author happened to look at, and silently reports every other
    /// declaration as <i>no unit</i> rather than as <i>not read</i>. Four
    /// instruments in <c>src/</c> supply the unit positionally, and an anchored
    /// scan misses all four while reporting a clean result.
    /// </remarks>
    public static IReadOnlyDictionary<string, string> UnitByDottedName => RegistryLazy.Value.UnitByDottedName;

    /// <summary>
    /// Declarations whose argument list could not be read to its closing
    /// parenthesis, so the declared unit is <b>unknown</b> rather than absent.
    /// Asserted empty, because a parser that classifies what it could not read as
    /// "no unit" reports its own depth as the repository's content.
    /// </summary>
    public static IReadOnlyList<string> UnitUnresolved => RegistryLazy.Value.UnitUnresolved;

    /// <summary>
    /// The number of instruments whose unit was supplied positionally rather than
    /// with a <c>unit:</c> label. Asserted non-zero, so that the parser's ability
    /// to read positional units stays proven rather than assumed.
    /// </summary>
    public static int PositionalUnitCount => RegistryLazy.Value.PositionalUnitCount;

    private static Registry Build()
    {
        var root = HygieneRepository.FindRepoRoot();
        var src = Path.Combine(root, "src");

        var constants = new Dictionary<string, string>(StringComparer.Ordinal);
        var ambiguous = new HashSet<string>(StringComparer.Ordinal);
        var files = new List<(string Path, string Text)>();

        foreach (var file in HygieneRepository.EnumerateFiles(src, "*.cs"))
        {
            var text = File.ReadAllText(file);
            files.Add((file, text));

            foreach (Match m in ConstRegex.Matches(text))
            {
                var id = m.Groups["id"].Value;
                var val = m.Groups["val"].Value;
                if (constants.TryGetValue(id, out var existing))
                {
                    if (!string.Equals(existing, val, StringComparison.Ordinal))
                    {
                        ambiguous.Add(id);
                    }
                }
                else
                {
                    constants[id] = val;
                }
            }
        }

        // The denominator of the scan, asserted here rather than left to each
        // consumer. EnumerateFiles yields nothing for a root that does not exist,
        // so a moved or renamed src/ reduces this registry to empty silently - and
        // an empty registry makes every gate built on it report a clean pass having
        // examined no instrument at all. Only one of the consuming fixtures floors
        // DeclarationCount, so without this the rest are vacuous whenever they are
        // selected on their own.
        HygieneDenominator.RequireExamined(
            files.Count,
            nameof(DeclaredInstruments),
            "source files",
            src);

        var byDotted = new Dictionary<string, DeclaredInstrumentKind>(StringComparer.Ordinal);
        var unitByDotted = new Dictionary<string, string>(StringComparer.Ordinal);
        var unresolved = new List<string>();
        var unitUnresolved = new List<string>();
        var positionalUnits = 0;
        var count = 0;

        foreach (var (path, text) in files)
        {
            foreach (Match m in CreateRegex.Matches(text))
            {
                count++;
                var kind = Enum.Parse<DeclaredInstrumentKind>(m.Groups["kind"].Value);
                var arg = m.Groups["arg"].Value;
                var dotted = ResolveName(arg, constants, ambiguous);

                if (dotted is null)
                {
                    unresolved.Add($"{Path.GetFileName(path)}: Create{kind}(... {arg} ...)");
                    continue;
                }

                // A name declared twice must agree on its kind; the exporter would
                // otherwise emit two conflicting "# TYPE" lines for one family.
                if (byDotted.TryGetValue(dotted, out var existing) && existing != kind)
                {
                    unresolved.Add($"{dotted}: declared as both {existing} and {kind}");
                    continue;
                }

                byDotted[dotted] = kind;

                var arguments = ReadArgumentList(text, m.Groups["open"].Index);
                if (arguments is null)
                {
                    unitUnresolved.Add($"{Path.GetFileName(path)}: {dotted} - argument list did not close");
                    continue;
                }

                unitByDotted[dotted] = ResolveUnit(SplitTopLevelArguments(arguments), kind, ref positionalUnits);
            }
        }

        unresolved.Sort(StringComparer.Ordinal);
        unitUnresolved.Sort(StringComparer.Ordinal);
        return new Registry(byDotted, unitByDotted, unresolved, unitUnresolved, count, positionalUnits);
    }

    /// <summary>
    /// Returns the text between the parenthesis at <paramref name="openIndex"/> and
    /// its match, or <see langword="null"/> when the list does not close. String
    /// literals, character literals, and comments are skipped so that a parenthesis
    /// inside one cannot unbalance the scan.
    /// </summary>
    private static string? ReadArgumentList(string text, int openIndex)
    {
        var depth = 0;

        for (var i = openIndex; i < text.Length; i++)
        {
            var c = text[i];

            if (c == '"')
            {
                i = SkipStringLiteral(text, i);
                if (i < 0)
                {
                    return null;
                }

                continue;
            }

            if (c == '\'')
            {
                i = SkipCharLiteral(text, i);
                if (i < 0)
                {
                    return null;
                }

                continue;
            }

            if (c == '/' && i + 1 < text.Length && text[i + 1] == '/')
            {
                while (i < text.Length && text[i] != '\n')
                {
                    i++;
                }

                continue;
            }

            if (c == '/' && i + 1 < text.Length && text[i + 1] == '*')
            {
                var end = text.IndexOf("*/", i + 2, StringComparison.Ordinal);
                if (end < 0)
                {
                    return null;
                }

                i = end + 1;
                continue;
            }

            if (c == '(')
            {
                depth++;
                continue;
            }

            if (c == ')')
            {
                depth--;
                if (depth == 0)
                {
                    return text[(openIndex + 1)..i];
                }
            }
        }

        return null;
    }

    /// <summary>Returns the index of the closing quote, or -1 when unterminated.</summary>
    private static int SkipStringLiteral(string text, int quoteIndex)
    {
        var verbatim = quoteIndex > 0 && text[quoteIndex - 1] == '@';

        for (var i = quoteIndex + 1; i < text.Length; i++)
        {
            if (verbatim)
            {
                if (text[i] != '"')
                {
                    continue;
                }

                // A doubled quote inside a verbatim literal is an escaped quote.
                if (i + 1 < text.Length && text[i + 1] == '"')
                {
                    i++;
                    continue;
                }

                return i;
            }

            if (text[i] == '\\')
            {
                i++;
                continue;
            }

            if (text[i] == '"')
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>Returns the index of the closing quote, or -1 when unterminated.</summary>
    private static int SkipCharLiteral(string text, int quoteIndex)
    {
        for (var i = quoteIndex + 1; i < text.Length; i++)
        {
            if (text[i] == '\\')
            {
                i++;
                continue;
            }

            if (text[i] == '\'')
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>Splits an argument list on its top-level commas.</summary>
    private static List<string> SplitTopLevelArguments(string arguments)
    {
        var parts = new List<string>();
        var depth = 0;
        var start = 0;

        for (var i = 0; i < arguments.Length; i++)
        {
            var c = arguments[i];

            if (c == '"')
            {
                var end = SkipStringLiteral(arguments, i);
                i = end < 0 ? arguments.Length : end;
                continue;
            }

            if (c is '(' or '[' or '{' or '<')
            {
                depth++;
                continue;
            }

            if (c is ')' or ']' or '}' or '>')
            {
                depth--;
                continue;
            }

            if (c == ',' && depth <= 0)
            {
                parts.Add(arguments[start..i].Trim());
                start = i + 1;
            }
        }

        parts.Add(arguments[start..].Trim());
        return parts;
    }

    /// <summary>
    /// Reads the declared unit from a split argument list. A <c>unit:</c> label
    /// wins; failing that the unit is the positional argument the
    /// <c>Meter.Create*</c> overloads define for the factory: the second for a
    /// synchronous instrument (<c>name, unit, description</c>), and the third for an
    /// observable one, whose second argument is its callback
    /// (<c>name, observeValue, unit, description</c>).
    /// </summary>
    /// <remarks>
    /// Reading the second argument of an observable declaration finds the callback,
    /// which is never a string literal, and so reported every positionally-united
    /// observable gauge as carrying no unit at all - silently, because "no unit" is
    /// a legitimate answer. <c>orleans.lattice.grainindex.backfill.percent_complete</c>
    /// (unit <c>%</c>) was one, and it is why a panel querying that gauge without
    /// the <c>_percent</c> word the exporter appends was certified (issue #3260).
    /// </remarks>
    private static string ResolveUnit(List<string> arguments, DeclaredInstrumentKind kind, ref int positionalUnits)
    {
        foreach (var argument in arguments)
        {
            var named = NamedUnitRegex.Match(argument);
            if (named.Success)
            {
                return named.Groups["u"].Value;
            }
        }

        var index = kind is DeclaredInstrumentKind.ObservableGauge
            or DeclaredInstrumentKind.ObservableCounter
            or DeclaredInstrumentKind.ObservableUpDownCounter
            ? 2
            : 1;

        if (arguments.Count > index && IsPlainStringLiteral(arguments[index]))
        {
            positionalUnits++;
            return arguments[index][1..^1];
        }

        return string.Empty;
    }

    private static bool IsPlainStringLiteral(string argument) =>
        argument.Length > 1 && argument[0] == '"' && argument[^1] == '"';

    private static string? ResolveName(string arg, Dictionary<string, string> constants, HashSet<string> ambiguous)
    {
        if (arg.Length > 1 && arg[0] == '"')
        {
            return arg[1..^1];
        }

        if (arg.Length > 2 && arg[0] == '@' && arg[1] == '"')
        {
            return arg[2..^1];
        }

        var identifier = arg.Split('.')[^1];
        if (ambiguous.Contains(identifier))
        {
            return null;
        }

        return constants.TryGetValue(identifier, out var value) ? value : null;
    }

    private sealed record Registry(
        IReadOnlyDictionary<string, DeclaredInstrumentKind> ByDottedName,
        IReadOnlyDictionary<string, string> UnitByDottedName,
        IReadOnlyList<string> Unresolved,
        IReadOnlyList<string> UnitUnresolved,
        int DeclarationCount,
        int PositionalUnitCount);
}