using System.Text.RegularExpressions;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// The census of production grains that no refinement note names.
/// <para>
/// THE CONVERSE OF THE REFINEMENT GATES. Those gates check that what a note
/// names still exists. Nothing checked which protocol-bearing production code no
/// note names at all, so a new grain, or an old one nobody thought to model, was
/// invisible to the formal suite until something broke. This census closes that
/// direction: every grain implementation class under <c>src/</c> is either named
/// somewhere in a refinement note (a mapping row, an abstraction-gap bullet or
/// prose) or listed in the register <c>spec/UNMODELLED.md</c> with a reason.
/// </para>
/// <para>
/// SYNTACTIC, NOT REFLECTIVE. Grains are found by parsing source text with
/// Roslyn's syntax layer only - no compilation, no semantic model, no loaded
/// assembly - so the answer covers every package under <c>src/</c>, including
/// the ones this test project does not reference. A grain is a class whose base
/// list names Orleans' <c>Grain</c> or <c>Grain&lt;T&gt;</c>, or
/// <c>IGrainBase</c>, or (to a fixed point) another type found that way, so an
/// intermediate base grain does not hide its subclasses. Base types are matched
/// by simple name; a collision can only add a grain to the population, which
/// costs a register row, never hide one.
/// </para>
/// <para>
/// WHAT "NAMED" MEANS. The grain's simple name appears in a note as a whole
/// identifier: <c>LatticeGrain</c> is not named by <c>LatticeGrainState</c>.
/// Any appearance counts, because a gap bullet that says a grain is
/// deliberately outside the model is exactly as much a decision about it as a
/// mapping row is. Naming the grain's own contract interface counts too, but
/// only that one and only when the grain implements it: notes conventionally
/// cite the contract (<c>ITreeDeletionGrain.DeleteTreeAsync</c>) of a grain that
/// is its one default implementation, whereas a shared interface such as
/// <c>IRemindable</c> says nothing about any particular grain.
/// </para>
/// <para>
/// STALE ENTRIES FAIL. A register row for a grain that no longer exists, that
/// moved package, or that a note now names is reported, so the register cannot
/// silently accumulate rows that describe nothing.
/// </para>
/// </summary>
internal static class RefinementSurfaceCensus
{
    /// <summary>The register's path, relative to the repository root.</summary>
    public const string RegisterRelativePath = "spec/UNMODELLED.md";

    /// <summary>The <c>##</c> heading the register table sits under.</summary>
    public const string RegisterSection = "Register";

    /// <summary>The register table's header cells, in order.</summary>
    public static IReadOnlyList<string> RegisterHeaders { get; } = ["Grain", "Package", "Reason"];

    private static readonly HashSet<string> Roots = new(StringComparer.Ordinal) { "Grain", "IGrainBase" };

    private static readonly Regex BacktickedIdentifier = new(
        @"^`([A-Za-z_][A-Za-z0-9_]*)`$", RegexOptions.CultureInvariant);

    private static readonly Regex BacktickedPackage = new(
        @"^`([a-z0-9][a-z0-9._-]*)`$", RegexOptions.CultureInvariant);

    private static readonly Regex IdentifierRun = new(@"[A-Za-z0-9_]+", RegexOptions.CultureInvariant);

    private static readonly Regex AlignmentCell = new(@"^:?-{1,}:?$", RegexOptions.CultureInvariant);

    private static readonly HashSet<string> PlaceholderReasons = new(StringComparer.OrdinalIgnoreCase)
    {
        "-", "n/a", "na", "none", "tbd", "todo", "?",
    };

    /// <summary>Absolute path of the repository's <c>src/</c> directory.</summary>
    public static string RepositorySourceRoot => Path.Combine(HygieneRepository.FindRepoRoot(), "src");

    /// <summary>Absolute path of the repository's register.</summary>
    public static string RepositoryRegisterPath =>
        Path.Combine(HygieneRepository.FindRepoRoot(), RegisterRelativePath.Replace('/', Path.DirectorySeparatorChar));

    /// <summary>
    /// Every grain implementation class in the tracked <c>.cs</c> files under
    /// <paramref name="sourceRoot"/>.
    /// </summary>
    public static IReadOnlyList<RefinementSurfaceCensusGrain> FindGrains(string sourceRoot)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourceRoot);

        var root = Path.GetFullPath(sourceRoot);
        var files = HygieneRepository.EnumerateFiles(root, "*.cs")
            .Select(path => (RelativePath: Path.GetRelativePath(root, path).Replace('\\', '/'), Text: File.ReadAllText(path)))
            .ToList();

        return FindGrains(files);
    }

    /// <summary>
    /// Every grain implementation class in <paramref name="files"/>, whose
    /// paths are relative to the source root so their first segment is the
    /// package. Exposed over text so a control can drive it with a synthetic
    /// tree.
    /// </summary>
    public static IReadOnlyList<RefinementSurfaceCensusGrain> FindGrains(IEnumerable<(string RelativePath, string Text)> files)
    {
        ArgumentNullException.ThrowIfNull(files);

        var declarations = files
            .AsParallel()
            .SelectMany(file => Declarations(file.RelativePath, file.Text))
            .ToList();

        // Grow the set of grain-bearing type names to a fixed point, so a class
        // deriving from an intermediate base grain is found however deep it sits.
        var grainNames = new HashSet<string>(Roots, StringComparer.Ordinal);
        var grown = true;
        while (grown)
        {
            grown = false;
            foreach (var declaration in declarations)
            {
                if (!grainNames.Contains(declaration.Name) && declaration.Bases.Any(grainNames.Contains))
                {
                    grainNames.Add(declaration.Name);
                    grown = true;
                }
            }
        }

        return declarations
            .Where(d => d.IsClass && d.Bases.Any(grainNames.Contains))
            .GroupBy(d => (d.Package, d.Key))
            .Select(group =>
            {
                var parts = group.OrderBy(d => d.RelativePath, StringComparer.Ordinal).ToList();
                var contract = "I" + parts[0].Name;
                return new RefinementSurfaceCensusGrain(
                    parts[0].Name,
                    parts[0].Package,
                    parts[0].RelativePath,
                    parts.Any(d => d.IsAbstract),
                    parts.Any(d => d.Bases.Contains(contract, StringComparer.Ordinal)) ? contract : null);
            })
            .OrderBy(g => g.Package, StringComparer.Ordinal)
            .ThenBy(g => g.Name, StringComparer.Ordinal)
            .ToList();
    }

    /// <summary>
    /// True when <paramref name="text"/> names <paramref name="grain"/> as a
    /// whole identifier.
    /// </summary>
    public static bool Names(string text, string grain)
    {
        ArgumentNullException.ThrowIfNull(text);
        ArgumentException.ThrowIfNullOrEmpty(grain);
        return Identifiers(text).Contains(grain);
    }

    // Every maximal run of identifier characters. A name is in this set exactly
    // when it appears with no identifier character on either side, so one pass
    // per note answers every grain.
    private static HashSet<string> Identifiers(string text) =>
        IdentifierRun.Matches(text).Select(m => m.Value).ToHashSet(StringComparer.Ordinal);

    /// <summary>
    /// Reads the register table out of <paramref name="markdown"/>. Throws when
    /// the section or its table is missing, the header is not
    /// <see cref="RegisterHeaders"/>, or a row is malformed, because a register
    /// the gate cannot read would otherwise excuse nothing or everything.
    /// </summary>
    /// <param name="markdown">The register's text.</param>
    /// <param name="label">How to name the register in an error, such as its path.</param>
    public static IReadOnlyList<RefinementSurfaceCensusEntry> ReadRegister(string markdown, string label)
    {
        ArgumentNullException.ThrowIfNull(markdown);
        ArgumentException.ThrowIfNullOrEmpty(label);

        var lines = markdown.ReplaceLineEndings("\n").Split('\n');
        var problems = new List<string>();
        var entries = new List<RefinementSurfaceCensusEntry>();
        var inSection = false;
        var sawSection = false;
        var sawHeader = false;
        var tableClosed = false;

        for (var i = 0; i < lines.Length; i++)
        {
            var line = lines[i].Trim();

            if (line.StartsWith("##", StringComparison.Ordinal))
            {
                inSection = string.Equals(line.TrimStart('#').Trim(), RegisterSection, StringComparison.Ordinal);
                sawSection |= inSection;
                continue;
            }

            if (!inSection)
            {
                continue;
            }

            if (!line.StartsWith('|'))
            {
                tableClosed |= sawHeader;
                continue;
            }

            if (tableClosed)
            {
                problems.Add($"{label}:{i + 1}: a second table under '## {RegisterSection}'. Keep one register table.");
                continue;
            }

            var cells = SplitCells(line);

            if (!sawHeader)
            {
                sawHeader = true;
                if (!cells.SequenceEqual(RegisterHeaders, StringComparer.Ordinal))
                {
                    problems.Add(
                        $"{label}:{i + 1}: the register header is '{string.Join(" | ", cells)}'; it must be "
                        + $"'{string.Join(" | ", RegisterHeaders)}'.");
                }

                continue;
            }

            if (cells.All(c => AlignmentCell.IsMatch(c)))
            {
                continue;
            }

            if (cells.Count != RegisterHeaders.Count)
            {
                problems.Add($"{label}:{i + 1}: a register row has {cells.Count} cells; it needs {RegisterHeaders.Count}.");
                continue;
            }

            var grain = BacktickedIdentifier.Match(cells[0]);
            var package = BacktickedPackage.Match(cells[1]);
            var reason = cells[2];

            if (!grain.Success)
            {
                problems.Add($"{label}:{i + 1}: the grain cell '{cells[0]}' must be one backticked class name, such as `MyGrain`.");
            }

            if (!package.Success)
            {
                problems.Add($"{label}:{i + 1}: the package cell '{cells[1]}' must be one backticked src/ directory name, such as `lattice`.");
            }

            if (reason.Length == 0 || PlaceholderReasons.Contains(reason))
            {
                problems.Add($"{label}:{i + 1}: the reason cell is '{reason}'. Every register row needs a real reason.");
            }

            if (grain.Success && package.Success)
            {
                entries.Add(new RefinementSurfaceCensusEntry(grain.Groups[1].Value, package.Groups[1].Value, reason, i + 1));
            }
        }

        if (!sawSection)
        {
            problems.Add($"{label} has no '## {RegisterSection}' section.");
        }
        else if (!sawHeader)
        {
            problems.Add($"{label}'s '## {RegisterSection}' section has no table.");
        }

        if (problems.Count > 0)
        {
            throw new InvalidOperationException(
                $"{label} could not be read as the unmodelled-grain register:"
                + Environment.NewLine + " - " + string.Join(Environment.NewLine + " - ", problems));
        }

        return entries;
    }

    /// <summary>
    /// Every way the grains, the notes and the register disagree: a grain no
    /// note names (by its name or its own contract interface) and no row lists, a row for no grain, a row for a grain a
    /// note names, and a register that repeats a row or is out of order. Empty
    /// when the three agree.
    /// </summary>
    /// <param name="grains">The grain population, from <see cref="FindGrains(string)"/>.</param>
    /// <param name="notes">Every refinement note, as a label and its text.</param>
    /// <param name="register">The register rows, from <see cref="ReadRegister"/>.</param>
    /// <param name="registerLabel">How to name the register in a message.</param>
    public static IReadOnlyList<string> Problems(
        IReadOnlyList<RefinementSurfaceCensusGrain> grains,
        IReadOnlyList<(string Label, string Text)> notes,
        IReadOnlyList<RefinementSurfaceCensusEntry> register,
        string registerLabel)
    {
        ArgumentNullException.ThrowIfNull(grains);
        ArgumentNullException.ThrowIfNull(notes);
        ArgumentNullException.ThrowIfNull(register);
        ArgumentException.ThrowIfNullOrEmpty(registerLabel);

        var problems = new List<string>();

        var noteIdentifiers = notes.Select(note => (note.Label, Identifiers: Identifiers(note.Text))).ToList();
        string? NamingNote(string grain) =>
            noteIdentifiers.Where(note => note.Identifiers.Contains(grain)).Select(note => note.Label).FirstOrDefault();

        var rows = register.ToLookup(e => (e.Grain, e.Package));

        foreach (var duplicate in register.GroupBy(e => (e.Grain, e.Package)).Where(g => g.Count() > 1))
        {
            problems.Add(
                $"{registerLabel} lists `{duplicate.Key.Grain}` (`{duplicate.Key.Package}`) on lines "
                + $"{string.Join(", ", duplicate.Select(e => e.LineNumber))}. List each grain once.");
        }

        for (var i = 1; i < register.Count; i++)
        {
            var previous = register[i - 1];
            var current = register[i];
            var order = string.CompareOrdinal(previous.Package, current.Package);
            if (order > 0 || (order == 0 && string.CompareOrdinal(previous.Grain, current.Grain) > 0))
            {
                problems.Add(
                    $"{registerLabel}:{current.LineNumber}: `{current.Grain}` (`{current.Package}`) is out of order. "
                    + "Rows are sorted by package, then grain, ordinally.");
                break;
            }
        }

        foreach (var grain in grains)
        {
            var named = NamingNote(grain.Name) ?? (grain.Contract is null ? null : NamingNote(grain.Contract));
            var listed = rows.Contains((grain.Name, grain.Package));

            if (named is null && !listed)
            {
                problems.Add(
                    $"src/{grain.SourcePath} declares the grain `{grain.Name}`, "
                    + "which no refinement note names and the register does not list. Name it in the refinement "
                    + "note of the specification that models it (a mapping row, or an abstraction-gap bullet "
                    + $"saying why the model leaves it out), or add a row to {registerLabel} with the reason no "
                    + "specification models it.");
            }
            else if (named is not null && listed)
            {
                problems.Add(
                    $"{registerLabel}:{rows[(grain.Name, grain.Package)].First().LineNumber}: `{grain.Name}` is named by "
                    + $"{named}, so it is no longer unmodelled. Remove its register row.");
            }
        }

        var population = grains.Select(g => (g.Name, g.Package)).ToHashSet();
        foreach (var entry in register.Where(e => !population.Contains((e.Grain, e.Package))))
        {
            var elsewhere = grains.Where(g => string.Equals(g.Name, entry.Grain, StringComparison.Ordinal)).ToList();
            problems.Add(elsewhere.Count > 0
                ? $"{registerLabel}:{entry.LineNumber}: `{entry.Grain}` is listed in package `{entry.Package}`, but it "
                    + $"is declared in {string.Join(", ", elsewhere.Select(g => $"`{g.Package}`"))}. Correct the package."
                : $"{registerLabel}:{entry.LineNumber}: `{entry.Grain}` (`{entry.Package}`) is not a grain declared "
                    + "under src/. It was renamed, removed, or stopped being a grain; update or remove its row.");
        }

        return problems;
    }

    private sealed record Declaration(
        string RelativePath,
        string Package,
        string Key,
        string Name,
        bool IsClass,
        bool IsAbstract,
        IReadOnlyList<string> Bases);

    private static IEnumerable<Declaration> Declarations(string relativePath, string text)
    {
        var slash = relativePath.IndexOf('/', StringComparison.Ordinal);
        if (slash <= 0)
        {
            // A file directly in the source root belongs to no package.
            yield break;
        }

        var package = relativePath[..slash];
        var root = CSharpSyntaxTree.ParseText(text, path: relativePath).GetCompilationUnitRoot();

        foreach (var type in root.DescendantNodes().OfType<TypeDeclarationSyntax>())
        {
            if (type.BaseList is null)
            {
                continue;
            }

            var isClass = type is ClassDeclarationSyntax
                || (type is RecordDeclarationSyntax record && !record.ClassOrStructKeyword.IsKind(SyntaxKind.StructKeyword));

            yield return new Declaration(
                relativePath,
                package,
                QualifiedKey(type),
                type.Identifier.ValueText,
                isClass,
                type.Modifiers.Any(m => m.IsKind(SyntaxKind.AbstractKeyword)),
                type.BaseList.Types.Select(b => SimpleName(b.Type)).ToList());
        }
    }

    private static string QualifiedKey(TypeDeclarationSyntax type)
    {
        var parts = new List<string>();
        for (SyntaxNode? node = type; node is not null; node = node.Parent)
        {
            switch (node)
            {
                case TypeDeclarationSyntax t:
                    parts.Add(t.Identifier.ValueText + "`" + (t.TypeParameterList?.Parameters.Count ?? 0));
                    break;
                case BaseNamespaceDeclarationSyntax n:
                    parts.Add(n.Name.ToString());
                    break;
            }
        }

        parts.Reverse();
        return string.Join(".", parts);
    }

    private static string SimpleName(TypeSyntax type) => type switch
    {
        QualifiedNameSyntax qualified => SimpleName(qualified.Right),
        AliasQualifiedNameSyntax alias => SimpleName(alias.Name),
        GenericNameSyntax generic => generic.Identifier.ValueText,
        IdentifierNameSyntax identifier => identifier.Identifier.ValueText,
        _ => type.ToString(),
    };

    private static List<string> SplitCells(string line)
    {
        var trimmed = line.Trim();
        if (trimmed.StartsWith('|'))
        {
            trimmed = trimmed[1..];
        }

        if (trimmed.EndsWith('|'))
        {
            trimmed = trimmed[..^1];
        }

        return trimmed.Split('|').Select(c => c.Trim()).ToList();
    }
}
