using System.Text.RegularExpressions;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Records, for a cited <c>Fixture.Method</c>, everything in the source that
/// decides whether NUnit runs it and which CI lane can select it (#5154): the
/// method-level and fixture-level <c>[Ignore]</c>, <c>[Explicit]</c> and
/// <c>[Category(...)]</c>, read across every partial declaration of the fixture
/// and up its base fixtures, plus the per-row <c>Category</c>, <c>Ignore</c> and
/// <c>Explicit</c> of <c>[TestCase]</c> rows.
/// <para>
/// WHY A SECOND INDEX BESIDE <see cref="RefinementDetectorResolver"/>. The
/// resolver answers "does a test of that name exist", from a brace-depth scan
/// that is deliberately cheap. This answers "does it run", which needs the
/// attribute lists on two kinds of declaration and the base list of a third,
/// so it reads Roslyn syntax trees instead of lines. It parses only the files
/// that declare a type of a requested name, found by a cheap text pass over
/// <c>test/</c>, so it costs a few hundred parses rather than several thousand.
/// The resolver stays the authority on existence; this index is the authority
/// on runnability, and the gate fails when the two disagree about whether a
/// cited test exists at all.
/// </para>
/// <para>
/// INHERITANCE FOLLOWS NUNIT, READ RATHER THAN ASSUMED. NUnit applies a base
/// fixture's attributes through <c>GetCustomAttributes(inherit: true)</c>, which
/// honours each attribute's <see cref="AttributeUsageAttribute.Inherited"/>. So
/// whether a base fixture's <c>[Category]</c>, <c>[Ignore]</c> or
/// <c>[Explicit]</c> reaches a derived fixture is read from NUnit's own
/// attribute types at run time, not hard-coded here.
/// </para>
/// <para>
/// WHAT IT CANNOT SEE: an ignore, explicit or category applied at run time
/// (the <c>Ignore</c>, <c>Explicit</c> and category setters of
/// <c>TestCaseData</c> in a
/// case source), an assembly-level attribute, and code excluded by a
/// preprocessor symbol. None is used by a cited detector today.
/// </para>
/// </summary>
internal sealed class RefinementDetectorRunnabilityIndex
{
    private static readonly Regex TypeDeclarationName = new(
        @"\b(?:class|struct|record)\s+([A-Za-z_][A-Za-z0-9_]*)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly HashSet<string> TestAttributes =
        new(["Test", "TestCase", "TestCaseSource", "Theory"], StringComparer.Ordinal);

    private static readonly CSharpParseOptions ParseOptions = new(LanguageVersion.Preview);

    private static readonly bool CategoryInherited = InheritedFlag(typeof(CategoryAttribute));
    private static readonly bool IgnoreInherited = InheritedFlag(typeof(IgnoreAttribute));
    private static readonly bool ExplicitInherited = InheritedFlag(typeof(ExplicitAttribute));
    private static readonly bool TestFixtureInherited = InheritedFlag(typeof(TestFixtureAttribute));

    private static readonly Lazy<RefinementDetectorRunnabilityIndex> Repository =
        new(() => new RefinementDetectorRunnabilityIndex(Path.Combine(HygieneRepository.FindRepoRoot(), "test")));

    private readonly string _testRoot;
    private readonly string _displayRoot;
    private readonly Dictionary<string, List<string>> _filesByTypeName = new(StringComparer.Ordinal);
    private readonly Dictionary<string, SyntaxNode> _roots = new(StringComparer.Ordinal);
    private readonly object _gate = new();

    /// <summary>One declaration of a type, possibly one of several partials.</summary>
    private sealed record Declaration(string Path, string Package, string QualifiedName, TypeDeclarationSyntax Node);

    /// <summary>The evidence gathered for one fixture and its bases.</summary>
    private sealed class Evidence
    {
        public List<string> Disqualifiers { get; } = [];

        public HashSet<string> Categories { get; } = new(StringComparer.Ordinal);

        public SortedSet<string> Sources { get; } = new(StringComparer.Ordinal);
    }

    /// <summary>
    /// Indexes, by name, every type declared in a <c>.cs</c> file under
    /// <paramref name="testRoot"/>. Each immediate subdirectory of the root is a
    /// package, as <c>test/&lt;package&gt;/</c> is in the repository.
    /// </summary>
    public RefinementDetectorRunnabilityIndex(string testRoot)
    {
        ArgumentNullException.ThrowIfNull(testRoot);
        _testRoot = Path.GetFullPath(testRoot);
        _displayRoot = Path.GetDirectoryName(_testRoot.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)) ?? _testRoot;

        foreach (var path in HygieneRepository.EnumerateFiles(_testRoot, "*.cs"))
        {
            foreach (var name in TypeDeclarationName.Matches(File.ReadAllText(path))
                .Select(match => match.Groups[1].Value)
                .Distinct(StringComparer.Ordinal))
            {
                if (!_filesByTypeName.TryGetValue(name, out var files))
                {
                    _filesByTypeName[name] = files = [];
                }

                files.Add(path);
            }
        }
    }

    /// <summary>
    /// An index over the repository's own <c>test/</c> tree, built once per
    /// test run, for the same reason <see cref="RefinementDetectorResolver.ForRepository"/> is.
    /// </summary>
    public static RefinementDetectorRunnabilityIndex ForRepository() => Repository.Value;

    /// <summary>The test root this index was built over.</summary>
    public string TestRoot => _testRoot;

    /// <summary>How many distinct type names the text pass found.</summary>
    public int IndexedTypeNameCount => _filesByTypeName.Count;

    /// <summary>
    /// Every NUnit-attributed declaration of <paramref name="method"/> in a
    /// fixture named <paramref name="fixture"/>, one entry per distinct fixture
    /// (package and namespace-qualified name). Partial declarations of one
    /// fixture are merged. Empty when no such test is declared; more than one
    /// entry when the simple fixture name is declared, with that test, in more
    /// than one namespace or package.
    /// </summary>
    public IReadOnlyList<RefinementDetectorRunnabilityState> Find(string fixture, string method)
    {
        ArgumentNullException.ThrowIfNull(fixture);
        ArgumentNullException.ThrowIfNull(method);

        var states = new List<RefinementDetectorRunnabilityState>();
        foreach (var group in Declarations(fixture)
            .GroupBy(d => (d.Package, d.QualifiedName))
            .OrderBy(g => g.Key.Package, StringComparer.Ordinal)
            .ThenBy(g => g.Key.QualifiedName, StringComparer.Ordinal))
        {
            var declarations = group.ToArray();
            var methods = declarations
                .SelectMany(d => d.Node.Members.OfType<MethodDeclarationSyntax>().Select(m => (Declaration: d, Method: m)))
                .Where(entry => string.Equals(entry.Method.Identifier.Text, method, StringComparison.Ordinal)
                    && Attributes(entry.Method.AttributeLists).Any(a => TestAttributes.Contains(Name(a))))
                .ToArray();

            if (methods.Length == 0)
            {
                continue;
            }

            states.Add(Describe(declarations, methods, method));
        }

        return states;
    }

    private RefinementDetectorRunnabilityState Describe(
        Declaration[] fixture,
        (Declaration Declaration, MethodDeclarationSyntax Method)[] methods,
        string method)
    {
        var evidence = new Evidence();
        CollectFixture(fixture, isBase: false, evidence, new HashSet<string>(StringComparer.Ordinal) { fixture[0].QualifiedName });

        var methodCategories = new HashSet<string>(StringComparer.Ordinal);
        var rows = new List<(HashSet<string> Categories, bool Runnable)>();

        foreach (var (declaration, syntax) in methods)
        {
            var where = $"the method ({Display(declaration.Path)})";
            evidence.Sources.Add(Display(declaration.Path));

            foreach (var attribute in Attributes(syntax.AttributeLists))
            {
                switch (Name(attribute))
                {
                    case "Ignore":
                        evidence.Disqualifiers.Add($"[Ignore] on {where}");
                        break;
                    case "Explicit":
                        evidence.Disqualifiers.Add($"[Explicit] on {where}");
                        break;
                    case "Category":
                        AddCategory(attribute, where, methodCategories, evidence);
                        break;
                    case "TestCaseSource":
                        ReadNamedRunState(attribute, $"[TestCaseSource] on {where}", methodCategories, evidence.Disqualifiers);
                        break;
                    case "TestCase":
                        var rowCategories = new HashSet<string>(StringComparer.Ordinal);
                        var rowDisqualifiers = new List<string>();
                        ReadNamedRunState(attribute, $"[TestCase] row on {where}", rowCategories, rowDisqualifiers);
                        rows.Add((rowCategories, rowDisqualifiers.Count == 0));
                        break;
                }
            }
        }

        var shared = evidence.Categories.Concat(methodCategories).ToHashSet(StringComparer.Ordinal);
        IReadOnlyList<IReadOnlySet<string>> cases;
        if (rows.Count == 0)
        {
            cases = [shared];
        }
        else
        {
            var runnable = rows.Where(r => r.Runnable).ToArray();
            if (runnable.Length == 0)
            {
                evidence.Disqualifiers.Add($"every one of its {rows.Count} [TestCase] row(s) is ignored or explicit");
            }

            cases = runnable
                .Select(r => (IReadOnlySet<string>)shared.Concat(r.Categories).ToHashSet(StringComparer.Ordinal))
                .ToArray();
        }

        return new RefinementDetectorRunnabilityState
        {
            Fixture = fixture[0].Node.Identifier.Text,
            Method = method,
            Package = fixture[0].Package,
            FullName = fixture[0].QualifiedName + "." + method,
            Disqualifiers = evidence.Disqualifiers.Distinct(StringComparer.Ordinal).ToArray(),
            CaseCategories = cases,
            Sources = evidence.Sources.ToArray(),
        };
    }

    private void CollectFixture(IReadOnlyList<Declaration> declarations, bool isBase, Evidence evidence, HashSet<string> visited)
    {
        foreach (var declaration in declarations)
        {
            var where = $"{(isBase ? "base fixture" : "fixture")} {declaration.Node.Identifier.Text} ({Display(declaration.Path)})";
            evidence.Sources.Add(Display(declaration.Path));

            foreach (var attribute in Attributes(declaration.Node.AttributeLists))
            {
                switch (Name(attribute))
                {
                    case "Ignore" when !isBase || IgnoreInherited:
                        evidence.Disqualifiers.Add($"[Ignore] on {where}");
                        break;
                    case "Explicit" when !isBase || ExplicitInherited:
                        evidence.Disqualifiers.Add($"[Explicit] on {where}");
                        break;
                    case "Category" when !isBase || CategoryInherited:
                        AddCategory(attribute, where, evidence.Categories, evidence);
                        break;
                    case "TestFixture" when !isBase || TestFixtureInherited:
                        ReadNamedRunState(attribute, $"[TestFixture] on {where}", evidence.Categories, evidence.Disqualifiers);
                        break;
                }
            }

            foreach (var baseName in BaseTypeNames(declaration.Node))
            {
                var bases = Declarations(baseName)
                    .Where(d => d.Node is not InterfaceDeclarationSyntax)
                    .ToArray();
                var samePackage = bases.Where(d => d.Package == declaration.Package).ToArray();

                foreach (var group in (samePackage.Length > 0 ? samePackage : bases).GroupBy(d => d.QualifiedName))
                {
                    if (visited.Add(group.Key))
                    {
                        CollectFixture(group.ToArray(), isBase: true, evidence, visited);
                    }
                }
            }
        }
    }

    private IEnumerable<Declaration> Declarations(string typeName)
    {
        if (!_filesByTypeName.TryGetValue(typeName, out var files))
        {
            yield break;
        }

        foreach (var path in files)
        {
            foreach (var node in Root(path).DescendantNodes().OfType<TypeDeclarationSyntax>())
            {
                if (string.Equals(node.Identifier.Text, typeName, StringComparison.Ordinal))
                {
                    yield return new Declaration(path, PackageOf(path), QualifiedName(node), node);
                }
            }
        }
    }

    private SyntaxNode Root(string path)
    {
        lock (_gate)
        {
            if (!_roots.TryGetValue(path, out var root))
            {
                root = CSharpSyntaxTree.ParseText(File.ReadAllText(path), ParseOptions, path).GetRoot();
                _roots[path] = root;
            }

            return root;
        }
    }

    private string PackageOf(string path)
    {
        var relative = Path.GetRelativePath(_testRoot, path).Replace('\\', '/');
        var slash = relative.IndexOf('/', StringComparison.Ordinal);
        return slash < 0 ? string.Empty : relative[..slash];
    }

    private string Display(string path) => Path.GetRelativePath(_displayRoot, path).Replace('\\', '/');

    /// <summary>
    /// <c>Namespace.Outer+Inner</c>, the form NUnit uses in a test's full name.
    /// </summary>
    private static string QualifiedName(TypeDeclarationSyntax node)
    {
        var types = node.AncestorsAndSelf().OfType<TypeDeclarationSyntax>().Select(t => t.Identifier.Text).Reverse();
        var namespaces = node.Ancestors().OfType<BaseNamespaceDeclarationSyntax>().Select(n => n.Name.ToString()).Reverse().ToArray();
        var typePart = string.Join("+", types);
        return namespaces.Length == 0 ? typePart : string.Join(".", namespaces) + "." + typePart;
    }

    private static IEnumerable<string> BaseTypeNames(TypeDeclarationSyntax node) =>
        node.BaseList?.Types.Select(t => SimpleName(t.Type)).OfType<string>() ?? [];

    private static string? SimpleName(TypeSyntax type) => type switch
    {
        QualifiedNameSyntax qualified => qualified.Right.Identifier.Text,
        AliasQualifiedNameSyntax alias => alias.Name.Identifier.Text,
        SimpleNameSyntax simple => simple.Identifier.Text,
        _ => null,
    };

    private static IEnumerable<AttributeSyntax> Attributes(SyntaxList<AttributeListSyntax> lists) =>
        lists.Where(list => list.Target is null
                || list.Target.Identifier.Text is "type" or "method")
            .SelectMany(list => list.Attributes);

    private static string Name(AttributeSyntax attribute)
    {
        var name = attribute.Name switch
        {
            QualifiedNameSyntax qualified => qualified.Right.Identifier.Text,
            AliasQualifiedNameSyntax alias => alias.Name.Identifier.Text,
            SimpleNameSyntax simple => simple.Identifier.Text,
            _ => attribute.Name.ToString(),
        };

        return name.Length > "Attribute".Length && name.EndsWith("Attribute", StringComparison.Ordinal)
            ? name[..^"Attribute".Length]
            : name;
    }

    private static void AddCategory(AttributeSyntax attribute, string where, HashSet<string> into, Evidence evidence)
    {
        var argument = attribute.ArgumentList?.Arguments.FirstOrDefault(a => a.NameEquals is null && a.NameColon is null);
        if (argument?.Expression is LiteralExpressionSyntax { RawKind: (int)SyntaxKind.StringLiteralExpression } literal)
        {
            into.Add(literal.Token.ValueText);
        }
        else
        {
            evidence.Disqualifiers.Add(
                $"[Category({argument?.Expression.ToString() ?? string.Empty})] on {where} is not a string literal, "
                + "so the lanes that select it cannot be evaluated");
        }
    }

    /// <summary>
    /// Reads the run-state named arguments that <c>[TestFixture]</c>,
    /// <c>[TestCase]</c> and <c>[TestCaseSource]</c> share: <c>Category</c>
    /// (comma-separated), <c>Ignore</c> / <c>IgnoreReason</c>, and
    /// <c>Explicit</c>. An argument whose value is not a literal disqualifies,
    /// because the gate cannot tell what it evaluates to.
    /// </summary>
    private static void ReadNamedRunState(AttributeSyntax attribute, string where, HashSet<string> categories, List<string> disqualifiers)
    {
        foreach (var argument in attribute.ArgumentList?.Arguments.Where(a => a.NameEquals is not null) ?? [])
        {
            var property = argument.NameEquals!.Name.Identifier.Text;
            var expression = argument.Expression;
            switch (property)
            {
                case "Category":
                    if (expression is LiteralExpressionSyntax { RawKind: (int)SyntaxKind.StringLiteralExpression } list)
                    {
                        foreach (var category in list.Token.ValueText.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                        {
                            categories.Add(category);
                        }
                    }
                    else
                    {
                        disqualifiers.Add($"Category = {expression} in {where} is not a string literal");
                    }

                    break;
                case "Ignore" or "IgnoreReason":
                    if (!expression.IsKind(SyntaxKind.NullLiteralExpression))
                    {
                        disqualifiers.Add($"{property} = {expression} in {where}");
                    }

                    break;
                case "Explicit":
                    if (!expression.IsKind(SyntaxKind.FalseLiteralExpression))
                    {
                        disqualifiers.Add($"Explicit = {expression} in {where}");
                    }

                    break;
            }
        }
    }

    private static bool InheritedFlag(Type attributeType) =>
        attributeType.GetCustomAttributes(typeof(AttributeUsageAttribute), inherit: true)
            .OfType<AttributeUsageAttribute>()
            .Select(usage => usage.Inherited)
            .DefaultIfEmpty(true)
            .First();
}
