using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// How a <see cref="RefinementCodeSymbol"/> resolved against the source tree.
/// <para>
/// The two failure outcomes are distinguished on purpose. "The type is gone"
/// and "the type is there but has nothing by that name" call for different
/// repairs, and a gate whose message conflates them makes the reader do the
/// bisection the gate already did.
/// </para>
/// </summary>
internal enum RefinementSymbolResolution
{
    /// <summary>No type of that name is declared anywhere under the source root.</summary>
    UnknownType,

    /// <summary>The type exists, but nothing of that name belongs to it.</summary>
    UnknownMember,

    /// <summary>A method, property, field, event, or enum member of the type.</summary>
    TypeMember,

    /// <summary>A type nested inside the named type.</summary>
    NestedType,

    /// <summary>
    /// A partial-class file suffix: a real file <c>Type.Member.cs</c>. This is
    /// NOT a member, and it is the form a naive checker gets wrong.
    /// </summary>
    PartialClassFile,
}

/// <summary>
/// Resolves the <c>Type.Member</c> references named by
/// <c>spec/Refinement.md</c> against the C# under <c>src/</c>, using nothing
/// but the source text. No compiler, no reflection, no external toolchain.
/// <para>
/// SOURCE TEXT RATHER THAN REFLECTION, deliberately. Reflection would resolve
/// members more precisely, but it resolves only what a loaded assembly
/// contains, and half of what this note names is not a member at all. It also
/// makes the gate's answer depend on which assemblies happen to be loaded,
/// which is a poor property for a check whose whole job is to be trustworthy
/// when it fails.
/// </para>
/// <para>
/// THE FORM THAT BREAKS NAIVE CHECKERS. Two of the note's references,
/// <c>ShardRootGrain.TxTerminal</c> and <c>BPlusLeafGrain.PendingTx</c>, are
/// partial-class FILE suffixes: real files at
/// <c>src/lattice/BPlusTree/Grains/ShardRootGrain.TxTerminal.cs</c> and
/// <c>BPlusLeafGrain.PendingTx.cs</c>. They are not methods and not
/// properties, so anything that assumes <c>Type.Member</c> reports both as
/// missing and is simply wrong. That is not hypothetical: the first probe run
/// while scoping this gate reported five missing symbols and all five were
/// false positives. The partial-class file form is therefore checked FIRST,
/// and <see cref="RefinementSymbolResolverTests"/> holds that case as a
/// standing regression.
/// </para>
/// <para>
/// BIAS. Member detection is deliberately permissive about declaration syntax:
/// it looks for the name in a declaration-shaped position anywhere in the
/// type's own files, with comments stripped. It can therefore accept a name
/// that only appears at a call site, which weakens the gate slightly. It
/// cannot report a name that is genuinely absent, which is the direction that
/// matters: a rename removes the name from the declaration and from every call
/// site in those files at once, so the gate still fires.
/// </para>
/// </summary>
internal sealed class RefinementSymbolResolver
{
    private static readonly Regex BlockComment =
        new(@"/\*.*?\*/", RegexOptions.Singleline | RegexOptions.Compiled);

    private static readonly Regex LineComment = new(@"//[^\n]*", RegexOptions.Compiled);

    private readonly string _sourceRoot;
    private readonly Dictionary<string, List<string>> _filesByStem = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _strippedText = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, IReadOnlyList<string>> _declaringFiles = new(StringComparer.Ordinal);
    private Dictionary<string, List<string>>? _typeIndex;

    /// <summary>
    /// Indexes the <c>.cs</c> files under <paramref name="sourceRoot"/> by
    /// filename stem. Only paths are read here; file contents are read lazily
    /// and cached, so a resolution touches a handful of files rather than the
    /// three thousand under <c>src/</c>.
    /// </summary>
    public RefinementSymbolResolver(string sourceRoot)
    {
        ArgumentNullException.ThrowIfNull(sourceRoot);
        _sourceRoot = sourceRoot;

        foreach (var path in HygieneRepository.EnumerateFiles(sourceRoot, "*.cs"))
        {
            var name = Path.GetFileName(path);
            var dot = name.IndexOf('.', StringComparison.Ordinal);
            var stem = dot > 0 ? name[..dot] : name;

            if (!_filesByStem.TryGetValue(stem, out var files))
            {
                files = new List<string>();
                _filesByStem[stem] = files;
            }

            files.Add(path);
        }
    }

    /// <summary>A resolver over the repository's own <c>src/</c> tree.</summary>
    public static RefinementSymbolResolver ForRepository() =>
        new(Path.Combine(HygieneRepository.FindRepoRoot(), "src"));

    /// <summary>The source root this resolver was built over.</summary>
    public string SourceRoot => _sourceRoot;

    /// <summary>True when the outcome means the reference still points at something real.</summary>
    public static bool IsResolved(RefinementSymbolResolution resolution) =>
        resolution is RefinementSymbolResolution.TypeMember
            or RefinementSymbolResolution.NestedType
            or RefinementSymbolResolution.PartialClassFile;

    /// <summary>Resolves one reference from the refinement note.</summary>
    public RefinementSymbolResolution Resolve(RefinementCodeSymbol symbol)
    {
        ArgumentNullException.ThrowIfNull(symbol);
        return Resolve(symbol.TypeName, symbol.MemberName);
    }

    /// <summary>Resolves a <c>Type.Member</c> pair.</summary>
    public RefinementSymbolResolution Resolve(string typeName, string memberName)
    {
        ArgumentNullException.ThrowIfNull(typeName);
        ArgumentNullException.ThrowIfNull(memberName);

        var files = DeclaringFiles(typeName);
        if (files.Count == 0)
        {
            return RefinementSymbolResolution.UnknownType;
        }

        // Checked before members: the partial-class file form is a statement
        // about the file layout, and it is what the note means when it writes
        // ShardRootGrain.TxTerminal.
        if (HasPartialClassFile(typeName, memberName))
        {
            return RefinementSymbolResolution.PartialClassFile;
        }

        // Nested types are checked before members because their declarations
        // are a strict subset of the member shape: `class Inputs {` and
        // `record struct Inputs(` both satisfy the member pattern, so testing
        // members first would report every nested type as a member. The
        // reverse cannot happen, since the member pattern has no type keyword.
        if (files.Any(file => DeclaresType(file, memberName)))
        {
            return RefinementSymbolResolution.NestedType;
        }

        if (files.Any(file => DeclaresMember(file, memberName)))
        {
            return RefinementSymbolResolution.TypeMember;
        }

        return RefinementSymbolResolution.UnknownMember;
    }

    /// <summary>
    /// True when a type of this name is declared under the source root. This
    /// is the bare-type-name resolution form, exercised by every reference
    /// through <see cref="Resolve(string, string)"/>.
    /// </summary>
    public bool TypeExists(string typeName)
    {
        ArgumentNullException.ThrowIfNull(typeName);
        return DeclaringFiles(typeName).Count > 0;
    }

    /// <summary>True when a file <c>Type.Suffix.cs</c> exists for the type.</summary>
    public bool HasPartialClassFile(string typeName, string suffix)
    {
        ArgumentNullException.ThrowIfNull(typeName);
        ArgumentNullException.ThrowIfNull(suffix);

        if (!_filesByStem.TryGetValue(typeName, out var files))
        {
            return false;
        }

        var expected = $"{typeName}.{suffix}.cs";
        return files.Any(f => string.Equals(Path.GetFileName(f), expected, StringComparison.Ordinal));
    }

    /// <summary>
    /// The files declaring a type. Resolved from the filename stem first,
    /// which is what the repository's one-top-level-type-per-file convention
    /// makes correct and cheap; a type declared in a file that does not share
    /// its name falls back to a whole-tree scan, cached, so an unconventional
    /// layout costs speed rather than a false alarm.
    /// </summary>
    private IReadOnlyList<string> DeclaringFiles(string typeName)
    {
        if (_declaringFiles.TryGetValue(typeName, out var cached))
        {
            return cached;
        }

        IReadOnlyList<string> resolved = Array.Empty<string>();

        if (_filesByStem.TryGetValue(typeName, out var candidates))
        {
            var declaring = candidates.Where(f => DeclaresType(f, typeName)).ToArray();
            if (declaring.Length > 0)
            {
                // Every partial file of the type re-declares it, so this is
                // the whole set and no wider scan is needed.
                resolved = declaring;
            }
        }

        if (resolved.Count == 0 && TypeIndex().TryGetValue(typeName, out var everywhere))
        {
            resolved = everywhere;
        }

        _declaringFiles[typeName] = resolved;
        return resolved;
    }

    private Dictionary<string, List<string>> TypeIndex()
    {
        if (_typeIndex is not null)
        {
            return _typeIndex;
        }

        var index = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        foreach (var path in _filesByStem.Values.SelectMany(v => v))
        {
            foreach (Match match in AnyTypeDeclaration.Matches(Stripped(path)))
            {
                var name = match.Groups[1].Value;
                if (!index.TryGetValue(name, out var files))
                {
                    files = new List<string>();
                    index[name] = files;
                }

                files.Add(path);
            }
        }

        _typeIndex = index;
        return index;
    }

    private static readonly Regex AnyTypeDeclaration = new(
        @"\b(?:class|interface|enum|struct|record)\s+(?!class\b|struct\b)([A-Za-z_][A-Za-z0-9_]*)",
        RegexOptions.Compiled);

    private bool DeclaresType(string path, string typeName) =>
        TypeDeclaration(typeName).IsMatch(Stripped(path));

    private bool DeclaresMember(string path, string memberName)
    {
        var text = Stripped(path);
        return MemberDeclaration(memberName).IsMatch(text) || EnumMember(memberName).IsMatch(text);
    }

    private string Stripped(string path)
    {
        if (_strippedText.TryGetValue(path, out var cached))
        {
            return cached;
        }

        var text = File.ReadAllText(path).ReplaceLineEndings("\n");
        text = BlockComment.Replace(text, string.Empty);
        text = LineComment.Replace(text, string.Empty);

        _strippedText[path] = text;
        return text;
    }

    private static readonly Dictionary<string, Regex> TypeDeclarationCache = new(StringComparer.Ordinal);
    private static readonly Dictionary<string, Regex> MemberDeclarationCache = new(StringComparer.Ordinal);
    private static readonly Dictionary<string, Regex> EnumMemberCache = new(StringComparer.Ordinal);

    private static Regex TypeDeclaration(string name) => Cached(
        TypeDeclarationCache,
        name,
        n => new Regex(
            @"\b(?:class|interface|enum|struct|record)\s+(?:class\s+|struct\s+)?"
            + Regex.Escape(n) + @"(?![A-Za-z0-9_])",
            RegexOptions.Compiled));

    // A name in declaration position: followed by a parameter list, a body, an
    // expression body, an initialiser, or a terminator, with an optional
    // generic parameter list in between. Comments are already stripped, so a
    // name that survives only in prose does not count.
    private static Regex MemberDeclaration(string name) => Cached(
        MemberDeclarationCache,
        name,
        n => new Regex(
            @"(?<![A-Za-z0-9_@.])" + Regex.Escape(n) + @"(?![A-Za-z0-9_])"
            + @"\s*(?:<[^<>;{}()]*>\s*)?(?:\(|\{|=>|;|=(?!=))",
            RegexOptions.Compiled));

    // An enum member sits alone on its line, optionally with an explicit
    // value, so it never matches the declaration shape above.
    private static Regex EnumMember(string name) => Cached(
        EnumMemberCache,
        name,
        n => new Regex(
            @"^[ \t]*" + Regex.Escape(n) + @"[ \t]*(?:=[^=\n][^\n]*?)?,?[ \t]*$",
            RegexOptions.Multiline | RegexOptions.Compiled));

    private static Regex Cached(Dictionary<string, Regex> cache, string key, Func<string, Regex> factory)
    {
        lock (cache)
        {
            if (!cache.TryGetValue(key, out var regex))
            {
                regex = factory(key);
                cache[key] = regex;
            }

            return regex;
        }
    }
}
