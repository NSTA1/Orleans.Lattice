namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// One TLA+ module under <c>spec/</c>, as discovered from disk by
/// <see cref="SpecModuleCatalogue"/>: its directory, its files, and the
/// manifest that records its counts.
/// <para>
/// Every Formal gate takes one of these rather than a hard-coded path, so a
/// module is gated exactly when discovery can see it. That is the property the
/// type exists for: before it, the gates named <c>spec/AtomicCommit.tla</c>
/// directly, and a second module would have sat beside it entirely outside
/// them while every gate stayed green - the vacuity epic #2299 found.
/// </para>
/// </summary>
public sealed record SpecModule
{
    /// <summary>The module's README, required in every module directory.</summary>
    public const string ReadmeFileName = "README.md";

    /// <summary>The TLA+ module name, which is also the file stem of its <c>.tla</c>, <c>.cfg</c> and manifest.</summary>
    public required string Name { get; init; }

    /// <summary>Absolute path of the <c>spec/</c> root the module was discovered under.</summary>
    public required string SpecRoot { get; init; }

    /// <summary>Absolute path of the module's directory, <c>spec/&lt;area&gt;/</c>.</summary>
    public required string Directory { get; init; }

    /// <summary>The parsed <c>&lt;Name&gt;.manifest.json</c>.</summary>
    public required SpecModuleManifest Manifest { get; init; }

    /// <summary>The module's specification, <c>&lt;Name&gt;.tla</c>.</summary>
    public string SpecificationPath => Path.Combine(Directory, $"{Name}.tla");

    /// <summary>The module's TLC model, <c>&lt;Name&gt;.cfg</c>.</summary>
    public string ConfigPath => Path.Combine(Directory, $"{Name}.cfg");

    /// <summary>The module's manifest, <c>&lt;Name&gt;.manifest.json</c>.</summary>
    public string ManifestPath => Path.Combine(Directory, Name + SpecModuleManifest.FileSuffix);

    /// <summary>The directory holding the module's <c>.mutation</c> files.</summary>
    public string MutationDirectory => Path.GetFullPath(Path.Combine(Directory, Manifest.MutationsDirectory));

    /// <summary>The module's refinement note.</summary>
    public string RefinementNotePath => Path.GetFullPath(Path.Combine(Directory, Manifest.RefinementNote));

    /// <summary>The README of the module's directory, which carries its counts table.</summary>
    public string ReadmePath => Path.Combine(Directory, ReadmeFileName);

    /// <summary>
    /// A path as a reader of the repository would write it, rooted at
    /// <c>spec/</c> with forward slashes, for failure messages:
    /// <c>spec/atomic-commit/AtomicCommit.tla</c>.
    /// </summary>
    public string Describe(string path)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);

        var relative = Path.GetRelativePath(SpecRoot, path).Replace('\\', '/');
        return $"{Path.GetFileName(SpecRoot.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar))}/{relative}";
    }

    /// <summary>Reads the module's specification text.</summary>
    public string ReadSpecification() => File.ReadAllText(SpecificationPath);

    /// <summary>Reads the module's TLC model.</summary>
    public string ReadConfig() => File.ReadAllText(ConfigPath);

    /// <summary>Reads the module's refinement note.</summary>
    public string ReadRefinementNote() => File.ReadAllText(RefinementNotePath);

    /// <summary>Reads the module's README.</summary>
    public string ReadReadme() => File.ReadAllText(ReadmePath);

    /// <summary>Parses the module's mutation catalogue, ordered by name.</summary>
    public IReadOnlyList<SpecMutation> LoadMutations() => SpecMutationCatalogue.Load(MutationDirectory);

    /// <summary>
    /// Every <c>.tla</c> in the module's directory, keyed by file name. TLC is
    /// run in a scratch directory, and a module that <c>EXTENDS</c> or
    /// <c>INSTANCE</c>s a sibling needs that sibling beside it there.
    /// </summary>
    public IReadOnlyDictionary<string, string> ReadSiblingSpecifications() =>
        System.IO.Directory
            .EnumerateFiles(Directory, "*.tla")
            .ToDictionary(f => Path.GetFileName(f), File.ReadAllText, StringComparer.Ordinal);

    /// <summary>Parses the refinement note's mapping tables, naming the note in any error.</summary>
    internal IReadOnlyDictionary<string, RefinementTable> ReadRefinementTables() =>
        RefinementNote.ParseTables(ReadRefinementNote(), Describe(RefinementNotePath));

    /// <summary>
    /// The module name, which is what NUnit renders for this argument in a
    /// test-case label, so every case of every gate names its module.
    /// </summary>
    public override string ToString() => Name;
}
