using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Discovers the TLA+ modules under <c>spec/</c> from disk.
/// <para>
/// THE CONTRACT. Every directory directly under <c>spec/</c> is a module
/// directory, and every <c>.tla</c> in a module directory is a root module that
/// needs three siblings of the same stem: <c>&lt;Module&gt;.cfg</c> (its TLC
/// model), <c>&lt;Module&gt;.manifest.json</c> (see
/// <see cref="SpecModuleManifest"/>) and, named by the manifest, a mutation
/// directory and a refinement note. The directory also needs a
/// <c>README.md</c>, whose counts table is checked against the manifest. One
/// module per directory is the norm; a directory may hold a second module that
/// extends the first, because every <c>.tla</c> beside a module is copied into
/// TLC's scratch directory with it.
/// </para>
/// <para>
/// MALFORMED IS LOUD, NEVER SKIPPED. A directory that looks like a module but is
/// not well-formed - a <c>.tla</c> with no <c>.cfg</c>, a <c>.cfg</c> or manifest
/// with no <c>.tla</c>, a missing README, a manifest naming a directory that
/// does not exist - throws, naming every problem at once. So does a directory
/// with no <c>.tla</c> at all, and a <c>.tla</c> left directly in <c>spec/</c>.
/// Skipping any of them would be the failure this type exists to prevent: a
/// module the gates cannot see is indistinguishable, from a green run, from a
/// module they checked.
/// </para>
/// </summary>
public static class SpecModuleCatalogue
{
    /// <summary>
    /// The fewest modules the repository's discovery may return. Zero would
    /// leave every gate parameterised over nothing, which NUnit reports as a
    /// test with no cases rather than as a failure.
    /// </summary>
    public const int MinimumRepositoryModules = 1;

    private static readonly Regex ModuleHeader = new(
        @"^-{4,}\s*MODULE\s+([A-Za-z][A-Za-z0-9_]*)\s+-{4,}",
        RegexOptions.Multiline | RegexOptions.CultureInvariant);

    private static readonly Lazy<IReadOnlyList<SpecModule>> RepositoryModules = new(DiscoverRepository);

    /// <summary>Absolute path of the repository's <c>spec/</c> directory.</summary>
    public static string RepositorySpecRoot => Path.Combine(HygieneRepository.FindRepoRoot(), "spec");

    /// <summary>
    /// The repository's modules, discovered once per test run. Throws when
    /// discovery finds a malformed module or fewer than
    /// <see cref="MinimumRepositoryModules"/>.
    /// </summary>
    public static IReadOnlyList<SpecModule> Repository() => RepositoryModules.Value;

    /// <summary>
    /// Discovers every module under <paramref name="specRoot"/>, ordered by
    /// directory then module name. Throws with every problem found when any
    /// module directory is malformed. Applies no population floor, so that a
    /// control can drive it over a hand-built root; <see cref="Repository"/>
    /// applies the floor for the real one.
    /// </summary>
    public static IReadOnlyList<SpecModule> Discover(string specRoot)
    {
        ArgumentException.ThrowIfNullOrEmpty(specRoot);

        var root = Path.GetFullPath(specRoot);
        if (!System.IO.Directory.Exists(root))
        {
            throw new InvalidOperationException($"the specification root '{root}' does not exist.");
        }

        var problems = new List<string>();
        var modules = new List<SpecModule>();

        foreach (var stray in System.IO.Directory.EnumerateFiles(root, "*.tla").Order(StringComparer.Ordinal))
        {
            problems.Add(
                $"{Path.GetFileName(stray)} sits directly in spec/. Modules live in a module directory, "
                + "spec/<area>/, with their cfg, manifest, mutations and refinement note; a module anywhere "
                + "else is invisible to every gate.");
        }

        foreach (var directory in System.IO.Directory.EnumerateDirectories(root).Order(StringComparer.Ordinal))
        {
            modules.AddRange(DiscoverDirectory(root, directory, problems));
        }

        foreach (var duplicate in modules.GroupBy(m => m.Name, StringComparer.Ordinal).Where(g => g.Count() > 1))
        {
            problems.Add(
                $"module '{duplicate.Key}' is declared in more than one directory "
                + $"({string.Join(", ", duplicate.Select(m => m.Describe(m.Directory)))}). Module names are "
                + "test-case names, so they must be unique across spec/.");
        }

        if (problems.Count > 0)
        {
            throw new InvalidOperationException(
                $"spec/ holds {problems.Count} malformed module artefact(s). Every directory under spec/ "
                + "must be a well-formed module directory; see spec/README.md for the layout."
                + Environment.NewLine + " - " + string.Join(Environment.NewLine + " - ", problems));
        }

        return modules;
    }

    private static IEnumerable<SpecModule> DiscoverDirectory(string root, string directory, List<string> problems)
    {
        var area = Path.GetFileName(directory);
        var where = $"spec/{area}/";

        var specifications = Stems(directory, "*.tla", ".tla");
        var configs = Stems(directory, "*.cfg", ".cfg");
        var manifests = Stems(directory, "*" + SpecModuleManifest.FileSuffix, SpecModuleManifest.FileSuffix);

        if (specifications.Count == 0)
        {
            problems.Add(
                $"{where} contains no .tla module. Every directory directly under spec/ is a module "
                + "directory; remove it or add the module.");
            return [];
        }

        if (!File.Exists(Path.Combine(directory, SpecModule.ReadmeFileName)))
        {
            problems.Add($"{where} has no {SpecModule.ReadmeFileName}, which carries the module's counts table.");
        }

        foreach (var orphan in configs.Except(specifications, StringComparer.Ordinal))
        {
            problems.Add($"{where}{orphan}.cfg has no {orphan}.tla beside it.");
        }

        foreach (var orphan in manifests.Except(specifications, StringComparer.Ordinal))
        {
            problems.Add($"{where}{orphan}{SpecModuleManifest.FileSuffix} has no {orphan}.tla beside it.");
        }

        var found = new List<SpecModule>();
        foreach (var name in specifications)
        {
            var before = problems.Count;

            if (!configs.Contains(name, StringComparer.Ordinal))
            {
                problems.Add(
                    $"{where}{name}.tla has no {name}.cfg. Every .tla in a module directory is a root module "
                    + "and needs a TLC model; a helper module that has none would be checked by no gate.");
            }

            var header = ModuleHeader.Match(File.ReadAllText(Path.Combine(directory, $"{name}.tla")));
            if (!header.Success || !string.Equals(header.Groups[1].Value, name, StringComparison.Ordinal))
            {
                problems.Add(
                    $"{where}{name}.tla does not open with '---- MODULE {name} ----'. TLC requires the module "
                    + "name to match the file name, and mutations rename the module by that header.");
            }

            SpecModuleManifest? manifest = null;
            var manifestPath = Path.Combine(directory, name + SpecModuleManifest.FileSuffix);
            if (!File.Exists(manifestPath))
            {
                problems.Add(
                    $"{where}{name}.tla has no {name}{SpecModuleManifest.FileSuffix}, which records where its "
                    + "mutations and refinement note live and the counts the gates assert.");
            }
            else
            {
                try
                {
                    manifest = SpecModuleManifest.Parse($"{where}{name}{SpecModuleManifest.FileSuffix}", File.ReadAllText(manifestPath));
                }
                catch (InvalidOperationException error)
                {
                    problems.Add(error.Message);
                }
            }

            if (manifest is null || problems.Count > before)
            {
                continue;
            }

            var module = new SpecModule
            {
                Name = name,
                SpecRoot = root,
                Directory = directory,
                Manifest = manifest,
            };

            if (!System.IO.Directory.Exists(module.MutationDirectory))
            {
                problems.Add($"{where}{name}: the manifest's mutation directory '{manifest.MutationsDirectory}' does not exist.");
            }

            if (!File.Exists(module.RefinementNotePath))
            {
                problems.Add($"{where}{name}: the manifest's refinement note '{manifest.RefinementNote}' does not exist.");
            }

            found.Add(module);
        }

        foreach (var shared in found.GroupBy(m => m.MutationDirectory, StringComparer.Ordinal).Where(g => g.Count() > 1))
        {
            problems.Add(
                $"{where}: modules {string.Join(", ", shared.Select(m => m.Name))} share the mutation directory "
                + $"'{shared.First().Manifest.MutationsDirectory}'. Each catalogue is checked against one module, "
                + "so a shared one would pair mutations with the wrong base.");
        }

        foreach (var shared in found.GroupBy(m => m.RefinementNotePath, StringComparer.Ordinal).Where(g => g.Count() > 1))
        {
            problems.Add(
                $"{where}: modules {string.Join(", ", shared.Select(m => m.Name))} share the refinement note "
                + $"'{shared.First().Manifest.RefinementNote}'. A note maps exactly one module's actions and properties.");
        }

        return found;
    }

    private static List<string> Stems(string directory, string pattern, string suffix) =>
        System.IO.Directory
            .EnumerateFiles(directory, pattern)
            .Select(f => Path.GetFileName(f))
            .Where(f => f.EndsWith(suffix, StringComparison.Ordinal))
            .Select(f => f[..^suffix.Length])
            .Order(StringComparer.Ordinal)
            .ToList();

    /// <summary>
    /// Discovers every module under <paramref name="specRoot"/> as
    /// <see cref="Discover(string)"/> does, then throws unless at least
    /// <paramref name="minimumModules"/> were found.
    /// </summary>
    public static IReadOnlyList<SpecModule> Discover(string specRoot, int minimumModules)
    {
        var modules = Discover(specRoot);
        if (modules.Count < minimumModules)
        {
            throw new InvalidOperationException(
                $"discovered {modules.Count} module(s) under '{specRoot}', fewer than the floor of "
                + $"{minimumModules}. Every Formal gate is parameterised over discovery, so this would "
                + "make all of them pass while checking nothing.");
        }

        return modules;
    }

    private static IReadOnlyList<SpecModule> DiscoverRepository() =>
        Discover(RepositorySpecRoot, MinimumRepositoryModules);
}
