using System.Diagnostics;
using System.Text.Json;
using Orleans.Lattice.Testing.Hygiene;
using Orleans.Lattice.Tests.Formal;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Pins <c>.github/workflows/tlc-fingerprint.py</c> (contract C4 of #5148,
/// #5160): the per-module content hash the TLC verdict cache keys a receipt on.
/// <para>
/// The direction that matters is omission. A file the TLC gates read for a
/// module but the fingerprint does not hash lets that file change while the
/// module's receipt stays valid, so a verdict nobody model-checked is reused.
/// The completeness gate therefore drives the script UNMODIFIED and requires
/// every path <see cref="SpecModule"/> exposes to the TLC gates - the
/// specification, every sibling <c>.tla</c>, the cfg, the variant cfgs, the
/// manifest and the mutation files - to appear in that module's input list.
/// A synthetic module with one sibling omitted from its list is the negative
/// control that proves the gate can fail.
/// </para>
/// <para>
/// This fixture lives under <c>test/lattice/Hygiene/</c>, outside every
/// <c>verdictInputs</c> glob, so changing it never reruns exhaustive TLC.
/// </para>
/// </summary>
[TestFixture]
public sealed class TlcFingerprintCompletenessTests
{
    private const string Script = ".github/workflows/tlc-fingerprint.py";
    private const string InputsFile = ".github/workflows/tlc-inputs.json";
    private const string SiblingName = "Helper.tla";

    /// <summary>
    /// The active lines of <c>.gitattributes</c> whose line-ending rules the
    /// script's normaliser mirrors.
    /// </summary>
    private static readonly string[] MirroredGitAttributes = ["* text=auto", "tools/tlc/** text eol=lf", "tools/ci-runners/** text eol=lf"];

    /// <summary>Bounds every child interpreter; tripping it means a hang.</summary>
    private static readonly TimeSpan ScriptTimeout = TimeSpan.FromMinutes(5);

    private static readonly Lazy<Dictionary<string, HashSet<string>>> RepositoryInputs =
        new(() => Fingerprint(RepoRoot, inputsFile: null));

    private static string RepoRoot => HygieneRepository.FindRepoRoot();

    [Test]
    public void The_fingerprint_self_test_passes()
    {
        var (exitCode, stdout, stderr) = RunPython([Path.Combine(RepoRoot, Script), "--self-test"]);

        Assert.That(exitCode, Is.Zero, $"{Script} --self-test failed.{Environment.NewLine}{stdout}{stderr}");
    }

    [Test]
    public void The_fingerprint_covers_exactly_the_discovered_modules()
    {
        var discovered = SpecModuleCatalogue.Repository().Select(m => m.Name).Order(StringComparer.Ordinal);

        Assert.That(
            RepositoryInputs.Value.Keys.Order(StringComparer.Ordinal),
            Is.EqualTo(discovered),
            $"{Script} must fingerprint every module SpecModuleCatalogue discovers, and no other: a module "
                + "with no fingerprint has no receipt key, and a fingerprint with no module is a discovery rule "
                + "the two disagree on.");
    }

    [Test]
    public void Every_path_a_module_exposes_to_the_TLC_gates_is_one_of_its_fingerprint_inputs()
    {
        var modules = SpecModuleCatalogue.Repository();
        Assert.That(modules, Is.Not.Empty, "discovery found no module, so this gate would check nothing");

        Assert.Multiple(() =>
        {
            foreach (var module in modules)
            {
                Assert.That(
                    RepositoryInputs.Value.TryGetValue(module.Name, out var inputs),
                    Is.True,
                    $"{Script} produced no fingerprint for {module.Name}.");

                Assert.That(
                    MissingInputs(module, RepoRoot, inputs ?? new HashSet<string>(StringComparer.Ordinal)),
                    Is.Empty,
                    $"{module.Name}: these files are read by the TLC gates but not hashed into the module's "
                        + $"fingerprint, so editing one would reuse a stale verdict. Fix the module discovery in "
                        + $"{Script}, or, for a new file, check it is tracked or added.");
            }
        });
    }

    [Test]
    public void The_completeness_gate_reports_a_sibling_omitted_from_a_synthetic_module()
    {
        using var synthetic = SyntheticSpecModule.Create();
        var module = synthetic.Discover();
        synthetic.Write(SiblingName, "---- MODULE Helper ----\n====");
        var root = Path.GetDirectoryName(synthetic.SpecRoot)!;
        var sibling = $"spec/{SyntheticSpecModule.AreaName}/{SiblingName}";

        var inputs = Fingerprint(root, Path.Combine(RepoRoot, InputsFile))[SyntheticSpecModule.ModuleName];

        Assert.Multiple(() =>
        {
            Assert.That(inputs, Does.Contain(sibling), "the script must hash a sibling it was never told about");
            Assert.That(MissingInputs(module, root, inputs), Is.Empty, "the unmodified list must pass the gate");

            inputs.Remove(sibling);
            Assert.That(
                MissingInputs(module, root, inputs),
                Is.EqualTo(new[] { sibling }),
                "with the sibling omitted from the input list, the gate must name exactly that sibling");
        });
    }

    [Test]
    public void The_gitattributes_line_ending_rules_the_normaliser_mirrors_are_unchanged()
    {
        var active = File.ReadAllLines(Path.Combine(RepoRoot, ".gitattributes"))
            .Select(line => line.Trim())
            .Where(line => line.Length > 0 && !line.StartsWith('#'))
            .ToArray();

        Assert.That(
            active,
            Is.EqualTo(MirroredGitAttributes),
            $"{Script} normalises line endings as these .gitattributes rules do, so a Windows and a Linux "
                + "checkout fingerprint identically. A changed rule must be mirrored in its normalise() and "
                + "FORCED_TEXT, then here.");
    }

    /// <summary>
    /// The repository-relative paths <paramref name="module"/> exposes to the
    /// TLC gates that <paramref name="inputs"/> does not list, in ordinal order.
    /// </summary>
    private static List<string> MissingInputs(SpecModule module, string root, IReadOnlySet<string> inputs) =>
        ExposedPaths(module)
            .Select(path => Path.GetRelativePath(root, path).Replace('\\', '/'))
            .Distinct(StringComparer.Ordinal)
            .Where(path => !inputs.Contains(path))
            .Order(StringComparer.Ordinal)
            .ToList();

    /// <summary>Every file the TLC gates read for <paramref name="module"/>.</summary>
    private static IEnumerable<string> ExposedPaths(SpecModule module)
    {
        yield return module.SpecificationPath;

        foreach (var sibling in module.ReadSiblingSpecifications().Keys)
        {
            yield return Path.Combine(module.Directory, sibling);
        }

        yield return module.ConfigPath;

        foreach (var variant in module.Manifest.Variants.Keys)
        {
            yield return module.VariantConfigPath(variant);
        }

        yield return module.ManifestPath;

        // The same enumeration SpecMutationCatalogue.Load uses.
        foreach (var mutation in Directory.EnumerateFiles(module.MutationDirectory, "*.mutation"))
        {
            yield return mutation;
        }
    }

    /// <summary>Runs the script over <paramref name="root"/> and returns each module's input set.</summary>
    private static Dictionary<string, HashSet<string>> Fingerprint(string root, string? inputsFile)
    {
        var output = Path.Combine(Path.GetTempPath(), $"tlc-fingerprint-{Guid.NewGuid():N}.json");
        try
        {
            List<string> arguments = [Path.Combine(RepoRoot, Script), "--root", root, "--output", output];
            if (inputsFile is not null)
            {
                arguments.AddRange(["--inputs", inputsFile]);
            }

            var (exitCode, stdout, stderr) = RunPython(arguments);
            Assert.That(exitCode, Is.Zero, $"{Script} failed.{Environment.NewLine}{stdout}{stderr}");

            using var document = JsonDocument.Parse(File.ReadAllText(output));
            Assert.That(document.RootElement.GetProperty("schema").GetInt32(), Is.EqualTo(1), "C4 schema");

            return document.RootElement
                .GetProperty("modules")
                .EnumerateObject()
                .ToDictionary(
                    module => module.Name,
                    module => module.Value.GetProperty("inputs").EnumerateArray().Select(p => p.GetString()!).ToHashSet(StringComparer.Ordinal),
                    StringComparer.Ordinal);
        }
        finally
        {
            File.Delete(output);
        }
    }

    /// <summary>
    /// Runs a script under the first working Python interpreter. Missing Python
    /// is a broken pipeline under CI and a visible skip locally - never an
    /// inconclusive, which NUnit would not count.
    /// </summary>
    private static (int ExitCode, string StandardOutput, string StandardError) RunPython(IEnumerable<string> arguments)
    {
        var interpreter = FindPython();

        if (interpreter is null)
        {
            if (string.Equals(Environment.GetEnvironmentVariable("GITHUB_ACTIONS"), "true", StringComparison.OrdinalIgnoreCase))
            {
                Assert.Fail("no Python interpreter was found on the CI runner, so the fingerprint cannot be checked");
            }

            Assert.Ignore("no Python interpreter is available on this host");
        }

        var start = new ProcessStartInfo(interpreter!)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            WorkingDirectory = RepoRoot,
        };

        foreach (var argument in arguments)
        {
            start.ArgumentList.Add(argument);
        }

        return Run(start);
    }

    private static string? FindPython()
    {
        // `python3` on Windows is often a Store alias stub that prints an error,
        // so each candidate is probed rather than assumed.
        string[] candidates = OperatingSystem.IsWindows() ? ["python", "py", "python3"] : ["python3", "python"];

        foreach (var candidate in candidates)
        {
            try
            {
                var probe = new ProcessStartInfo(candidate)
                {
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                };
                probe.ArgumentList.Add("--version");

                var (exitCode, stdout, stderr) = Run(probe);

                if (exitCode == 0 && (stdout + stderr).Contains("Python 3", StringComparison.Ordinal))
                {
                    return candidate;
                }
            }
            catch (System.ComponentModel.Win32Exception)
            {
                // Not on PATH.
            }
        }

        return null;
    }

    /// <summary>
    /// Drains both pipes before waiting, and bounds the wait, so a stuck child
    /// fails with a diagnosis instead of a blame-hang abort.
    /// </summary>
    private static (int ExitCode, string StandardOutput, string StandardError) Run(ProcessStartInfo start)
    {
        using var process = Process.Start(start)!;
        var stdoutTask = process.StandardOutput.ReadToEndAsync();
        var stderrTask = process.StandardError.ReadToEndAsync();

        if (!process.WaitForExit((int)ScriptTimeout.TotalMilliseconds))
        {
            try
            {
                process.Kill(entireProcessTree: true);
            }
            catch (InvalidOperationException)
            {
                // Already exited.
            }

            Assert.Fail($"'{start.FileName} {string.Join(' ', start.ArgumentList)}' did not exit within {ScriptTimeout}.");
        }

        return (process.ExitCode, stdoutTask.GetAwaiter().GetResult(), stderrTask.GetAwaiter().GetResult());
    }
}
