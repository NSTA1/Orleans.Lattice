namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// The neutral class must never swallow a TLC module. A module is a
/// <c>.tla</c> with a <c>.manifest.json</c> beside it (<c>SpecModule</c>,
/// <c>tlc-fingerprint.py</c>); a neutral file changes no fingerprint and runs no
/// exhaustive TLC, so a neutral module would be model-checked by nobody.
/// <c>spec/**/design/**</c> is neutral because an unmanifested design model is
/// not a module; this guard fails the moment one gains a manifest there, which
/// is when it must become a verdict input (#5605 member 6, its promotion).
/// </summary>
public sealed partial class TlcInputClassificationTests
{
    private const string ManifestSuffix = ".manifest.json";

    [Test]
    public void No_TLC_module_is_neutral()
    {
        var inputs = Load();
        var tracked = TrackedRelativeFiles().ToList();

        Assert.Multiple(() =>
        {
            Assert.That(tracked.Count(path => path.EndsWith(ManifestSuffix, StringComparison.Ordinal) && path.StartsWith("spec/", StringComparison.Ordinal)),
                Is.GreaterThan(0), "no module manifest is tracked under spec/, so this rule checks nothing");
            Assert.That(tracked.Count(path => path.Contains("/design/", StringComparison.Ordinal) && inputs.IsNeutral(path)),
                Is.GreaterThan(0), "no design file is neutral, so the design glob is dead configuration");
            Assert.That(NeutralModuleProblems(tracked, inputs), Is.Empty);
        });
    }

    [Test]
    public void Negative_control_a_manifest_under_a_design_folder_fails()
    {
        var inputs = Load();
        var tracked = TrackedRelativeFiles().ToList();
        string[] promoted = ["spec/wal/design/WalLiveAdvance.manifest.json", "spec/wal/design/WalLiveAdvance.tla"];

        Assert.Multiple(() =>
        {
            Assert.That(NeutralModuleProblems(tracked, inputs), Is.Empty, "control: the real tree has no neutral module");
            Assert.That(NeutralModuleProblems([.. tracked, .. promoted], inputs),
                Has.Some.Contains("spec/wal/design/WalLiveAdvance.manifest.json"));
        });
    }

    [Test]
    public void Negative_control_a_neutral_glob_that_swallows_a_real_module_fails()
    {
        var tracked = TrackedRelativeFiles().ToList();

        Assert.That(NeutralModuleProblems(tracked, Load().WithNeutral("spec/wal/*")),
            Has.Some.Contains("spec/wal/WalDurability"));
    }

    /// <summary>
    /// Every module manifest under spec/, its <c>.tla</c> and every cfg it owns
    /// must not be neutral.
    /// </summary>
    private static List<string> NeutralModuleProblems(IReadOnlyCollection<string> trackedPaths, TlcInputs inputs)
    {
        var problems = new List<string>();
        foreach (var manifest in trackedPaths.Where(path =>
            path.StartsWith("spec/", StringComparison.Ordinal) && path.EndsWith(ManifestSuffix, StringComparison.Ordinal)))
        {
            var module = manifest[..^ManifestSuffix.Length];
            var owned = trackedPaths
                .Where(path => path == manifest
                    || path == module + ".tla"
                    || (path.StartsWith(module + ".", StringComparison.Ordinal) && path.EndsWith(".cfg", StringComparison.Ordinal)))
                .Append(module + ".tla")
                .Distinct(StringComparer.Ordinal);
            foreach (var path in owned.Where(inputs.IsNeutral))
            {
                problems.Add($"{path} belongs to the module {manifest} but is neutral, so no exhaustive TLC run would check it; narrow the neutral glob or promote the module out of it");
            }
        }

        return problems;
    }
}
