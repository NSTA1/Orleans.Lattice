using System.IO;
using NUnit.Framework;

namespace Orleans.Lattice.Testing.Hygiene;

/// <summary>
/// Perturbation-residue gate. Fails if a perturbation marker survives into a
/// tracked file anywhere in the repository.
/// <para>
/// <b>The failure this defends against.</b> A perturbation arm edits a source
/// file, builds, runs a test, and restores. If the shell running it is killed
/// between the edit and the restore - a timeout, a cancelled turn, a closed
/// window - the perturbed source stays in the working tree. In
/// <c>git status</c> it is a modified tracked file, byte-for-byte
/// indistinguishable from the author's own work in progress, and one
/// <c>git add -A</c> away from being committed. The defence cannot be "always
/// restore", because the restore step is precisely the one that did not run.
/// </para>
/// <para>
/// <b>Why this is a gate and not guidance.</b> Both halves of issue #2735 are
/// about tests that pass while proving nothing, so answering them with more
/// prose would repeat the mistake in a new register. A residue check is
/// mechanical: it runs in the same non-chaos suite as every other hygiene gate,
/// it fails the required <c>build-and-test</c> check, and it costs one file
/// walk.
/// </para>
/// <para>
/// <b>It is only as good as the marker convention, and that is deliberate.</b>
/// This gate detects a perturbation that stamped the marker. A perturbation
/// applied as a bare string substitution leaves nothing to detect and this gate
/// will not see it - which is why the marker is required of every perturbation
/// driver in <c>.github/instructions/testing.instructions.md</c> rather than
/// being optional. The gate makes the convention enforceable; it does not
/// replace it. Stating the blind spot here rather than implying coverage it
/// does not have is the point - a guard whose limit is undocumented is itself
/// the third false green.
/// </para>
/// </summary>
public abstract class PerturbationResidueHygieneTestsBase
{
    // Assembled at runtime from two halves so this file - which must name the
    // marker in order to search for it - does not contain the literal token and
    // is therefore not flagged by its own scan. The same technique the em-dash
    // gate uses on itself, and for the same reason: a self-flagging guard gets
    // suppressed, and a suppressed guard is worse than none because it still
    // reads as coverage.
    private const string MarkerHead = "LATTICE";
    private const string MarkerTail = "-PERTURBATION";

    /// <summary>
    /// The token a perturbation driver stamps beside every edit it makes, so
    /// residue is detectable rather than merely undesirable.
    /// </summary>
    public static string Marker => MarkerHead + MarkerTail;

    /// <summary>The repository slice this fixture is responsible for scanning.</summary>
    protected abstract HygieneScanScope Scope { get; }

    /// <summary>
    /// Scans every tracked text file in scope and fails if the perturbation
    /// marker is present, naming each file and line.
    /// </summary>
    [Test]
    public void No_perturbation_markers_survive_into_tracked_files()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var files = HygieneFiles.EnumerateTextFiles(repoRoot, Scope).ToList();

        // Anti-vacuity control, asserted on the DENOMINATOR and never on the
        // violation list. A scope whose roots have moved would otherwise scan
        // nothing and report a clean repository it never read - the same false
        // green the issue this gate serves is about.
        HygieneDenominator.RequireExamined(
            files.Count,
            nameof(PerturbationResidueHygieneTestsBase),
            "text files",
            HygieneDenominator.Describe(Scope));

        var violations = Scan(repoRoot, files);

        Assert.That(violations, Is.Empty,
            $"A perturbation marker ('{Marker}') survived into tracked content. A perturbation arm was "
            + "almost certainly interrupted between its edit and its restore, leaving perturbed source in "
            + "the working tree where git status cannot tell it apart from your own edits. Restore the file "
            + "from the harness snapshot - not with Copy-Item, which leaves the perturbed BINARY in place "
            + "and lets a rebuild-free rerun report a green from a stale artefact - and stage explicit paths "
            + "rather than 'git add -A'."
            + Environment.NewLine
            + string.Join(Environment.NewLine, violations));
    }

    /// <summary>
    /// The positive control, and the reason this gate is not itself an instance
    /// of what it guards against. The assertion above concludes something from an
    /// EMPTY violation list, which is also exactly what a scanner that cannot
    /// match anything produces. This feeds the matcher a file that does carry the
    /// marker and requires it to be found, at the right line, so the green above
    /// is evidence rather than silence.
    /// <para>
    /// The probe file is written outside the repository on purpose. Writing it
    /// inside would be a closer end-to-end test and a worse idea: a run killed
    /// part-way would leave real residue in the working tree, which is the very
    /// accident this gate exists to catch, and a gate that can manufacture its
    /// own violations is not one an author can trust a red from.
    /// </para>
    /// </summary>
    [Test]
    public void Control_the_scan_finds_a_marker_that_is_present()
    {
        var probeRoot = Path.Combine(Path.GetTempPath(), "lattice-residue-probe-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(probeRoot);
        try
        {
            var probe = Path.Combine(probeRoot, "Perturbed.cs");
            File.WriteAllLines(
                probe,
                ["// untouched", "var x = 1; // " + Marker + " arm-3", "// untouched"]);

            var violations = Scan(probeRoot, [probe]);

            Assert.Multiple(() =>
            {
                Assert.That(violations, Has.Count.EqualTo(1),
                    "One marked line, one violation. If this is zero the scan matches nothing and every "
                    + "green this fixture reports is vacuous.");
                Assert.That(violations[0], Does.StartWith("Perturbed.cs:2:"),
                    "The line number is part of the contract: the failure message has to point an author at "
                    + "the residue, and an off-by-one would send them to the wrong line of a file they "
                    + "already believe is clean.");
            });
        }
        finally
        {
            Directory.Delete(probeRoot, recursive: true);
        }
    }

    /// <summary>
    /// Finds every line carrying the marker across the supplied files.
    /// </summary>
    /// <param name="root">The root that paths are reported relative to. Must not be <see langword="null"/>.</param>
    /// <param name="files">The files to scan. Must not be <see langword="null"/>.</param>
    /// <returns>One entry per marked line, formatted <c>path:line: text</c>.</returns>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
    private static List<string> Scan(string root, IEnumerable<string> files)
    {
        ArgumentNullException.ThrowIfNull(root);
        ArgumentNullException.ThrowIfNull(files);

        var marker = Marker;
        var violations = new List<string>();
        foreach (var file in files)
        {
            var rel = Path.GetRelativePath(root, file).Replace('\\', '/');

            // This file necessarily names the marker in order to search for it,
            // so it is skipped by PATH rather than by suppressing the token.
            // Skipping by path keeps the token greppable here, which is how a
            // reader who hits a red finds the convention that produced it.
            if (rel.EndsWith(nameof(PerturbationResidueHygieneTestsBase) + ".cs", StringComparison.Ordinal))
            {
                continue;
            }

            var lines = File.ReadAllLines(file);
            for (var i = 0; i < lines.Length; i++)
            {
                if (lines[i].Contains(marker, StringComparison.Ordinal))
                {
                    violations.Add($"{rel}:{i + 1}: {lines[i].Trim()}");
                }
            }
        }

        return violations;
    }
}
