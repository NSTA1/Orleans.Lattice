using Orleans.Lattice.Explorer.EndToEnd.Tests.Pending;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.SelfTests;

/// <summary>
/// The pending wrappers' contract (#5361): <see cref="KnownDefect"/> passes while its defect
/// reproduces and <see cref="KnownGap"/> while its affordance is absent; each fails loudly once
/// its issue is resolved; and the source scan lists every wrapper written.
/// </summary>
[TestFixture]
[Category("EndToEnd")]
public sealed class PendingWrapperSelfTests
{
    /// <summary>A defect that still reproduces lets the step pass, and the run lists the wrapper.</summary>
    [Test]
    public async Task A_known_defect_that_still_reproduces_passes_and_is_listed()
    {
        var wrapper = new KnownDefect("#9001", "Self-test defect that still reproduces");

        await wrapper.ReproducesAsync(() => Task.CompletedTask);

        Assert.That(PendingWrappers.RanThisRun, Has.Some.Matches<PendingWrapper>(ran => ran.Kind == nameof(KnownDefect) && ran.Issue == "#9001"));
    }

    /// <summary>A defect that no longer reproduces fails loudly, naming its issue and the fix.</summary>
    [Test]
    public void A_known_defect_that_no_longer_reproduces_fails_loudly()
    {
        var wrapper = new KnownDefect("#9002", "Self-test defect that was fixed");

        Assert.That(
            async () => await wrapper.ReproducesAsync(() => throw new InvalidOperationException("the defective behaviour is gone")),
            Throws.TypeOf<PendingWrapperResolvedException>()
                .With.Message.Contains("KnownDefect(#9002) no longer reproduces")
                .And.Message.Contains("Remove the wrapper")
                .And.InnerException.TypeOf<InvalidOperationException>());
    }

    /// <summary>A gap whose affordance is still absent lets the step pass, and the run lists the wrapper.</summary>
    [Test]
    public async Task A_known_gap_that_is_still_absent_passes_and_is_listed()
    {
        var wrapper = new KnownGap("#9003", "Self-test gap still absent");

        await wrapper.StillAbsentAsync(() => Task.CompletedTask);

        Assert.That(PendingWrappers.RanThisRun, Has.Some.Matches<PendingWrapper>(ran => ran.Kind == nameof(KnownGap) && ran.Issue == "#9003"));
    }

    /// <summary>A gap whose affordance now exists fails loudly, naming its issue and the fix.</summary>
    [Test]
    public void A_known_gap_that_has_landed_fails_loudly()
    {
        var wrapper = new KnownGap("#9004", "Self-test gap that landed");

        Assert.That(
            async () => await wrapper.StillAbsentAsync(() => throw new InvalidOperationException("the affordance is here")),
            Throws.TypeOf<PendingWrapperResolvedException>()
                .With.Message.Contains("KnownGap(#9004) is no longer a gap")
                .And.Message.Contains("Remove the wrapper"));
    }

    /// <summary>A wrapper names its issue as #N.</summary>
    [Test]
    public void A_wrapper_names_its_issue_as_hash_number()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => new KnownDefect("9005", "no hash"), Throws.ArgumentException);
            Assert.That(() => new KnownGap("#0", "zero"), Throws.ArgumentException);
            Assert.That(() => new KnownGap("#12", " "), Throws.ArgumentException);
        });
    }

    /// <summary>
    /// The scan lists a wrapper written with literals, with its file and line, and reports one it
    /// cannot read rather than silently omitting it; the self-tests and wrapper definitions are skipped.
    /// </summary>
    [Test]
    public void The_scan_lists_written_wrappers_and_reports_unreadable_ones()
    {
        var root = Path.Combine(Path.GetTempPath(), "lattice-e2e-wrappers-" + Guid.NewGuid().ToString("N"));
        var project = Path.Combine(root, "project");
        try
        {
            Directory.CreateDirectory(Path.Combine(project, "Journeys", "A3"));
            Directory.CreateDirectory(Path.Combine(project, "SelfTests"));
            File.WriteAllText(Path.Combine(project, "Journeys", "A3", "Steps.cs"),
                "class Steps\n{\n    async Task Run()\n    {\n        await new KnownGap(\"#5368\", \"No \\\"New tree\\\" yet\").StillAbsentAsync(Absent);\n        await new KnownDefect(\"#5412\", summary).ReproducesAsync(Defect);\n    }\n}\n");
            File.WriteAllText(Path.Combine(project, "SelfTests", "Skipped.cs"), "new KnownDefect(\"#1\", \"skipped\");");

            var written = PendingWrappers.Scan(project, root, out var unreadable);

            Assert.Multiple(() =>
            {
                Assert.That(written, Has.Count.EqualTo(1));
                Assert.That(written[0], Is.EqualTo(new PendingWrapper(nameof(KnownGap), "#5368", "No \"New tree\" yet", "project/Journeys/A3/Steps.cs", 5)));
                Assert.That(unreadable, Is.EqualTo(new[] { "project/Journeys/A3/Steps.cs:6" }));
                Assert.That(PendingWrappers.ToMarkdown(written, [], hard: false), Does.Contain("#5368").And.Contain("Steps.cs:5"));
            });
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }
}
