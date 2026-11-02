using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.SelfTests;

/// <summary>The coverage gate's evaluation on a small catalogue: covered, exempt, uncovered and stale entries are told apart.</summary>
[TestFixture]
[Category("EndToEnd")]
public sealed class CoverageGateSelfTests
{
    /// <summary>A hit covers its row, an exemption excuses its row, any other row is uncovered, and stale names are reported.</summary>
    [Test]
    public void The_gate_tells_covered_exempt_and_uncovered_apart()
    {
        var catalogue = FeatureCatalogue.Parse(
            string.Join('\n',
                "id\tkind\tsource\tdescription\tjourney",
                "route:/a\troute\tPageA\tThe page at /a\tA1",
                "route:/b\troute\tPageB\tThe page at /b\tA2",
                "use-case:shell/c\tuse-case\tdocs/lattice.explorer/areas.md#data\tC\tA3"),
            "selftest.tsv");
        var exemptions = FeatureCatalogue.ParseExemptions("id\treason\nroute:/b\tNo journey can reach it in CI.\nroute:/gone\tA removed page.\n", "selftest-exemptions.tsv");

        var report = CoverageGate.Evaluate(catalogue, exemptions, new HashSet<string>(StringComparer.Ordinal) { "route:/a", "route:/old" });
        var markdown = report.ToMarkdown(hard: false);

        Assert.Multiple(() =>
        {
            Assert.That(report.Covered, Is.EquivalentTo(new[] { "route:/a" }));
            Assert.That(report.Exempt, Is.EquivalentTo(new[] { "route:/b" }));
            Assert.That(report.Uncovered.Select(row => row.Id), Is.EqualTo(new[] { "use-case:shell/c" }));
            Assert.That(report.UnknownExemptions, Is.EqualTo(new[] { "route:/gone" }));
            Assert.That(report.UnknownHits, Is.EqualTo(new[] { "route:/old" }));
            Assert.That(report.IsComplete, Is.False);
            Assert.That(markdown, Does.Contain("1 of 3").And.Contain("reports without failing").And.Contain("use-case:shell/c"));
        });
    }

    /// <summary>A malformed catalogue fails with its line.</summary>
    [Test]
    public void A_malformed_catalogue_fails_with_its_line()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => FeatureCatalogue.Parse("id\tkind\n", "bad.tsv"), Throws.TypeOf<FormatException>().With.Message.Contains("bad.tsv:1"));
            Assert.That(() => FeatureCatalogue.Parse("id\tkind\tsource\tdescription\tjourney\nroute:/a\troute\tPageA\tA1\n", "bad.tsv"),
                Throws.TypeOf<FormatException>().With.Message.Contains("bad.tsv:2"));
        });
    }

    /// <summary>Anchors follow GitHub: lower case, punctuation dropped, duplicates suffixed, fenced code ignored.</summary>
    [Test]
    public void Doc_anchors_follow_github()
    {
        var headings = DocAnchors.Headings("# Title\n## Admin set, quota caps and app bindings\n```\n## not a heading\n```\n## `LatticeExplorerWebOptions`\n### Groups\n### Groups\n");

        Assert.That(headings.Select(heading => heading.Anchor), Is.EqualTo(new[]
        {
            "title",
            "admin-set-quota-caps-and-app-bindings",
            "latticeexplorerweboptions",
            "groups",
            "groups-1",
        }));
    }
}
