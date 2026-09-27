using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Docs;

/// <summary>
/// Keeps the prepare-ledger guidance explicit about both persistence and
/// activation risk, including on the default SQLite profile.
/// </summary>
[TestFixture]
[Category("Docs")]
public sealed class UnresolvedPrepareLedgerGuidanceTests
{
    [Test]
    public void Instrument_description_recommends_alerting_on_every_profile()
    {
        AssertGuidance(LatticeMetrics.LeafUnresolvedPrepareLedgerBeyondCap.Description);
    }

    [TestCase("docs/lattice/metrics.md", "| `orleans.lattice.leaf.unresolved_prepare_ledger_beyond_cap`", "\n")]
    [TestCase("docs/lattice.dashboards/metrics-to-panel-map.md", "| `orleans.lattice.leaf.unresolved_prepare_ledger_beyond_cap`", "\n")]
    [TestCase("docs/lattice/configuration.md", "The bound applies to **deferred terminals only**.", "\n")]
    [TestCase("docs/lattice/tree-storage.md", "Only one thing can grow this row past a provider limit", "\n")]
    [TestCase("src/lattice/BPlusTree/Grains/BPlusLeafGrain.DurableReplayWork.cs", "/// Issue #2183 observability.", "/// </para>")]
    [TestCase("src/lattice/BPlusTree/Grains/BPlusLeafGrain.DurableReplayWork.cs", "\"Leaf {TreeId} has {Count}", "state.State.TreeId, work.Count, thresholdCap")]
    [TestCase("src/lattice/LatticeMetrics.cs", "/// Counter of resident unresolved saga prepares", "/// </summary>")]
    public void Risk_note_covers_both_directions_and_every_profile(string path, string start, string end)
    {
        var source = File.ReadAllText(Path.Combine(HygieneRepository.FindRepoRoot(), path));
        var startIndex = source.IndexOf(start, StringComparison.Ordinal);
        Assert.That(startIndex, Is.GreaterThanOrEqualTo(0), $"Missing guidance anchor in {path}.");
        var endIndex = source.IndexOf(end, startIndex + start.Length, StringComparison.Ordinal);
        Assert.That(endIndex, Is.GreaterThan(startIndex), $"Missing guidance terminator in {path}.");
        var guidance = source[startIndex..endIndex];
        guidance = Regex.Replace(guidance, @"///|""\s*\+\s*""", " ");
        AssertGuidance(Regex.Replace(guidance, @"\s+", " "));
    }

    private static void AssertGuidance(string? guidance)
    {
        Assert.Multiple(() =>
        {
            Assert.That(guidance, Does.Contain("persist").IgnoreCase);
            Assert.That(guidance, Does.Contain("activation").IgnoreCase);
            Assert.That(guidance, Does.Contain("Azure Table"));
            Assert.That(guidance, Does.Contain("SQLite"));
            Assert.That(guidance, Does.Match("bound(s|ing) persisted row growth"));
            Assert.That(guidance, Does.Contain("read budget"));
            Assert.That(guidance, Does.Contain("before grain-level repair"));
            Assert.That(guidance, Does.Match("(?i)alert[^.]*every[^.]*profile"));
            Assert.That(guidance, Does.Not.Contain("benign").IgnoreCase);
            Assert.That(guidance, Does.Not.Contain("dead weight").IgnoreCase);
        });
    }
}
