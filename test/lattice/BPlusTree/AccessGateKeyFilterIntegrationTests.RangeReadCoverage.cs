using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration coverage for the range-read gate-coverage seam
/// (<see cref="ILattice.GetRangeReadGateCoverageAsync"/>, issue #2423) against a
/// real <see cref="ILatticeAccessGate"/>.
/// <para>
/// The defect these pin is the asymmetry between the point and range read paths.
/// A denied <b>point</b> read throws, so the caller cannot miss it. A denied
/// <b>range</b> read resolves to a reject-all key filter and yields a clean,
/// successful, empty result, so the caller cannot see it at all. That is the
/// shared mechanism behind issues #2277, #2252/#2407, #2406 and #2480: an empty
/// result read as a factual negative.
/// </para>
/// <para>
/// The decisive test is
/// <see cref="Denied_and_genuinely_empty_ranges_are_indistinguishable_on_the_plain_scan"/>,
/// which demonstrates the ambiguity on the plain scan and then resolves it from
/// the coverage seam using the identical inputs.
/// </para>
/// </summary>
public partial class AccessGateKeyFilterIntegrationTests
{
    [Test]
    public async Task Denied_and_genuinely_empty_ranges_are_indistinguishable_on_the_plain_scan()
    {
        // The decisive test for issue #2423.
        //
        // Two trees. One holds four entries and denies the range outright; the
        // other is genuinely empty and is fully allowed. The plain scan answers
        // identically for both - zero keys, no exception, no diagnostic - so no
        // inspection of the returned sequence could ever separate "you may not
        // look" from "there is nothing there". The coverage seam is given the
        // same ranges and recovers the distinction, because it reports the gate
        // decision rather than the shape of the answer.
        const string deniedTree = "agf-rangecov-denied";
        const string emptyTree = "agf-rangecov-empty";
        var denied = _cluster.GrainFactory.GetGrain<ILattice>(deniedTree);
        var empty = _cluster.GrainFactory.GetGrain<ILattice>(emptyTree);

        await SeedAsync(denied, "user/alice", "user/amy", "user/bob", "user/carol");
        // emptyTree is deliberately never seeded.

        ConfigurableAccessGate.Decide = req =>
            req.TreeId == deniedTree
                ? LatticeAccessDecision.Deny("range denied for the test")
                : LatticeAccessDecision.Allow();

        var deniedKeys = await CollectKeysAsync(denied);
        var emptyKeys = await CollectKeysAsync(empty);

        var deniedCoverage = await denied.GetRangeReadGateCoverageAsync();
        var emptyCoverage = await empty.GetRangeReadGateCoverageAsync();

        Assert.Multiple(() =>
        {
            // The ambiguity, demonstrated rather than asserted in prose.
            Assert.That(deniedKeys, Is.Empty, "Denied: every key withheld by the gate.");
            Assert.That(emptyKeys, Is.Empty, "Genuinely empty: nothing was ever written.");
            Assert.That(
                deniedKeys,
                Is.EqualTo(emptyKeys),
                "The plain scan reports a fully-denied range and an empty store identically.");

            // And the distinction is now recoverable.
            Assert.That(deniedCoverage, Is.EqualTo(LatticeRangeReadGateCoverage.Denied));
            Assert.That(emptyCoverage, Is.EqualTo(LatticeRangeReadGateCoverage.Unrestricted));
        });
    }

    [Test]
    public async Task GetRangeReadGateCoverageAsync_reports_Unrestricted_on_the_ungated_path()
    {
        const string treeId = "agf-rangecov-ungated";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice", "user/bob");
        // No decision configured: the default allow with a null key filter, which
        // is the zero-per-key-cost hot path every ungated deployment takes.

        var coverage = await tree.GetRangeReadGateCoverageAsync();

        Assert.That(coverage, Is.EqualTo(LatticeRangeReadGateCoverage.Unrestricted));
    }

    [Test]
    public async Task GetRangeReadGateCoverageAsync_reports_Filtered_under_a_partial_allow()
    {
        // A partial allow is the third outcome, and it is the one the two-way
        // filter type collapsed together with a deny. An empty result under a
        // filtered allow is just as uninterpretable as under a deny, so it must
        // not report Unrestricted.
        const string treeId = "agf-rangecov-filtered";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice", "user/amy", "user/bob");
        FilterUserAToTree(treeId);

        var coverage = await tree.GetRangeReadGateCoverageAsync();
        var keys = await CollectKeysAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(coverage, Is.EqualTo(LatticeRangeReadGateCoverage.Filtered));
            Assert.That(
                keys,
                Is.EquivalentTo(new[] { "user/alice", "user/amy" }),
                "The seam reports coverage; it does not change what the gate admits.");
        });
    }

    [Test]
    public async Task A_denied_scan_still_returns_empty_rather_than_throwing()
    {
        // Pins the behaviour this seam deliberately does NOT change. Making a
        // denied range read throw would be a breaking change for every existing
        // caller, so the empty result stays and the coverage is reported beside
        // it. If this test ever fails, the seam has become breaking.
        const string treeId = "agf-rangecov-nothrow";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice");
        ConfigurableAccessGate.Decide = req =>
            req.TreeId == treeId
                ? LatticeAccessDecision.Deny("range denied for the test")
                : LatticeAccessDecision.Allow();

        var keys = await CollectKeysAsync(tree);
        var entries = new List<string>();
        await foreach (var e in tree.EntriesAsync())
        {
            entries.Add(e.Key);
        }

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.Empty);
            Assert.That(entries, Is.Empty);
        });
    }

    [Test]
    public async Task A_denied_count_reports_zero_and_the_coverage_seam_explains_it()
    {
        // CountAsync is the range verb whose empty answer is most obviously a
        // factual claim: zero is a number, not an absence, so a caller is even
        // less likely to suspect authorization. Both overloads route through the
        // same reject-all filter.
        const string treeId = "agf-rangecov-count";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice", "user/bob", "user/carol");
        ConfigurableAccessGate.Decide = req =>
            req.TreeId == treeId
                ? LatticeAccessDecision.Deny("range denied for the test")
                : LatticeAccessDecision.Allow();

        var whole = await tree.CountAsync();
        var ranged = await tree.CountAsync("user/", "user0");
        var coverage = await tree.GetRangeReadGateCoverageAsync("user/", "user0");

        Assert.Multiple(() =>
        {
            Assert.That(whole, Is.Zero, "A denied whole-tree count reports zero, not a denial.");
            Assert.That(ranged, Is.Zero, "A denied ranged count reports zero, not a denial.");
            Assert.That(
                coverage,
                Is.EqualTo(LatticeRangeReadGateCoverage.Denied),
                "The zero is an authorization outcome, and the seam says so.");
        });
    }

    [Test]
    public async Task GetRangeReadGateCoverageAsync_honours_the_requested_bounds()
    {
        // Coverage is per-range, so a gate that denies one sub-range and allows
        // another must report each accordingly. This is what makes the seam
        // usable from a caller that scans a namespace prefix rather than a whole
        // tree, which is how every repo-context sweep reads.
        const string treeId = "agf-rangecov-bounds";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice", "sys/config");
        ConfigurableAccessGate.Decide = req =>
            req.TreeId == treeId && req.RangeStart is not null
            && req.RangeStart.StartsWith("sys/", StringComparison.Ordinal)
                ? LatticeAccessDecision.Deny("sys namespace is denied for the test")
                : LatticeAccessDecision.Allow();

        var userCoverage = await tree.GetRangeReadGateCoverageAsync("user/", "user0");
        var sysCoverage = await tree.GetRangeReadGateCoverageAsync("sys/", "sys0");

        Assert.Multiple(() =>
        {
            Assert.That(userCoverage, Is.EqualTo(LatticeRangeReadGateCoverage.Unrestricted));
            Assert.That(sysCoverage, Is.EqualTo(LatticeRangeReadGateCoverage.Denied));
        });
    }

    private static async Task<List<string>> CollectKeysAsync(ILattice tree)
    {
        var keys = new List<string>();
        await foreach (var key in tree.KeysAsync())
        {
            keys.Add(key);
        }

        return keys;
    }
}
