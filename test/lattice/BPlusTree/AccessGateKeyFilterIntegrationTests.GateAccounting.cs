using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration coverage for the gate-accounting multi-read seam
/// (<see cref="ILattice.GetManyWithGateAccountingAsync"/>, issue #2277) against a
/// real <see cref="ILatticeAccessGate"/>.
/// <para>
/// The defect these pin is not that the gate prunes - that is its job - but that
/// a caller reading coverage from the returned rows cannot tell a pruned key from
/// a key that was never written, because both are simply absent. The decisive test
/// here is <see cref="Pruned_and_never_written_keys_are_indistinguishable_on_the_plain_read"/>,
/// which demonstrates the ambiguity on the plain read and then resolves it on the
/// accounting read using the identical inputs.
/// </para>
/// </summary>
public partial class AccessGateKeyFilterIntegrationTests
{
    [Test]
    public async Task GetManyWithGateAccountingAsync_reports_how_many_keys_the_gate_pruned()
    {
        const string treeId = "agf-gateacct-count";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice", "user/amy", "user/bob", "user/carol");
        FilterUserAToTree(treeId);

        var result = await tree.GetManyWithGateAccountingAsync(
            new List<string> { "user/alice", "user/amy", "user/bob", "user/carol" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Values.Keys, Is.EquivalentTo(new[] { "user/alice", "user/amy" }));
            Assert.That(result.PrunedByAccessGate, Is.EqualTo(2), "bob and carol were pruned by the filter.");
            Assert.That(result.IsComplete, Is.False, "A pruned read cannot be read as a complete one.");
            // The admitted values are unchanged: this seam adds a count, it does not
            // alter what the gate lets through.
            Assert.That(Encoding.UTF8.GetString(result.Values["user/alice"]), Is.EqualTo("user/alice"));
        });
    }

    [Test]
    public async Task GetManyWithGateAccountingAsync_reports_zero_pruned_on_the_ungated_path()
    {
        const string treeId = "agf-gateacct-ungated";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice", "user/bob");
        // No decision configured: the default allow with a null key filter, which is
        // the zero-per-key-cost hot path every ungated deployment takes.

        var result = await tree.GetManyWithGateAccountingAsync(
            new List<string> { "user/alice", "user/bob" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Values.Keys, Is.EquivalentTo(new[] { "user/alice", "user/bob" }));
            Assert.That(result.PrunedByAccessGate, Is.Zero);
            Assert.That(result.IsComplete, Is.True);
        });
    }

    [Test]
    public async Task GetManyWithGateAccountingAsync_does_not_count_a_missing_key_as_pruned()
    {
        // The count must be of keys the GATE removed, not of keys the read did not
        // answer with. Conflating the two would make every ordinary absence look
        // like a prune and stand the gap sweep down on a healthy repository - the
        // exact failure mode that made the originally proposed short-read check
        // unsafe.
        const string treeId = "agf-gateacct-absent";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice");
        FilterUserAToTree(treeId);

        var result = await tree.GetManyWithGateAccountingAsync(
            new List<string> { "user/alice", "user/anna" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Values.Keys, Is.EquivalentTo(new[] { "user/alice" }));
            Assert.That(
                result.PrunedByAccessGate,
                Is.Zero,
                "user/anna is admitted by the filter and simply does not exist, so nothing was pruned.");
            Assert.That(result.IsComplete, Is.True, "A genuine absence leaves the read interpretable.");
        });
    }

    [Test]
    public async Task GetManyWithGateAccountingAsync_counts_every_key_under_a_deny_all_filter()
    {
        const string treeId = "agf-gateacct-denyall";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice", "user/bob");
        ConfigurableAccessGate.Decide = req =>
            req.TreeId == treeId
                ? LatticeAccessDecision.Filtered(static _ => false)
                : LatticeAccessDecision.Allow();

        var result = await tree.GetManyWithGateAccountingAsync(
            new List<string> { "user/alice", "user/bob" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Values, Is.Empty);
            Assert.That(result.PrunedByAccessGate, Is.EqualTo(2));
            Assert.That(result.IsComplete, Is.False);
        });
    }

    [Test]
    public async Task Pruned_and_never_written_keys_are_indistinguishable_on_the_plain_read()
    {
        // The decisive test for issue #2277.
        //
        // Three requested keys, one of each interesting kind: admitted and present,
        // PRUNED by the gate (but present in the store), and admitted but NEVER
        // WRITTEN. The plain read answers with exactly one row, and the two missing
        // keys are byte-identical in its output - there is no field, no sentinel and
        // no count that separates them, which is precisely why no comparison of the
        // returned rows against the requested keys could ever recover the
        // distinction. The accounting read is given the IDENTICAL inputs and
        // recovers it, because the count comes from the layer that applied the
        // filter rather than from the shape of the answer.
        const string treeId = "agf-gateacct-decisive";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice", "user/bob");
        FilterUserAToTree(treeId);

        var keys = new List<string> { "user/alice", "user/bob", "user/anna" };
        var plain = await tree.GetManyAsync(new List<string>(keys));
        var accounted = await tree.GetManyWithGateAccountingAsync(new List<string>(keys));

        Assert.Multiple(() =>
        {
            // The ambiguity, demonstrated rather than asserted in prose: the plain
            // read reports both a pruned key and a never-written key the same way.
            Assert.That(plain.ContainsKey("user/bob"), Is.False, "Pruned by the gate.");
            Assert.That(plain.ContainsKey("user/anna"), Is.False, "Never written.");
            Assert.That(
                keys.Count - plain.Count,
                Is.EqualTo(2),
                "The plain read's shortfall lumps the pruned key together with the absent one.");

            // The plain seam is unchanged by this work - the accounting read is
            // additive, not a replacement - so the same rows come back.
            Assert.That(accounted.Values.Keys, Is.EquivalentTo(plain.Keys));

            // And the distinction is now recoverable by subtraction, which is the
            // whole point: notReturned(2) - pruned(1) = 1 genuinely absent key.
            Assert.That(accounted.PrunedByAccessGate, Is.EqualTo(1));
            Assert.That(
                keys.Count - accounted.Values.Count - accounted.PrunedByAccessGate,
                Is.EqualTo(1),
                "Exactly one requested key is a genuine absence: user/anna.");
        });
    }

    [Test]
    public async Task GetManyAsync_is_unchanged_under_a_gate_by_the_accounting_seam()
    {
        // GetManyAsync now delegates to the same gated implementation, so this pins
        // that the refactor left its observable behaviour alone: existing callers
        // see exactly the pruned dictionary they saw before.
        const string treeId = "agf-gateacct-unchanged";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice", "user/amy", "user/bob");
        FilterUserAToTree(treeId);

        var result = await tree.GetManyAsync(
            new List<string> { "user/alice", "user/amy", "user/bob" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Keys, Is.EquivalentTo(new[] { "user/alice", "user/amy" }));
            Assert.That(Encoding.UTF8.GetString(result["user/amy"]), Is.EqualTo("user/amy"));
        });
    }
}
