using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Coverage for the per-key merge-mode decode path (issue #1402 item 14): a typed
/// CRDT written to a <b>local, non-replicated</b> tree - one the per-tree
/// <see cref="ILatticeMergeModeResolver"/> reports nothing for - must still be
/// decoded into its logical value, because the leaf records the convergence
/// discriminator per key. The read facade reports each entry's
/// <see cref="EntryRecord.MergeMode"/> and only flags genuinely opaque or
/// undecodable payloads <see cref="EntryRecord.Raw"/>. The fixture's resolver only
/// declares trees prefixed <c>orset</c> / <c>pncounter</c>, so a tree named
/// otherwise exercises exactly the single-cluster / mixed-mode gap the item fixes.
/// </summary>
public sealed partial class LatticeStateQueryIntegrationTests
{
    [Test]
    public async Task GetEntryAsync_local_counter_decodes_via_per_key_mode_not_per_tree()
    {
        // Tree id is deliberately NOT "pncounter*", so the per-tree resolver returns
        // null - the exact case that previously read the counter back as its opaque
        // internal serialization.
        const string treeId = "local-mixed-counter";
        await _fixture.CreatePnCounterTreeAsync(treeId, "votes", increment: 5, decrement: 2);

        var result = await _fixture.Query.GetEntryAsync(treeId, "votes");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(result.Entry, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(result.Entry!.MergeMode, Is.EqualTo(LatticeMergeMode.PnCounter),
                "the per-key discriminator resolves the mode even on a local tree");
            Assert.That(result.Entry!.CrdtShape, Is.EqualTo("PnCounter"));
            Assert.That(result.Entry!.Raw, Is.False, "a decoded typed CRDT is not raw");
            Assert.That(System.Text.Encoding.UTF8.GetString(result.Entry!.CurrentMembers[0].Element),
                Is.EqualTo("3"), "the net value (5 - 2) is decoded, not the internal state");
        });
    }

    [Test]
    public async Task ScanEntriesAsync_local_counter_decodes_via_per_key_mode_not_per_tree()
    {
        const string treeId = "local-mixed-counter-scan";
        await _fixture.CreatePnCounterTreeAsync(treeId, "votes", increment: 4, decrement: 1);

        var page = await _fixture.Query.ScanEntriesAsync(new EntryScanRequest
        {
            TreeId = treeId,
            PageSize = 100,
        });

        Assert.That(page.Status, Is.EqualTo(StateQueryStatus.Found));
        var entry = page.Entries.Single(e => e.Key == "votes");
        Assert.Multiple(() =>
        {
            Assert.That(entry.MergeMode, Is.EqualTo(LatticeMergeMode.PnCounter));
            Assert.That(entry.CrdtShape, Is.EqualTo("PnCounter"));
            Assert.That(entry.Raw, Is.False);
            Assert.That(System.Text.Encoding.UTF8.GetString(entry.CurrentMembers[0].Element), Is.EqualTo("3"));
        });
    }

    [Test]
    public async Task GetEntryAsync_mixed_mode_tree_reports_each_key_independently()
    {
        // A single local tree carrying both a typed CRDT key and a plain value - the
        // literal "mixed-mode" tree the resolver cannot describe.
        const string treeId = "local-truly-mixed";
        var tree = await _fixture.CreatePnCounterTreeAsync(treeId, "counter", increment: 5, decrement: 2);
        await tree.SetAsync("plain", Encoding.UTF8.GetBytes("just-bytes"));

        var counter = await _fixture.Query.GetEntryAsync(treeId, "counter");
        var plain = await _fixture.Query.GetEntryAsync(treeId, "plain");

        Assert.Multiple(() =>
        {
            Assert.That(counter.Entry!.MergeMode, Is.EqualTo(LatticeMergeMode.PnCounter));
            Assert.That(counter.Entry!.Raw, Is.False);

            // The plain value carries no per-key mode and is flagged raw, never
            // presented as a decoded projection.
            Assert.That(plain.Entry!.MergeMode, Is.Null);
            Assert.That(plain.Entry!.CrdtShape, Is.Null);
            Assert.That(plain.Entry!.Raw, Is.True);
            Assert.That(plain.Entry!.CurrentMembers, Is.Empty);
        });
    }

    [Test]
    public async Task GetEntryAsync_lww_value_is_flagged_raw_with_no_mode()
    {
        const string treeId = "lww-raw-flag";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 3);

        var result = await _fixture.Query.GetEntryAsync(treeId, "key-00001");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.Multiple(() =>
        {
            Assert.That(result.Entry!.MergeMode, Is.Null);
            Assert.That(result.Entry!.Raw, Is.True,
                "an opaque last-writer-wins value is flagged raw so its bytes are never mistaken for a decoded value");
        });
    }

    [Test]
    public async Task GetEntryAsync_declared_orset_tree_still_decodes_and_is_not_raw()
    {
        // The per-tree resolver path (a declared replicated tree) keeps working: the
        // per-key fallback does not regress trees whose mode is declared per tree.
        const string treeId = "orset-declared-notraw";
        await _fixture.CreateOrSetTreeAsync(treeId, "focus");

        var result = await _fixture.Query.GetEntryAsync(treeId, "focus");

        Assert.Multiple(() =>
        {
            Assert.That(result.Entry!.CrdtShape, Is.EqualTo("OrSet"));
            Assert.That(result.Entry!.MergeMode, Is.EqualTo(LatticeMergeMode.OrSet));
            Assert.That(result.Entry!.Raw, Is.False);
        });
    }
}
