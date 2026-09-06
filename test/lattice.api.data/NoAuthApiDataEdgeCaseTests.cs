namespace Orleans.Lattice.Api.Data.Tests;

/// <summary>
/// Covers the non-CRDT <see cref="ILatticeDataApi"/> paths the sibling no-auth
/// suite leaves unexercised: the bulk <c>SetManyAsync</c> upsert, a cross-tree
/// atomic batch that also retracts keys, range-read pagination via a continuation
/// token, an unknown-tree range read, a malformed continuation token, and the
/// null-entry guard on a bulk upsert. Reuses the shared
/// <see cref="NoAuthApiDataClusterFixture"/> without modifying it.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class NoAuthApiDataEdgeCaseTests
{
    private NoAuthApiDataClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new NoAuthApiDataClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }

    [Test]
    public async Task SetManyAsync_writes_every_upsert()
    {
        const string tree = "edge-setmany";
        await _fixture.RegisterTreeAsync(tree);

        var upserts = new List<DataEntry>
        {
            new() { Key = "a", Value = new byte[] { 1 } },
            new() { Key = "b", Value = new byte[] { 2 } },
            new() { Key = "c", Value = new byte[] { 3 } },
        };

        await _fixture.Api.SetManyAsync(tree, upserts);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That((await _fixture.Api.GetAsync(tree, "a")).Value, Is.EqualTo(new byte[] { 1 }));
            Assert.That((await _fixture.Api.GetAsync(tree, "b")).Value, Is.EqualTo(new byte[] { 2 }));
            Assert.That((await _fixture.Api.GetAsync(tree, "c")).Value, Is.EqualTo(new byte[] { 3 }));
        });
    }

    [Test]
    public async Task SetManyAsync_with_empty_upserts_is_a_no_op()
    {
        const string tree = "edge-setmany-empty";
        await _fixture.RegisterTreeAsync(tree);

        await _fixture.Api.SetManyAsync(tree, Array.Empty<DataEntry>());

        var page = await _fixture.Api.ReadRangeAsync(new DataRangeRequest { TreeId = tree, PageSize = 10 });
        Assert.That(page.Entries, Is.Empty);
    }

    [Test]
    public async Task SetManyAsync_with_null_entry_throws_ArgumentNullException()
    {
        const string tree = "edge-setmany-null";
        await _fixture.RegisterTreeAsync(tree);

        var upserts = new List<DataEntry> { null! };

        Assert.That(
            async () => await _fixture.Api.SetManyAsync(tree, upserts),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task SetManyAtomicCrossTreeAsync_with_delete_keys_retracts_them()
    {
        const string treeA = "edge-xt-del-a";
        const string treeB = "edge-xt-del-b";
        await _fixture.RegisterTreeAsync(treeA);
        await _fixture.RegisterTreeAsync(treeB);

        // Seed the key the cross-tree batch will retract.
        await _fixture.Api.SetAsync(treeB, "gone", new byte[] { 7 });

        var batches = new List<DataTreeBatch>
        {
            new() { TreeId = treeA, Upserts = [new DataEntry { Key = "kept", Value = new byte[] { 1 } }] },
            new()
            {
                TreeId = treeB,
                Upserts = [new DataEntry { Key = "fresh", Value = new byte[] { 2 } }],
                DeleteKeys = ["gone"],
            },
        };

        var outcome = await _fixture.Api.SetManyAtomicCrossTreeAsync(batches, "edge-xt-del-op");

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
            Assert.That((await _fixture.Api.GetAsync(treeA, "kept")).Value, Is.EqualTo(new byte[] { 1 }));
            Assert.That((await _fixture.Api.GetAsync(treeB, "fresh")).Value, Is.EqualTo(new byte[] { 2 }));
            Assert.That((await _fixture.Api.GetAsync(treeB, "gone")).Found, Is.False);
        });
    }

    [Test]
    public async Task ReadRangeAsync_on_unknown_tree_returns_empty_drained_page()
    {
        const string tree = "edge-range-unknown";

        var page = await _fixture.Api.ReadRangeAsync(new DataRangeRequest { TreeId = tree, PageSize = 10 });

        Assert.Multiple(() =>
        {
            Assert.That(page.TreeId, Is.EqualTo(tree));
            Assert.That(page.Entries, Is.Empty);
            Assert.That(page.ContinuationToken, Is.Null);
        });
    }

    [Test]
    public async Task ReadRangeAsync_paginates_across_pages_with_continuation_token()
    {
        const string tree = "edge-range-page";
        await _fixture.RegisterTreeAsync(tree);

        await _fixture.Api.SetAsync(tree, "a", new byte[] { 1 });
        await _fixture.Api.SetAsync(tree, "b", new byte[] { 2 });
        await _fixture.Api.SetAsync(tree, "c", new byte[] { 3 });

        var first = await _fixture.Api.ReadRangeAsync(new DataRangeRequest { TreeId = tree, PageSize = 2 });

        Assert.That(first.ContinuationToken, Is.Not.Null.And.Not.Empty);
        Assert.That(first.Entries, Has.Count.EqualTo(2));

        var second = await _fixture.Api.ReadRangeAsync(new DataRangeRequest
        {
            TreeId = tree,
            ContinuationToken = first.ContinuationToken,
        });

        var allKeys = first.Entries.Select(e => e.Key).Concat(second.Entries.Select(e => e.Key));
        Assert.Multiple(() =>
        {
            Assert.That(second.ContinuationToken, Is.Null, "the second page drains the range");
            Assert.That(allKeys, Is.EquivalentTo(new[] { "a", "b", "c" }));
        });
    }

    [Test]
    public async Task ReadRangeAsync_with_invalid_continuation_token_throws_ArgumentException()
    {
        const string tree = "edge-range-badtoken";
        await _fixture.RegisterTreeAsync(tree);

        Assert.That(
            async () => await _fixture.Api.ReadRangeAsync(new DataRangeRequest
            {
                TreeId = tree,
                ContinuationToken = "not-a-real-cursor-id",
            }),
            Throws.ArgumentException);
    }
}
