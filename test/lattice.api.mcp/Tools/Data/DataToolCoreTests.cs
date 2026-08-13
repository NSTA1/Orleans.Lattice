using Orleans.Lattice.Api.Data;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="DataToolCore"/>, the pure adapter mapping behind the
/// data tools. Proves each verb round-trips through the <see cref="ILatticeDataApi"/>
/// facade with a deterministic <see cref="FakeDataApi"/> - reads, writes, deletes,
/// single-tree and cross-tree atomic batches - and inherits the facade's
/// fail-closed contract: a denied read reports absent, a denied write throws with
/// nothing persisted. No cluster, no MCP envelope, no timing.
/// </summary>
[TestFixture]
public sealed class DataToolCoreTests
{
    private const string Tree = "tree-a";

    private static byte[] Bytes(string s) => System.Text.Encoding.UTF8.GetBytes(s);

    [Test]
    public async Task SetAsync_then_GetAsync_round_trips_the_value()
    {
        var api = new FakeDataApi();

        var setResult = await DataToolCore.SetAsync(api, Tree, "k1", Bytes("v1"), CancellationToken.None);
        var getResult = await DataToolCore.GetAsync(api, Tree, "k1", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(setResult.TreeId, Is.EqualTo(Tree));
            Assert.That(setResult.Key, Is.EqualTo("k1"));
            Assert.That(setResult.Committed, Is.True);
            Assert.That(getResult.Found, Is.True);
            Assert.That(getResult.Value, Is.EqualTo(Bytes("v1")));
        });
    }

    [Test]
    public async Task GetAsync_reports_absent_for_a_missing_key()
    {
        var api = new FakeDataApi();

        var result = await DataToolCore.GetAsync(api, Tree, "missing", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Found, Is.False);
            Assert.That(result.Value, Is.Empty);
        });
    }

    [Test]
    public async Task DeleteAsync_removes_a_live_value_and_reports_it()
    {
        var api = new FakeDataApi();
        await DataToolCore.SetAsync(api, Tree, "k1", Bytes("v1"), CancellationToken.None);

        var deleted = await DataToolCore.DeleteAsync(api, Tree, "k1", CancellationToken.None);
        var afterGet = await DataToolCore.GetAsync(api, Tree, "k1", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(deleted.Deleted, Is.True);
            Assert.That(deleted.TreeId, Is.EqualTo(Tree));
            Assert.That(deleted.Key, Is.EqualTo("k1"));
            Assert.That(afterGet.Found, Is.False);
        });
    }

    [Test]
    public async Task DeleteAsync_reports_false_when_no_live_value_existed()
    {
        var api = new FakeDataApi();

        var deleted = await DataToolCore.DeleteAsync(api, Tree, "absent", CancellationToken.None);

        Assert.That(deleted.Deleted, Is.False);
    }

    [Test]
    public async Task DeleteRangeAsync_drains_the_range_and_reports_the_count()
    {
        var api = new FakeDataApi();
        await DataToolCore.SetAsync(api, Tree, "k1", Bytes("1"), CancellationToken.None);
        await DataToolCore.SetAsync(api, Tree, "k2", Bytes("2"), CancellationToken.None);
        await DataToolCore.SetAsync(api, Tree, "k3", Bytes("3"), CancellationToken.None);
        await DataToolCore.SetAsync(api, Tree, "zzz", Bytes("9"), CancellationToken.None);

        var result = await DataToolCore.DeleteRangeAsync(
            api, Tree, startInclusive: "k1", endExclusive: "k9", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.TreeId, Is.EqualTo(Tree));
            Assert.That(result.DeletedCount, Is.EqualTo(3));
            Assert.That(api.Contains(Tree, "k1"), Is.False);
            Assert.That(api.Contains(Tree, "zzz"), Is.True);
        });
    }

    [Test]
    public void DeleteRangeAsync_denies_the_whole_range_when_a_key_is_denied()
    {
        var api = new FakeDataApi();
        api.SetAsync(Tree, "k1", Bytes("1"), CancellationToken.None).GetAwaiter().GetResult();
        api.SetAsync(Tree, "k2", Bytes("2"), CancellationToken.None).GetAwaiter().GetResult();
        api.Denied.Add((Tree, "k2"));

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            () => DataToolCore.DeleteRangeAsync(api, Tree, "k1", "k9", CancellationToken.None));

        // All-or-nothing: nothing was removed.
        Assert.That(api.Contains(Tree, "k1"), Is.True);
    }

    [Test]
    public async Task ReadRangeAsync_returns_the_authorized_ascending_page()
    {
        var api = new FakeDataApi();
        await DataToolCore.SetAsync(api, Tree, "b", Bytes("2"), CancellationToken.None);
        await DataToolCore.SetAsync(api, Tree, "a", Bytes("1"), CancellationToken.None);
        await DataToolCore.SetAsync(api, Tree, "c", Bytes("3"), CancellationToken.None);

        var page = await DataToolCore.ReadRangeAsync(
            api, Tree, startInclusive: "a", endExclusive: "c", pageSize: 10, continuationToken: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.TreeId, Is.EqualTo(Tree));
            Assert.That(page.Entries.Select(e => e.Key), Is.EqualTo(new[] { "a", "b" }));
            Assert.That(page.Entries[0].Value, Is.EqualTo(Bytes("1")));
        });
    }

    [Test]
    public async Task SetManyAtomicAsync_commits_upserts_and_deletes_together()
    {
        var api = new FakeDataApi();
        await DataToolCore.SetAsync(api, Tree, "old", Bytes("x"), CancellationToken.None);

        var upserts = new[]
        {
            new DataEntryDto { Key = "k1", Value = Bytes("v1") },
            new DataEntryDto { Key = "k2", Value = Bytes("v2") },
        };

        var result = await DataToolCore.SetManyAtomicAsync(
            api, Tree, upserts, new[] { "old" }, "op-1", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.OperationId, Is.EqualTo("op-1"));
            Assert.That(result.Committed, Is.True);
            Assert.That(api.Contains(Tree, "k1"), Is.True);
            Assert.That(api.Contains(Tree, "k2"), Is.True);
            Assert.That(api.Contains(Tree, "old"), Is.False);
        });
    }

    [Test]
    public async Task SetManyAtomicAsync_accepts_null_collections_as_empty()
    {
        var api = new FakeDataApi();

        var result = await DataToolCore.SetManyAtomicAsync(
            api, Tree, upserts: null, deleteKeys: null, "op-empty", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Committed, Is.True);
            Assert.That(api.Count, Is.EqualTo(0));
        });
    }

    [Test]
    public async Task SetManyAtomicCrossTreeAsync_commits_across_trees_when_authorized()
    {
        var api = new FakeDataApi();
        var batches = new[]
        {
            new DataTreeBatchDto { TreeId = "t1", Upserts = new[] { new DataEntryDto { Key = "k", Value = Bytes("1") } } },
            new DataTreeBatchDto { TreeId = "t2", Upserts = new[] { new DataEntryDto { Key = "k", Value = Bytes("2") } } },
        };

        var result = await DataToolCore.SetManyAtomicCrossTreeAsync(api, batches, "op-xt", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Outcome, Is.EqualTo("Committed"));
            Assert.That(result.Committed, Is.True);
            Assert.That(result.OperationId, Is.EqualTo("op-xt"));
            Assert.That(api.Contains("t1", "k"), Is.True);
            Assert.That(api.Contains("t2", "k"), Is.True);
        });
    }

    [Test]
    public async Task SetManyAtomicCrossTreeAsync_maps_a_precondition_miss_to_a_non_committed_value()
    {
        var api = new FakeDataApi { CrossTreeOutcome = CrossTreeAtomicWriteOutcome.PreconditionFailed };
        var batches = new[]
        {
            new DataTreeBatchDto { TreeId = "t1", Upserts = new[] { new DataEntryDto { Key = "k", Value = Bytes("1") } } },
        };

        var result = await DataToolCore.SetManyAtomicCrossTreeAsync(api, batches, "op-xt", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Outcome, Is.EqualTo("PreconditionFailed"));
            Assert.That(result.Committed, Is.False);
            Assert.That(api.Contains("t1", "k"), Is.False, "A precondition miss commits nothing.");
        });
    }

    [Test]
    public void SetAsync_on_a_denied_key_throws_and_persists_nothing()
    {
        var api = new FakeDataApi();
        api.Denied.Add((Tree, "secret"));

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            () => DataToolCore.SetAsync(api, Tree, "secret", Bytes("v"), CancellationToken.None));
        Assert.That(api.Contains(Tree, "secret"), Is.False);
    }

    [Test]
    public async Task GetAsync_on_a_denied_key_reports_absent_rather_than_throwing()
    {
        var api = new FakeDataApi();
        await DataToolCore.SetAsync(api, Tree, "secret", Bytes("v"), CancellationToken.None);
        api.Denied.Add((Tree, "secret"));

        var result = await DataToolCore.GetAsync(api, Tree, "secret", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Found, Is.False);
            Assert.That(result.Value, Is.Empty);
        });
    }

    [Test]
    public void SetManyAtomicAsync_with_one_denied_leg_aborts_the_whole_batch()
    {
        var api = new FakeDataApi();
        api.Denied.Add((Tree, "secret"));
        var upserts = new[]
        {
            new DataEntryDto { Key = "ok", Value = Bytes("1") },
            new DataEntryDto { Key = "secret", Value = Bytes("2") },
        };

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            () => DataToolCore.SetManyAtomicAsync(api, Tree, upserts, deleteKeys: null, "op-1", CancellationToken.None));
        Assert.Multiple(() =>
        {
            Assert.That(api.Contains(Tree, "ok"), Is.False, "A denied leg must abort the batch with nothing persisted.");
            Assert.That(api.Contains(Tree, "secret"), Is.False);
        });
    }

    [Test]
    public void Methods_reject_a_null_facade()
    {
        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<ArgumentNullException>(
                () => DataToolCore.GetAsync(null!, Tree, "k", CancellationToken.None));
            Assert.ThrowsAsync<ArgumentNullException>(
                () => DataToolCore.SetAsync(null!, Tree, "k", Bytes("v"), CancellationToken.None));
            Assert.ThrowsAsync<ArgumentNullException>(
                () => DataToolCore.DeleteAsync(null!, Tree, "k", CancellationToken.None));
            Assert.ThrowsAsync<ArgumentNullException>(
                () => DataToolCore.ReadRangeAsync(null!, Tree, null, null, 10, null, CancellationToken.None));
            Assert.ThrowsAsync<ArgumentNullException>(
                () => DataToolCore.SetManyAtomicAsync(null!, Tree, null, null, "op", CancellationToken.None));
            Assert.ThrowsAsync<ArgumentNullException>(
                () => DataToolCore.SetManyAtomicCrossTreeAsync(null!, Array.Empty<DataTreeBatchDto>(), "op", CancellationToken.None));
        });
    }

    [Test]
    public async Task SetManyAsync_writes_every_upsert_without_atomicity()
    {
        var api = new FakeDataApi();
        var upserts = new[]
        {
            new DataEntryDto { Key = "k1", Value = Bytes("v1") },
            new DataEntryDto { Key = "k2", Value = Bytes("v2") },
        };

        var result = await DataToolCore.SetManyAsync(api, Tree, upserts, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.TreeId, Is.EqualTo(Tree));
            Assert.That(result.Count, Is.EqualTo(2));
            Assert.That(api.Contains(Tree, "k1"), Is.True);
            Assert.That(api.Contains(Tree, "k2"), Is.True);
        });
    }

    [Test]
    public async Task SetManyAsync_accepts_a_null_upsert_list_as_empty()
    {
        var api = new FakeDataApi();

        var result = await DataToolCore.SetManyAsync(api, Tree, upserts: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Count, Is.EqualTo(0));
            Assert.That(api.Count, Is.EqualTo(0));
        });
    }

    [Test]
    public void SetManyAtomicCrossTreeAsync_rejects_a_null_batch_list()
    {
        var api = new FakeDataApi();

        Assert.ThrowsAsync<ArgumentNullException>(
            () => DataToolCore.SetManyAtomicCrossTreeAsync(api, null!, "op", CancellationToken.None));
    }
}
