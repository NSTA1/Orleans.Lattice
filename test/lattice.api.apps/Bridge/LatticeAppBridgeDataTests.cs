using System.Text;

namespace Orleans.Lattice.Api.Apps.Tests.Bridge;

/// <summary>
/// The app bridge's data verbs once authorized: reads, writes, deletes and paged scans on the effective tree,
/// the size bounds, and the sanitised translation of data-path faults (step 5, where the caller's own rights
/// apply).
/// </summary>
[TestFixture]
public sealed class LatticeAppBridgeDataTests
{
    private static BridgeHarness Editor() => new BridgeHarness().Installed("alice", BridgeHarness.Editors);

    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    [Test]
    public async Task GetAsync_returns_the_stored_value_or_null()
    {
        var harness = Editor().Seed(BridgeHarness.NotesTree, "a", Bytes("one"));
        var bridge = harness.Bridge;

        var found = await bridge.GetAsync(BridgeHarness.Target(), "a");
        var missing = await bridge.GetAsync(BridgeHarness.Target(), "b");

        Assert.That(found!.Key, Is.EqualTo("a"));
        Assert.That(found.Value.ToArray(), Is.EqualTo(Bytes("one")));
        Assert.That(missing, Is.Null);
    }

    [Test]
    public void GetAsync_refuses_a_stored_value_larger_than_the_bound()
    {
        var harness = Editor().Seed(BridgeHarness.NotesTree, "big", new byte[AppBridgeLimits.MaxValueBytes + 1]);

        BridgeAssert.Fails(AppBridgeFailure.TooLarge, () => harness.Bridge.GetAsync(BridgeHarness.Target(), "big"));
    }

    [Test]
    public async Task GetAsync_returns_a_value_exactly_at_the_bound()
    {
        var harness = Editor().Seed(BridgeHarness.NotesTree, "big", new byte[AppBridgeLimits.MaxValueBytes]);

        Assert.That((await harness.Bridge.GetAsync(BridgeHarness.Target(), "big"))!.Value.Length, Is.EqualTo(AppBridgeLimits.MaxValueBytes));
    }

    [Test]
    public async Task SetAsync_writes_the_value_to_the_effective_tree()
    {
        var harness = Editor();

        await harness.Bridge.SetAsync(BridgeHarness.Target(), "k", Bytes("v"));

        Assert.That(harness.Store(BridgeHarness.NotesTree)["k"], Is.EqualTo(Bytes("v")));
        Assert.That(harness.Dialled, Is.EqualTo(new[] { BridgeHarness.NotesTree }));
    }

    [Test]
    public async Task SetAsync_copies_a_value_that_is_a_slice_of_a_larger_buffer()
    {
        var harness = Editor();
        var buffer = Bytes("xxvalueyy");

        await harness.Bridge.SetAsync(BridgeHarness.Target(), "k", buffer.AsMemory(2, 5));

        Assert.That(harness.Store(BridgeHarness.NotesTree)["k"], Is.EqualTo(Bytes("value")));
    }

    [Test]
    public async Task SetAsync_accepts_an_empty_value_and_a_value_exactly_at_the_bound()
    {
        var harness = Editor();

        await harness.Bridge.SetAsync(BridgeHarness.Target(), "empty", ReadOnlyMemory<byte>.Empty);
        await harness.Bridge.SetAsync(BridgeHarness.Target(), "full", new byte[AppBridgeLimits.MaxValueBytes]);

        Assert.That(harness.Store(BridgeHarness.NotesTree)["empty"], Is.Empty);
        Assert.That(harness.Store(BridgeHarness.NotesTree)["full"], Has.Length.EqualTo(AppBridgeLimits.MaxValueBytes));
    }

    [Test]
    public async Task DeleteAsync_reports_whether_a_live_value_was_removed()
    {
        var harness = Editor().Seed(BridgeHarness.NotesTree, "k", Bytes("v"));
        var bridge = harness.Bridge;

        Assert.That(await bridge.DeleteAsync(BridgeHarness.Target(), "k"), Is.True);
        Assert.That(await bridge.DeleteAsync(BridgeHarness.Target(), "k"), Is.False);
        Assert.That(harness.Store(BridgeHarness.NotesTree), Is.Empty);
    }

    [Test]
    public async Task ScanAsync_pages_through_a_prefix_in_key_order()
    {
        var harness = Editor()
            .Seed(BridgeHarness.NotesTree, "n/3", Bytes("3"))
            .Seed(BridgeHarness.NotesTree, "n/1", Bytes("1"))
            .Seed(BridgeHarness.NotesTree, "n/2", Bytes("2"))
            .Seed(BridgeHarness.NotesTree, "o/1", Bytes("outside"))
            .Seed(BridgeHarness.NotesTree, "m/1", Bytes("outside"));
        var bridge = harness.Bridge;

        var first = await bridge.ScanAsync(BridgeHarness.Target(), "n/", 2);
        var second = await bridge.ScanAsync(BridgeHarness.Target(), "n/", 2, first.Continuation);

        Assert.That(first.Entries.Select(e => e.Key), Is.EqualTo(new[] { "n/1", "n/2" }));
        Assert.That(first.Continuation, Is.Not.Null);
        Assert.That(second.Entries.Select(e => e.Key), Is.EqualTo(new[] { "n/3" }));
        Assert.That(second.Entries[0].Value.ToArray(), Is.EqualTo(Bytes("3")));
        Assert.That(second.Continuation, Is.Null);
    }

    [Test]
    public async Task ScanAsync_with_an_empty_prefix_scans_the_whole_tree()
    {
        var harness = Editor().Seed(BridgeHarness.NotesTree, "a", [1]).Seed(BridgeHarness.NotesTree, "z", [2]);

        var page = await harness.Bridge.ScanAsync(BridgeHarness.Target(), string.Empty, 10);

        Assert.That(page.Entries.Select(e => e.Key), Is.EqualTo(new[] { "a", "z" }));
        Assert.That(page.Continuation, Is.Null);
    }

    [Test]
    public async Task ScanAsync_of_an_empty_range_returns_an_empty_last_page()
    {
        var page = await Editor().Bridge.ScanAsync(BridgeHarness.Target(), "nothing/", 10);

        Assert.That(page.Entries, Is.Empty);
        Assert.That(page.Continuation, Is.Null);
    }

    [Test]
    public async Task ScanAsync_clamps_the_page_size_to_the_bound()
    {
        var harness = Editor();
        for (var i = 0; i < AppBridgeLimits.MaxPageSize + 5; i++)
        {
            harness.Seed(BridgeHarness.NotesTree, $"k{i:D4}", [1]);
        }

        var page = await harness.Bridge.ScanAsync(BridgeHarness.Target(), string.Empty, int.MaxValue);

        Assert.That(page.Entries, Has.Length.EqualTo(AppBridgeLimits.MaxPageSize));
        Assert.That(page.Continuation, Is.Not.Null);
    }

    [Test]
    public async Task ScanAsync_ends_a_page_early_to_stay_within_the_response_budget()
    {
        var harness = Editor();
        for (var i = 0; i < 20; i++)
        {
            harness.Seed(BridgeHarness.NotesTree, $"k{i:D2}", new byte[AppBridgeLimits.MaxValueBytes]);
        }

        var bridge = harness.Bridge;
        var first = await bridge.ScanAsync(BridgeHarness.Target(), string.Empty, 20);
        var estimate = first.Entries.Sum(e => AppBridgeLimits.EstimateEntryBytes(e.Key, e.Value.Length)) + AppBridgeLimits.PageOverheadBytes;
        var second = await bridge.ScanAsync(BridgeHarness.Target(), string.Empty, 20, first.Continuation);

        Assert.That(first.Entries.Length, Is.GreaterThan(0).And.LessThan(20));
        Assert.That(estimate, Is.LessThanOrEqualTo(AppBridgeLimits.MaxResponseBytes));
        Assert.That(first.Continuation, Is.Not.Null);
        Assert.That(first.Entries.Length + second.Entries.Length, Is.EqualTo(20));
        Assert.That(second.Continuation, Is.Null);
    }

    [Test]
    public void ScanAsync_refuses_a_page_holding_a_value_larger_than_the_bound()
    {
        var harness = Editor().Seed(BridgeHarness.NotesTree, "big", new byte[AppBridgeLimits.MaxValueBytes + 1]);

        BridgeAssert.Fails(AppBridgeFailure.TooLarge, () => harness.Bridge.ScanAsync(BridgeHarness.Target(), string.Empty, 10));
    }

    // Step 5 - the caller's own rights, and sanitised faults.

    [Test]
    public void A_data_path_authorization_denial_is_a_denial()
    {
        var harness = Editor();
        harness.DataFault = new LatticeAuthorizationDeniedException("tree 'a/crm/notes' denied for subject 'alice'");

        BridgeAssert.EveryVerbFails(harness.Bridge, BridgeHarness.Target(), AppBridgeFailure.Denied);
    }

    [Test]
    public void A_data_path_tenant_denial_is_a_denial()
    {
        var harness = Editor();
        harness.DataFault = new LatticeTenantAccessDeniedException("tenant 't/acme' refused");

        BridgeAssert.EveryVerbFails(harness.Bridge, BridgeHarness.Target(), AppBridgeFailure.Denied);
    }

    [Test]
    public void A_data_path_argument_fault_is_invalid()
    {
        var harness = Editor();
        harness.DataFault = new ArgumentException("key 'secret' rejected by tree 'a/crm/notes'");

        BridgeAssert.EveryVerbFails(harness.Bridge, BridgeHarness.Target(), AppBridgeFailure.Invalid);
    }

    [Test]
    public void An_unexpected_data_path_fault_is_unavailable_and_discloses_nothing()
    {
        var harness = Editor();
        harness.DataFault = new InvalidOperationException("shard of physical tree 'a/crm/notes' for 'alice' failed");

        BridgeAssert.EveryVerbFails(harness.Bridge, BridgeHarness.Target(), AppBridgeFailure.Unavailable);
    }

    [Test]
    public void A_cancellation_by_the_caller_propagates_as_cancellation()
    {
        var harness = Editor();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        harness.DataFault = new OperationCanceledException(cancellation.Token);

        Assert.That(() => harness.Bridge.GetAsync(BridgeHarness.Target(), "k", cancellation.Token), Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void A_cancellation_the_caller_did_not_request_is_unavailable()
    {
        var harness = Editor();
        harness.DataFault = new OperationCanceledException();

        BridgeAssert.Fails(AppBridgeFailure.Unavailable, () => harness.Bridge.GetAsync(BridgeHarness.Target(), "k"));
    }
}
