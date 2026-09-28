using System.Collections.Immutable;
using System.Text.Json;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.Shell.Framing;
using Orleans.Lattice.Explorer.Shell.Framing.Broker;
using static Orleans.Lattice.Explorer.Tests.Shell.Framing.AppFrameTestData;
using static Orleans.Lattice.Explorer.Tests.Shell.Framing.Broker.BrokerHarness;

namespace Orleans.Lattice.Explorer.Tests.Shell.Framing.Broker;

/// <summary>Relaying to <see cref="ILatticeAppBridge"/>: the target, the results, the failure mapping and revocation.</summary>
public sealed partial class AppBridgeBrokerTests
{
    [Test]
    public async Task The_target_is_the_launchs_slug_and_revision_and_the_frames_logical_tree()
    {
        var harness = await CreateAsync();

        await harness.SendAsync(1, "data.read", new { action = "get", tree = "notes", key = "k" });

        var target = harness.Bridge!.Calls.Single().Target;
        Assert.Multiple(() =>
        {
            Assert.That(target.AppSlug, Is.EqualTo(Slug));
            Assert.That(target.InstallRevision, Is.EqualTo(Revision));
            Assert.That(target.LogicalTree, Is.EqualTo("notes"));
        });
    }

    [Test]
    public async Task A_missing_key_reads_as_not_found_with_a_null_value()
    {
        var harness = await CreateAsync();

        var result = AssertOk(await harness.SendAsync(1, "data.read", OrdersGet), 1);

        Assert.Multiple(() =>
        {
            Assert.That(result.GetProperty("found").GetBoolean(), Is.False);
            Assert.That(result.GetProperty("value").ValueKind, Is.EqualTo(JsonValueKind.Null));
        });
    }

    [Test]
    public async Task A_stored_value_over_the_size_limit_is_too_large()
    {
        var harness = await CreateAsync();
        harness.Bridge!.Values["k1"] = new byte[AppFrameProtocol.MaxValueBytes + 1];

        AssertRefused(await harness.SendAsync(1, "data.read", OrdersGet), 1, "too_large");
    }

    [Test]
    public async Task A_scan_relays_prefix_page_size_and_continuation_and_returns_the_page()
    {
        var harness = await CreateAsync();
        harness.Bridge!.Page = new AppBridgePage
        {
            Entries = [new AppBridgeValue { Key = "a<b>", Value = new byte[] { 1 } }, new AppBridgeValue { Key = "c", Value = new byte[] { 2 } }],
            Continuation = "next",
        };

        var result = AssertOk(await harness.SendAsync(1, "data.read", new { action = "scan", tree = "orders", prefix = "a", pageSize = 10, continuation = "c1" }), 1);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Bridge.LastScan, Is.EqualTo(("a", 10, (string?)"c1")));
            Assert.That(result.GetProperty("entries").GetArrayLength(), Is.EqualTo(2));
            Assert.That(result.GetProperty("entries")[0].GetProperty("key").GetString(), Is.EqualTo("a<b>"));
            Assert.That(result.GetProperty("entries")[1].GetProperty("value").GetBytesFromBase64(), Is.EqualTo(new byte[] { 2 }));
            Assert.That(result.GetProperty("continuation").GetString(), Is.EqualTo("next"));
        });
    }

    [Test]
    public async Task A_scan_without_a_page_size_uses_the_default_and_a_last_page_has_a_null_continuation()
    {
        var harness = await CreateAsync();

        var result = AssertOk(await harness.SendAsync(1, "data.read", new { action = "scan", tree = "orders", prefix = string.Empty, continuation = (string?)null }), 1);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Bridge!.LastScan, Is.EqualTo((string.Empty, AppFrameProtocol.DefaultPageSize, (string?)null)));
            Assert.That(result.GetProperty("continuation").ValueKind, Is.EqualTo(JsonValueKind.Null));
        });
    }

    [Test]
    public async Task A_response_over_the_size_limit_is_too_large()
    {
        var harness = await CreateAsync();
        var value = new byte[AppFrameProtocol.MaxValueBytes];
        harness.Bridge!.Page = new AppBridgePage
        {
            Entries = Enumerable.Range(0, 20).Select(i => new AppBridgeValue { Key = "k" + i, Value = value }).ToImmutableArray(),
        };

        AssertRefused(await harness.SendAsync(1, "data.read", new { action = "scan", tree = "orders", prefix = string.Empty }), 1, "too_large");
    }

    [Test]
    public async Task A_write_relays_the_decoded_bytes()
    {
        var harness = await CreateAsync();

        var result = AssertOk(await harness.SendAsync(1, "data.write", new { action = "set", tree = "orders", key = "k", value = "AQID" }), 1);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Bridge!.LastWrite, Is.EqualTo(new byte[] { 1, 2, 3 }));
            Assert.That(result.EnumerateObject(), Is.Empty);
        });
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task A_delete_reports_whether_a_value_was_removed(bool deleted)
    {
        var harness = await CreateAsync();
        harness.Bridge!.DeleteResult = deleted;

        var result = AssertOk(await harness.SendAsync(1, "data.delete", new { action = "delete", tree = "orders", key = "k" }), 1);

        Assert.That(result.GetProperty("deleted").GetBoolean(), Is.EqualTo(deleted));
    }

    [TestCase(AppBridgeFailure.Invalid, "invalid")]
    [TestCase(AppBridgeFailure.TooLarge, "too_large")]
    [TestCase(AppBridgeFailure.Conflict, "conflict")]
    [TestCase(AppBridgeFailure.Unavailable, "unavailable")]
    public async Task A_cluster_failure_maps_to_the_closed_code_set_with_a_sanitised_message(AppBridgeFailure failure, string code)
    {
        var harness = await CreateAsync();
        harness.Bridge!.Throw = new AppBridgeException(failure, "internal detail t/default/a/taskboard/orders");

        var outcome = await harness.SendAsync(1, "data.read", OrdersGet);

        AssertRefused(outcome, 1, code);
        Assert.That(outcome.Reply, Does.Not.Contain("internal detail").And.Not.Contain("t/default"));
    }

    [TestCase(AppBridgeFailure.NotFound, "not_found")]
    [TestCase(AppBridgeFailure.Denied, "denied")]
    public async Task A_refusal_while_the_launch_is_still_current_is_answered_and_not_revoked(AppBridgeFailure failure, string code)
    {
        var harness = await CreateAsync();
        harness.Bridge!.Throw = new AppBridgeException(failure);

        var outcome = await harness.SendAsync(1, "data.read", OrdersGet);

        AssertRefused(outcome, 1, code);
        Assert.That(harness.Session.IsClosed, Is.False);
    }

    [Test]
    public async Task A_revision_mismatch_revokes_the_session()
    {
        var harness = await CreateAsync();
        harness.Bridge!.Throw = new AppBridgeException(AppBridgeFailure.NotFound);
        harness.Workspace.Descriptions[Slug] = Describe(revision: Revision + 1);

        var outcome = await harness.SendAsync(1, "data.read", OrdersGet);
        var after = await harness.SendAsync(2, "context.read");

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Effect, Is.EqualTo(AppBridgeEffect.Revoked));
            Assert.That(outcome.Argument, Is.EqualTo("revision"));
            Assert.That(harness.Session.IsClosed, Is.True);
            Assert.That(after, Is.EqualTo(AppBridgeOutcome.Dropped));
            Assert.That(harness.Log.Messages, Has.Some.Contains("Revoked"));
        });
    }

    [Test]
    public async Task A_disabled_app_revokes_the_session_on_a_denial()
    {
        var harness = await CreateAsync();
        harness.Bridge!.Throw = new AppBridgeException(AppBridgeFailure.Denied);
        harness.Workspace.Descriptions[Slug] = Describe() with { State = AppLifecycleState.Disabled };

        var outcome = await harness.SendAsync(1, "data.write", new { action = "set", tree = "orders", key = "k", value = "AA==" });

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Effect, Is.EqualTo(AppBridgeEffect.Revoked));
            Assert.That(outcome.Argument, Is.EqualTo("disabled"));
        });
    }

    [Test]
    public async Task An_unexpected_exception_is_unavailable_and_discloses_nothing()
    {
        var harness = await CreateAsync();
        harness.Bridge!.Throw = new InvalidOperationException("connection string password=hunter2");

        var outcome = await harness.SendAsync(1, "data.read", OrdersGet);

        AssertRefused(outcome, 1, "unavailable");
        Assert.Multiple(() =>
        {
            Assert.That(outcome.Reply, Does.Not.Contain("hunter2"));
            Assert.That(harness.Log.Messages, Has.None.Contains("hunter2"));
        });
    }

    [Test]
    public async Task Cancellation_propagates_from_the_relay()
    {
        var harness = await CreateAsync();
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        harness.Bridge!.Throw = new OperationCanceledException(cancelled.Token);

        Assert.That(
            async () => await harness.Broker.HandleAsync(harness.Session, "{\"id\":1,\"op\":\"data.read\",\"args\":{\"action\":\"get\",\"tree\":\"orders\",\"key\":\"k\"}}", cancelled.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }
}
