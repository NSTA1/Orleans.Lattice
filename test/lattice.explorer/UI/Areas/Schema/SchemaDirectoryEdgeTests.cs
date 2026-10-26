using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Areas.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>
/// The directory's refusals and skips that a rendered page cannot tell apart from
/// a healthy answer: a single-tree read overtaken by a change of caller, the
/// caller's own cancellation let out of the policy, version and declaration reads
/// rather than classified as a failed tree, and the declaring-app pass skipping an
/// app with no description or a declaration naming no tree.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class SchemaDirectoryEdgeTests : SchemaTestContext
{
    [Test]
    public async Task A_single_tree_read_overtaken_by_a_change_of_caller_is_cancelled()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Schema.CapabilityGate = gate;

        var pending = Directory.ReadTreeAsync("orders", CancellationToken.None);
        await Orleans.Lattice.Testing.TestPoll.UntilAsync(
            () => Schema.CountOf("ProbeCapabilities") == 1,
            "the grants probe is in flight");
        Auth.SignIn("reader");
        gate.SetResult();

        Assert.That(async () => await pending, Throws.InstanceOf<OperationCanceledException>());
        var fresh = await Directory.ReadTreeAsync("orders", CancellationToken.None);
        Assert.That(fresh.TreeId, Is.EqualTo("orders"), "the new caller's own read is answered");
    }

    [Test]
    public async Task The_callers_cancellation_during_the_policy_read_is_let_out()
    {
        Schema.Policies["orders"] = SchemaTestData.Policy();
        await Directory.ReadTreeAsync("orders", CancellationToken.None);
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        Schema.Faults["GetPolicy"] = new OperationCanceledException(cancelled.Token);

        Assert.That(
            async () => await Directory.ReadTreeAsync("orders", cancelled.Token),
            Throws.InstanceOf<OperationCanceledException>(),
            "an abandoned read is not a tree whose policy failed");
    }

    [Test]
    public async Task The_callers_cancellation_during_the_version_read_is_let_out()
    {
        Schema.Versions["orders"] = new(2, 1);
        await Directory.ReadTreeAsync("orders", CancellationToken.None);
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        Schema.Faults["GetVersionConfig"] = new OperationCanceledException(cancelled.Token);

        Assert.That(
            async () => await Directory.ReadTreeAsync("orders", cancelled.Token),
            Throws.InstanceOf<OperationCanceledException>(),
            "an abandoned read is not a tree whose version failed");
    }

    [Test]
    public async Task A_cancellation_the_caller_did_not_ask_for_classifies_the_policy_as_failed()
    {
        Schema.Faults["GetPolicy"] = new OperationCanceledException("the server gave up");

        var row = await Directory.ReadTreeAsync("orders", CancellationToken.None);

        Assert.That(row.PolicyState, Is.EqualTo(SchemaReadState.Failed));
    }

    [Test]
    public void The_callers_cancellation_during_the_declarations_read_is_let_out()
    {
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        Apps.ListFailure = new OperationCanceledException(cancelled.Token);

        Assert.That(
            async () => await Directory.ReadTreeAsync("a/crm/orders", cancelled.Token),
            Throws.InstanceOf<OperationCanceledException>(),
            "an abandoned read is not an apps catalogue the caller may not read");
    }

    [Test]
    public async Task An_app_listed_but_no_longer_described_declares_nothing()
    {
        Apps.Add("crm", "2.1.0", "orders");
        Apps.Add("billing", "1.0.0", "invoices");
        Apps.Undescribed.Add("crm");

        var crm = await Directory.ReadTreeAsync("a/crm/orders", CancellationToken.None);
        var billing = await Directory.ReadTreeAsync("a/billing/invoices", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(crm.Declaration, Is.Null);
            Assert.That(billing.Declaration?.Slug, Is.EqualTo("billing"), "the next app is still read");
        });
    }

    [Test]
    public async Task A_declaration_naming_no_tree_is_skipped_and_its_siblings_are_kept()
    {
        Apps.Apps.Add(("crm", "2.1.0",
        [
            new AppSchemaDescriptor { Tree = string.Empty, Family = "orphan", Version = 1 },
            new AppSchemaDescriptor { Tree = "orders", Family = "orders-family", Version = 2, StrictIngest = true },
        ]));

        var orders = await Directory.ReadTreeAsync("a/crm/orders", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(orders.Declaration?.Family, Is.EqualTo("orders-family"), "the empty name did not abandon the app's declarations");
            Assert.That(orders.Declaration?.Version, Is.EqualTo(2));
        });
    }
}
