using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NSubstitute;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Shell.Areas.Schema;
using Orleans.Lattice.Explorer.Shell.Transport;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Schema;

/// <summary>
/// The area's reads: the tree catalogue (logical trees only), the directory
/// (per-tree policy and version reads, app declarations, bounds and caching),
/// per-tree grants, and the long-running operation tracker.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class SchemaReadsTests : SchemaTestContext
{
    [Test]
    public void The_catalogue_keeps_logical_trees_and_drops_every_physical_shadow()
    {
        var trees = SchemaTreeCatalog.Project(
        [
            SchemaTestData.Entry("orders") with { IsAlias = true, PhysicalTreeId = "orders-resized-1" },
            SchemaTestData.Entry("orders-resized-1"),
            SchemaTestData.Entry("orders-restore-9") with { RestoreShadowOfTreeId = "orders" },
            SchemaTestData.Entry("audit"),
            SchemaTestData.Entry("audit"),
        ]);

        Assert.That(trees, Is.EqualTo(new[] { "audit", "orders" }));
    }

    [Test]
    public async Task The_catalogue_follows_pages_and_is_remembered_until_it_goes_stale()
    {
        Explorer.Connection
            .ListTreesAsync(Arg.Any<CatalogRequest>(), Arg.Any<CancellationToken>())
            .Returns(
                _ => Task.FromResult(new TreeCatalogPage { Entries = [SchemaTestData.Entry("b")], NextPageToken = "p2" }),
                _ => Task.FromResult(new TreeCatalogPage { Entries = [SchemaTestData.Entry("a")] }),
                _ => Task.FromResult(new TreeCatalogPage { Entries = [SchemaTestData.Entry("c")] }));
        var catalog = Services.GetRequiredService<SchemaTreeCatalog>();

        var first = await catalog.GetAsync(refresh: false, CancellationToken.None);
        var again = await catalog.GetAsync(refresh: false, CancellationToken.None);
        Time.Advance(SchemaTreeCatalog.Freshness);
        var stale = await catalog.GetAsync(refresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(new[] { "a", "b" }));
            Assert.That(again, Is.SameAs(first));
            Assert.That(stale, Is.EqualTo(new[] { "c" }));
            Assert.That(catalog.Truncated, Is.False);
        });
    }

    [Test]
    public async Task The_catalogue_stops_at_its_page_bound_and_says_so()
    {
        Explorer.Connection
            .ListTreesAsync(Arg.Any<CatalogRequest>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(new TreeCatalogPage
            {
                Entries = [SchemaTestData.Entry("t" + call.Arg<CatalogRequest>().PageToken)],
                NextPageToken = "x" + call.Arg<CatalogRequest>().PageToken,
            }));
        var catalog = Services.GetRequiredService<SchemaTreeCatalog>();

        var trees = await catalog.GetAsync(refresh: true, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(trees, Has.Count.EqualTo(SchemaTreeCatalog.MaximumPages));
            Assert.That(catalog.Truncated, Is.True);
        });
    }

    [Test]
    public void The_catalogue_needs_a_connection()
    {
        Services.AddSingleton<Orleans.Lattice.Explorer.Core.Configuration.IExplorerSession>(
            new Orleans.Lattice.Explorer.Tests.Shell.Session.FakeExplorerSession(new Orleans.Lattice.Explorer.Tests.Shell.Session.FakeStateConnection()));
        var catalog = Services.GetRequiredService<SchemaTreeCatalog>();

        Assert.That(
            async () => await catalog.GetAsync(refresh: false, CancellationToken.None),
            Throws.InvalidOperationException.With.Message.EqualTo(SchemaTreeCatalog.NotConnected));
    }

    [Test]
    public async Task The_directory_reads_each_trees_policy_version_and_app_declaration()
    {
        UseEstate();

        var read = await Directory.GetAsync(refresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(read.Rows.Select(row => row.TreeId), Is.EqualTo(new[] { "a/crm/orders", "audit", "orders", "scratch" }));
            Assert.That(read.Governed.Select(row => row.TreeId), Is.EqualTo(new[] { "a/crm/orders", "audit", "orders" }));
            Assert.That(read.TreeCount, Is.EqualTo(4));
            Assert.That(read.Truncated, Is.False);
            Assert.That(read.Find("orders")!.Policy!.Rules, Has.Count.EqualTo(3));
            Assert.That(read.Find("orders")!.Version!.Value.TargetVersion, Is.EqualTo(3u));
            Assert.That(read.Find("scratch")!.IsGoverned, Is.False);
            Assert.That(read.Find("missing"), Is.Null);
            var declaration = read.Find("a/crm/orders")!.Declaration!;
            Assert.That(declaration, Is.EqualTo(new SchemaAppDeclaration("crm", "2.1.0", "a/crm/orders", "orders-family", 2, StrictIngest: true)));
        });
    }

    [Test]
    public async Task A_read_the_caller_may_not_make_or_the_cluster_cannot_serve_is_marked_not_thrown()
    {
        UseTrees("orders");
        Schema.Faults["GetPolicy"] = new LatticeAuthorizationDeniedException("denied");
        Schema.VersioningRegistered = false;

        var row = (await Directory.GetAsync(refresh: false, CancellationToken.None)).Find("orders")!;

        Assert.Multiple(() =>
        {
            Assert.That(row.PolicyState, Is.EqualTo(SchemaReadState.Denied));
            Assert.That(row.VersionState, Is.EqualTo(SchemaReadState.Unavailable));
            Assert.That(row.IsGoverned, Is.False);
        });
    }

    [Test]
    public async Task Any_other_fault_on_a_tree_marks_it_failed()
    {
        UseTrees("orders");
        Schema.Faults["GetPolicy"] = new ShellTransportException("boom", isTransient: false, new InvalidOperationException());

        var row = (await Directory.GetAsync(refresh: false, CancellationToken.None)).Find("orders")!;

        Assert.That(row.PolicyState, Is.EqualTo(SchemaReadState.Failed));
    }

    [Test]
    public async Task The_directory_asks_about_a_bounded_number_of_trees_and_says_so()
    {
        UseTrees([.. Enumerable.Range(0, SchemaDirectory.MaximumInspected + 1).Select(index => $"t{index:0000}")]);

        var read = await Directory.GetAsync(refresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(read.Rows, Has.Count.EqualTo(SchemaDirectory.MaximumInspected));
            Assert.That(read.TreeCount, Is.EqualTo(SchemaDirectory.MaximumInspected + 1));
            Assert.That(read.Truncated, Is.True);
            Assert.That(Schema.CountOf("GetPolicy"), Is.EqualTo(SchemaDirectory.MaximumInspected));
        });
    }

    [Test]
    public async Task The_directory_is_remembered_until_it_goes_stale_or_is_invalidated()
    {
        UseEstate();

        var first = await Directory.GetAsync(refresh: false, CancellationToken.None);
        Assert.That(await Directory.GetAsync(refresh: false, CancellationToken.None), Is.SameAs(first));
        Assert.That(Directory.Last, Is.SameAs(first));

        Time.Advance(SchemaDirectory.Freshness);
        Assert.That(await Directory.GetAsync(refresh: false, CancellationToken.None), Is.Not.SameAs(first));

        var second = Directory.Last;
        Directory.Invalidate();
        Assert.Multiple(async () =>
        {
            Assert.That(Directory.Last, Is.Null);
            Assert.That(await Directory.GetAsync(refresh: false, CancellationToken.None), Is.Not.SameAs(second));
        });
    }

    [Test]
    public async Task A_fresh_read_of_one_tree_updates_the_remembered_listing()
    {
        UseEstate();
        await Directory.GetAsync(refresh: false, CancellationToken.None);
        Schema.Policies["scratch"] = SchemaTestData.Policy();

        var row = await Directory.ReadTreeAsync("scratch", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(row.IsGoverned, Is.True);
            Assert.That(Directory.Last!.Find("scratch")!.IsGoverned, Is.True);
        });
    }

    [Test]
    public async Task A_tree_created_since_the_last_listing_is_found_by_reading_the_catalogue_again()
    {
        UseTrees("orders");
        Assert.That(await Directory.ExistsAsync("orders", CancellationToken.None), Is.True);

        UseTrees("orders", "fresh");

        Assert.Multiple(async () =>
        {
            Assert.That(await Directory.ExistsAsync("fresh", CancellationToken.None), Is.True);
            Assert.That(await Directory.ExistsAsync("never", CancellationToken.None), Is.False);
        });
    }

    [Test]
    public async Task A_caller_who_may_not_read_the_apps_simply_sees_no_declaring_app()
    {
        UseEstate();
        Apps.ListFailure = new LatticeAuthorizationDeniedException("denied");

        var read = await Directory.GetAsync(refresh: false, CancellationToken.None);

        Assert.That(read.Find("a/crm/orders")!.Declaration, Is.Null);
    }

    [Test]
    public void The_directory_needs_the_schema_facade()
    {
        Services.RemoveAll<ILatticeSchemaControl>();

        Assert.That(async () => await Directory.GetAsync(refresh: false, CancellationToken.None), Throws.TypeOf<NotSupportedException>());
    }

    [Test]
    public async Task A_trees_grants_come_from_its_own_probe_are_remembered_and_fail_closed()
    {
        Schema.Capabilities["orders"] = FakeSchemaControl.ReadOnly;
        var access = Services.GetRequiredService<SchemaAccess>();

        var grants = await access.GetGrantsAsync("orders", refresh: false, CancellationToken.None);
        await access.GetGrantsAsync("orders", refresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(grants.ViewPolicy && grants.ScanCompliance && grants.ViewDeadLetters, Is.True);
            Assert.That(grants.CanChangeAnything, Is.False);
            Assert.That(Schema.CountOf("ProbeCapabilities"), Is.EqualTo(1));
        });

        Schema.Faults["ProbeCapabilities"] = new LatticeAuthorizationDeniedException("denied");
        Assert.That(await access.GetGrantsAsync("orders", refresh: true, CancellationToken.None), Is.EqualTo(SchemaGrants.None));
    }

    [Test]
    public void The_grants_of_no_answer_are_none()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SchemaGrants.From(null), Is.SameAs(SchemaGrants.None));
            Assert.That(SchemaGrants.None.HasAny, Is.False);
            Assert.That(SchemaGrants.From(FakeSchemaControl.All("t")).CanChangeAnything, Is.True);
        });
    }

    [Test]
    public async Task An_operation_runs_in_the_background_and_keeps_its_outcome()
    {
        var gate = new TaskCompletionSource<LatticeSchemaRemediationReport>();
        var changes = new List<string>();
        Operations.Changed += changes.Add;

        var started = Operations.Start("orders", SchemaOperationKind.Migrate, "Migrating", _ => gate.Task);

        Assert.Multiple(() =>
        {
            Assert.That(started.Stage, Is.EqualTo(SchemaOperationStage.Starting));
            Assert.That(Operations.Find("orders")!.Stage, Is.EqualTo(SchemaOperationStage.Running));
            Assert.That(Operations.Find("orders")!.IsActive, Is.True);
            Assert.That(() => Operations.Start("orders", SchemaOperationKind.Migrate, "Again", _ => gate.Task), Throws.InvalidOperationException);
        });

        Operations.Dismiss("orders");
        Assert.That(Operations.Find("orders"), Is.Not.Null, "a running operation is kept");

        var ended = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Operations.Changed += tree =>
        {
            if (Operations.Find(tree) is { IsActive: false })
            {
                ended.TrySetResult();
            }
        };
        gate.SetResult(LatticeSchemaRemediationReport.Completed(12, "shadow", "op-7"));
        await ended.Task.WaitAsync(TimeSpan.FromSeconds(10));

        var finished = Operations.Find("orders")!;
        Assert.Multiple(() =>
        {
            Assert.That(finished.Stage, Is.EqualTo(SchemaOperationStage.Completed));
            Assert.That(finished.Report!.Value.ScannedCount, Is.EqualTo(12));
            Assert.That(finished.FinishedAt, Is.EqualTo(Time.GetUtcNow()));
            Assert.That(changes, Is.EqualTo(new[] { "orders", "orders", "orders" }));
        });

        Operations.Dismiss("orders");
        Assert.That(Operations.Find("orders"), Is.Null);
    }

    [Test]
    public async Task An_operation_that_aborts_or_is_refused_says_so()
    {
        Operations.Start("a", SchemaOperationKind.Remediate, "Remediating", _ =>
            Task.FromResult(LatticeSchemaRemediationReport.Aborted(3, "k", "bad", [1], "op")));
        Operations.Start("b", SchemaOperationKind.AdvanceAndMigrate, "Advancing", _ =>
            Task.FromException<LatticeSchemaRemediationReport>(new LatticeAuthorizationDeniedException("denied")));
        Operations.Start("c", SchemaOperationKind.Migrate, "Migrating", _ =>
            Task.FromException<LatticeSchemaRemediationReport>(new InvalidOperationException("unversioned")));
        await Task.Yield();

        Assert.Multiple(() =>
        {
            Assert.That(Operations.Find("a")!.Stage, Is.EqualTo(SchemaOperationStage.Aborted));
            Assert.That(Operations.Find("b")!.Stage, Is.EqualTo(SchemaOperationStage.Failed));
            Assert.That(Operations.Find("b")!.Failure, Is.EqualTo("You are not permitted to advance and migrate this tree."));
            Assert.That(Operations.Find("c")!.Failure, Is.EqualTo("Could not migrate this tree. unversioned."));
        });
    }

    [Test]
    public void An_operation_the_circuit_abandons_is_left_to_the_cluster()
    {
        var operations = new SchemaOperations(Time);
        var gate = new TaskCompletionSource<LatticeSchemaRemediationReport>();
        operations.Start("orders", SchemaOperationKind.Migrate, "Migrating", ct => gate.Task.WaitAsync(ct));

        operations.Dispose();

        Assert.That(operations.Find("orders")!.Stage, Is.EqualTo(SchemaOperationStage.Running), "the status page resumes it from the cluster");
    }
}
