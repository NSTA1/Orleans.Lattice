using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc.Tests;

/// <summary>
/// Unit coverage for <see cref="LatticeTreeAdminApiGrpcClient"/> driven over a
/// <see cref="CallInvoker"/> test double - no live server. Pins the client-side
/// argument guards and the request shaping for the operations the end-to-end
/// fixture does not drive (set restore and revert, WAL move planning, the
/// materialised-view rebuild / reconcile / drop trio, and the history-retention
/// window guard), so a regression in request construction is caught without
/// standing up a cluster.
/// </summary>
[TestFixture]
public sealed class LatticeTreeAdminApiGrpcClientUnitTests
{
    private ServiceProvider _services = null!;
    private LatticeTreeAdminGrpcMethods _methods = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _methods = LatticeTreeAdminGrpcMethods.FromServiceProvider(_services);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private (LatticeTreeAdminApiGrpcClient Client, UnaryResponseCallInvoker Invoker) Create(object response)
    {
        var invoker = new UnaryResponseCallInvoker(response);
        return (new LatticeTreeAdminApiGrpcClient(invoker, _methods), invoker);
    }

    private static TreeRestoreResult Restore(string targetTreeId) => new()
    {
        BackupId = "bk-1",
        TargetTreeId = targetTreeId,
        Mode = TreeRestoreMode.InPlace,
        OperationId = "op-1",
        ManifestChain = ["m-1"],
        EntriesApplied = 3,
    };

    // ----- Construction guards -----

    [Test]
    public void Create_rejects_a_null_call_invoker()
    {
        Assert.Throws<ArgumentNullException>(() =>
            LatticeTreeAdminApiGrpcClient.Create(null!, _services));
    }

    [Test]
    public void Create_rejects_a_null_serializer_provider()
    {
        Assert.Throws<ArgumentNullException>(() =>
            LatticeTreeAdminApiGrpcClient.Create(new UnaryResponseCallInvoker(new object()), null!));
    }

    [Test]
    public void Create_builds_a_client_over_the_supplied_invoker()
    {
        var client = LatticeTreeAdminApiGrpcClient.Create(
            new UnaryResponseCallInvoker(new TreeExistenceResult { TreeId = "orders" }), _services);

        Assert.That(client, Is.Not.Null);
    }

    // ----- Restore set / revert -----

    [Test]
    public async Task RestoreTreeSetAsync_projects_the_set_id_and_unwraps_the_member_results()
    {
        var expected = Restore("orders");
        var (client, invoker) = Create(new TreeRestoreSetResult { Results = [expected] });

        var results = await client.RestoreTreeSetAsync("nightly-set");

        Assert.Multiple(() =>
        {
            Assert.That(results, Has.Count.EqualTo(1));
            Assert.That(results[0], Is.SameAs(expected));
            Assert.That(
                ((TreeAdminRestoreSetRequest)invoker.LastRequest!).SetId,
                Is.EqualTo("nightly-set"));
            Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeTreeAdminGrpcMethods.RestoreTreeSetMethodName));
        });
    }

    [Test]
    public void RestoreTreeSetAsync_rejects_an_empty_set_id()
    {
        var (client, _) = Create(new TreeRestoreSetResult { Results = [] });

        Assert.ThrowsAsync<ArgumentException>(async () => await client.RestoreTreeSetAsync(string.Empty));
    }

    [Test]
    public async Task RevertTreeRestoreAsync_sends_the_restore_result_as_the_request()
    {
        var restore = Restore("orders");
        var (client, invoker) = Create(restore);

        await client.RevertTreeRestoreAsync(restore);

        Assert.Multiple(() =>
        {
            Assert.That(invoker.LastRequest, Is.SameAs(restore));
            Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeTreeAdminGrpcMethods.RevertTreeRestoreMethodName));
        });
    }

    [Test]
    public void RevertTreeRestoreAsync_rejects_a_null_restore_result()
    {
        var (client, _) = Create(Restore("orders"));

        Assert.ThrowsAsync<ArgumentNullException>(async () => await client.RevertTreeRestoreAsync(null!));
    }

    // ----- WAL move planning -----

    [Test]
    public async Task PlanWalMoveAsync_projects_the_tree_partition_and_target_provider()
    {
        var (client, invoker) = Create(new TreeWalMovePlan { TreeId = "orders", Partition = 2 });

        var plan = await client.PlanWalMoveAsync("orders", 2, "azure-table");

        var request = (TreeAdminWalMovePlanRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(plan.TreeId, Is.EqualTo("orders"));
            Assert.That(request.TreeId, Is.EqualTo("orders"));
            Assert.That(request.Partition, Is.EqualTo(2));
            Assert.That(request.TargetProviderKey, Is.EqualTo("azure-table"));
        });
    }

    [Test]
    public void PlanWalMoveAsync_rejects_an_empty_tree_id()
    {
        var (client, _) = Create(new TreeWalMovePlan { TreeId = "orders" });

        Assert.ThrowsAsync<ArgumentException>(async () => await client.PlanWalMoveAsync(string.Empty, 0, "azure-table"));
    }

    [Test]
    public void PlanWalMoveAsync_rejects_an_empty_target_provider_key()
    {
        var (client, _) = Create(new TreeWalMovePlan { TreeId = "orders" });

        Assert.ThrowsAsync<ArgumentException>(async () => await client.PlanWalMoveAsync("orders", 0, string.Empty));
    }

    // ----- Materialised views -----

    [Test]
    public async Task RebuildViewAsync_projects_the_view_name()
    {
        var (client, invoker) = Create(new TreeViewStatus { ViewName = "by-region", SourceTreeId = "orders" });

        var status = await client.RebuildViewAsync("by-region");

        Assert.Multiple(() =>
        {
            Assert.That(status.ViewName, Is.EqualTo("by-region"));
            Assert.That(((TreeAdminViewRequest)invoker.LastRequest!).ViewName, Is.EqualTo("by-region"));
            Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeTreeAdminGrpcMethods.RebuildViewMethodName));
        });
    }

    [Test]
    public void RebuildViewAsync_rejects_an_empty_view_name()
    {
        var (client, _) = Create(new TreeViewStatus { ViewName = "by-region", SourceTreeId = "orders" });

        Assert.ThrowsAsync<ArgumentException>(async () => await client.RebuildViewAsync(string.Empty));
    }

    [Test]
    public async Task ReconcileViewAsync_projects_the_view_name_and_returns_the_drift_verdict()
    {
        var (client, invoker) = Create(new TreeViewReconcileResult
        {
            ViewName = "by-region",
            SourceTreeId = "orders",
            DriftRepaired = true,
        });

        var result = await client.ReconcileViewAsync("by-region");

        Assert.Multiple(() =>
        {
            Assert.That(result.DriftRepaired, Is.True);
            Assert.That(((TreeAdminViewRequest)invoker.LastRequest!).ViewName, Is.EqualTo("by-region"));
            Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeTreeAdminGrpcMethods.ReconcileViewMethodName));
        });
    }

    [Test]
    public void ReconcileViewAsync_rejects_an_empty_view_name()
    {
        var (client, _) = Create(new TreeViewReconcileResult { ViewName = "by-region", SourceTreeId = "orders" });

        Assert.ThrowsAsync<ArgumentException>(async () => await client.ReconcileViewAsync(string.Empty));
    }

    [Test]
    public async Task DropViewAsync_projects_the_view_name_and_discards_the_ack()
    {
        var (client, invoker) = Create(new TreeAdminViewRequest { ViewName = "by-region" });

        await client.DropViewAsync("by-region");

        Assert.Multiple(() =>
        {
            Assert.That(((TreeAdminViewRequest)invoker.LastRequest!).ViewName, Is.EqualTo("by-region"));
            Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeTreeAdminGrpcMethods.DropViewMethodName));
        });
    }

    [Test]
    public void DropViewAsync_rejects_an_empty_view_name()
    {
        var (client, _) = Create(new TreeAdminViewRequest { ViewName = "by-region" });

        Assert.ThrowsAsync<ArgumentException>(async () => await client.DropViewAsync(string.Empty));
    }

    // ----- History retention -----

    [Test]
    public async Task SetHistoryRetentionAsync_projects_the_mode_and_window()
    {
        var (client, invoker) = Create(new TreeHistoryRetention
        {
            TreeId = "orders",
            Window = TimeSpan.FromHours(6),
        });

        await client.SetHistoryRetentionAsync("orders", TreeHistoryRetentionMode.Hybrid, TimeSpan.FromHours(6));

        var request = (TreeAdminSetRetentionRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(request.TreeId, Is.EqualTo("orders"));
            Assert.That(request.Mode, Is.EqualTo(TreeHistoryRetentionMode.Hybrid));
            Assert.That(request.Window, Is.EqualTo(TimeSpan.FromHours(6)));
        });
    }

    [Test]
    public void SetHistoryRetentionAsync_rejects_an_empty_tree_id()
    {
        var (client, _) = Create(new TreeHistoryRetention { TreeId = "orders" });

        Assert.ThrowsAsync<ArgumentException>(async () =>
            await client.SetHistoryRetentionAsync(string.Empty, TreeHistoryRetentionMode.Hybrid, TimeSpan.FromHours(1)));
    }

    [Test]
    public void SetHistoryRetentionAsync_rejects_a_non_positive_window()
    {
        // A zero or negative window is not a retention policy: rejecting it client
        // side keeps a nonsensical request off the wire entirely.
        var (client, _) = Create(new TreeHistoryRetention { TreeId = "orders" });

        var ex = Assert.ThrowsAsync<ArgumentException>(async () =>
            await client.SetHistoryRetentionAsync("orders", TreeHistoryRetentionMode.Hybrid, TimeSpan.Zero));

        Assert.That(ex!.ParamName, Is.EqualTo("window"));
    }

    [Test]
    public async Task SetHistoryRetentionAsync_allows_a_null_window_for_a_mode_only_change()
    {
        var (client, invoker) = Create(new TreeHistoryRetention { TreeId = "orders" });

        await client.SetHistoryRetentionAsync("orders", TreeHistoryRetentionMode.MetadataOnly, window: null);

        Assert.That(((TreeAdminSetRetentionRequest)invoker.LastRequest!).Window, Is.Null);
    }

    // ----- Tag indexes -----

    [Test]
    public async Task ListTagIndexesAsync_sends_the_empty_catalog_request()
    {
        var (client, invoker) = Create(new TreeTagIndexCatalog());

        await client.ListTagIndexesAsync();

        Assert.Multiple(() =>
        {
            Assert.That(invoker.LastRequest, Is.InstanceOf<TreeAdminTagIndexListRequest>());
            Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeTreeAdminGrpcMethods.ListTagIndexesMethodName));
        });
    }

    [Test]
    public async Task UnaryAsync_threads_the_caller_cancellation_token_onto_the_call_options()
    {
        using var cts = new CancellationTokenSource();
        var (client, invoker) = Create(new TreeExistenceResult { TreeId = "orders", Exists = true });

        await client.CheckTreeExistsAsync("orders", cts.Token);

        Assert.That(invoker.LastCancellationToken, Is.EqualTo(cts.Token));
    }
}
