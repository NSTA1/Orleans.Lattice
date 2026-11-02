using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>App binding suggestions retain tenant priority and isolation through directory faults and cancellation.</summary>
[TestFixture]
public sealed class AppGroupSuggestionSourceTests
{
    [Test]
    public async Task SuggestAsync_directory_failure_still_offers_cluster_groups_and_retries_next_time()
    {
        var facades = new FakeTenantAccessFacades().AsTenantAdmin().WithGroup("acme", "ops", "Operations");
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.ListGroupsAsync("acme", Arg.Any<TenantAccessPageRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<TenantGroupPage>(new InvalidOperationException("offline")));
        facades.DirectoryOverride = directory;
        var cluster = Cluster(LtSuggestionSet.Of([new("operators", "Operations")]));
        var source = new AppGroupSuggestionSource(cluster, new TenantAccessCatalog(facades), "acme");

        var failed = await source.SuggestAsync("op", 10, CancellationToken.None);
        facades.DirectoryOverride = null;
        var retried = await source.SuggestAsync("op", 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(failed.IsAvailable, Is.True);
            Assert.That(failed.Items.Select(item => (item.Value, item.Detail)),
                Is.EqualTo(new[] { ("operators", "Operations") }), "the cluster source's own label is kept");
            Assert.That(retried.Items.Select(item => item.Value), Is.EqualTo(new[] { "t/acme/ops", "operators" }));
            Assert.That(source.Tenant, Is.EqualTo("acme"));
        });
    }

    [Test]
    public void SuggestAsync_cancelled_tenant_read_propagates_without_asking_the_cluster()
    {
        using var caller = new CancellationTokenSource();
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.ListGroupsAsync("acme", Arg.Any<TenantAccessPageRequest>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                caller.Cancel();
                return Task.FromCanceled<TenantGroupPage>(call.Arg<CancellationToken>());
            });
        var facades = new FakeTenantAccessFacades { DirectoryOverride = directory };
        var cluster = Cluster(LtSuggestionSet.Of([new("operators", null)]));
        var source = new AppGroupSuggestionSource(cluster, new TenantAccessCatalog(facades), "acme");

        var error = Assert.CatchAsync<OperationCanceledException>(async () => await source.SuggestAsync("op", 10, caller.Token));
        Assert.That(error!.CancellationToken, Is.EqualTo(caller.Token));
        cluster.DidNotReceive().SuggestAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task SuggestAsync_missing_tenant_and_unavailable_cluster_preserve_the_reason()
    {
        var unavailable = LtSuggestionSet.Unavailable("Directory not served.");
        var source = new AppGroupSuggestionSource(Cluster(unavailable), new TenantAccessCatalog(new FakeTenantAccessFacades()), null);

        var answer = await source.SuggestAsync("op", 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(answer.IsAvailable, Is.False);
            Assert.That(answer.UnavailableReason, Is.EqualTo("Directory not served."));
            Assert.That(source.Tenant, Is.Null);
        });
    }

    [Test]
    public async Task SuggestAsync_available_tenant_groups_survive_an_unavailable_cluster()
    {
        var facades = new FakeTenantAccessFacades().AsTenantAdmin().WithGroup("acme", "ops");
        var source = new AppGroupSuggestionSource(Cluster(LtSuggestionSet.Unavailable("offline")), new TenantAccessCatalog(facades), "acme");

        var answer = await source.SuggestAsync(" op ", 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(answer.IsAvailable, Is.True);
            Assert.That(answer.Items.Select(item => (item.Value, item.Detail)), Is.EqualTo(new[] { ("t/acme/ops", "Tenant") }));
        });
    }

    [TestCase(0, new string[] { })]
    [TestCase(1, new[] { "t/acme/ops" })]
    [TestCase(2, new[] { "t/acme/ops", "operators" })]
    public async Task SuggestAsync_tenant_groups_take_priority_and_reserved_cluster_ids_are_never_offered(int limit, string[] expected)
    {
        var facades = new FakeTenantAccessFacades().AsTenantAdmin().WithGroup("acme", "ops");
        var cluster = Cluster(LtSuggestionSet.Of([new("t/other/ops", null), new("operators", null)], truncated: true));
        var source = new AppGroupSuggestionSource(cluster, new TenantAccessCatalog(facades), "acme");

        var answer = await source.SuggestAsync("op", limit, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(answer.Items.Select(item => item.Value), Is.EqualTo(expected));
            Assert.That(answer.Truncated, Is.True);
        });
    }

    [Test]
    public async Task SuggestAsync_invalid_local_names_are_omitted_and_tenant_truncation_is_preserved()
    {
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.ListGroupsAsync("acme", Arg.Any<TenantAccessPageRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new TenantGroupPage
            {
                Entries = [
                    new() { Name = "" },
                    new() { Name = "t/other/ops" },
                    new() { Name = "ops-a" },
                    new() { Name = "ops-b" },
                ],
            }));
        var facades = new FakeTenantAccessFacades { DirectoryOverride = directory };
        var source = new AppGroupSuggestionSource(Cluster(LtSuggestionSet.Of([])), new TenantAccessCatalog(facades), "acme");

        var answer = await source.SuggestAsync("ops", 1, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(answer.Items.Select(item => item.Value), Is.EqualTo(new[] { "t/acme/ops-a" }));
            Assert.That(answer.Truncated, Is.True);
        });
    }

    [Test]
    public void SuggestAsync_null_text_is_rejected_before_either_directory_is_asked()
    {
        var cluster = Cluster(LtSuggestionSet.Of([]));
        var facades = new FakeTenantAccessFacades();
        var source = new AppGroupSuggestionSource(cluster, new TenantAccessCatalog(facades), "acme");

        Assert.ThrowsAsync<ArgumentNullException>(async () => await source.SuggestAsync(null!, 10, CancellationToken.None));
        Assert.That(facades.Gate.Calls, Is.Empty);
        cluster.DidNotReceive().SuggestAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
    }

    private static ILtSuggestionSource Cluster(LtSuggestionSet answer)
    {
        var cluster = Substitute.For<ILtSuggestionSource>();
        cluster.SuggestAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(ValueTask.FromResult(answer));
        return cluster;
    }
}
