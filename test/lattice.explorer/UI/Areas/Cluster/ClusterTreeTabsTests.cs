using System.Collections.Immutable;
using Bunit;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>
/// A tree's tabs: lifecycle verbs behind typed confirmations and the TreeLifecycle
/// grant (purge stating it is not an app operation), alias behind admin
/// authority, configuration and retention changes that apply only what changed,
/// the shard join and its expensive deep read, and storage with WAL placement.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterTreeTabsTests : ClusterTestContext
{
    private const string TreeId = "a/crm/orders";

    [Test]
    public void Delete_needs_the_trees_name_typed_then_soft_deletes_it()
    {
        Admin.GetTreeDeletionStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeDeletionStatus { TreeId = TreeId });
        Admin.DeleteTreeAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(new TreeDeletionStatus { TreeId = TreeId, IsDeleted = true, CanRecover = true, RecoveryDeadlineUtc = new DateTimeOffset(2026, 10, 5, 0, 0, 0, TimeSpan.Zero) });
        var cut = RenderTab<ClusterTreeLifecycle>();

        Button(cut, "Delete tree...").Click();
        Assert.That(cut.Find(".lt-confirm__consequence").TextContent, Does.Contain("Every read and write on it fails at once"));
        cut.Find(".lt-confirm input").Input("a/crm/Orders");
        Assert.That(cut.Find(".lt-confirm button[type=submit]").HasAttribute("disabled"), Is.True);
        ConfirmTyping(cut, TreeId);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Markup, Does.Contain("Deleted, recoverable").And.Contain("2026-10-05 00:00:00 UTC"));
            Assert.That(HasButton(cut, "Recover tree..."), Is.True);
            Assert.That(HasButton(cut, "Purge now..."), Is.True);
            Assert.That(Toasts, Does.Contain("Tree deleted. It can be recovered until its window closes."));
        });
    }

    [Test]
    public void Recover_and_purge_each_confirm_and_purge_states_its_grant_and_that_it_is_no_app_operation()
    {
        Admin.GetTreeDeletionStatusAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(new TreeDeletionStatus { TreeId = TreeId, IsDeleted = true, CanRecover = true });
        Admin.RecoverTreeAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeDeletionStatus { TreeId = TreeId });
        Admin.PurgeTreeAsync(TreeId, true, Arg.Any<CancellationToken>())
            .Returns(new TreeDeletionStatus { TreeId = TreeId, IsDeleted = true, PurgeComplete = true });
        var cut = RenderTab<ClusterTreeLifecycle>();

        Button(cut, "Purge now...").Click();
        Assert.That(cut.Find(".lt-confirm__consequence").TextContent,
            Does.Contain("cannot be undone").And.Contain("Purge requires the TreeLifecycle grant and is not an app operation"));
        ConfirmTyping(cut, TreeId);
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("Purged")));
        Admin.Received(1).PurgeTreeAsync(TreeId, true, Arg.Any<CancellationToken>());

        var recover = RenderTab<ClusterTreeLifecycle>();
        Admin.GetTreeDeletionStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeDeletionStatus { TreeId = TreeId, IsDeleted = true, CanRecover = true });
        Button(recover, "Recover tree...").Click();
        ConfirmTyping(recover, TreeId);
        recover.WaitUntil(() => Assert.That(Toasts, Does.Contain("Tree recovered.")));
    }

    [Test]
    public void Lifecycle_verbs_are_hidden_without_the_grant_and_a_failure_is_a_toast()
    {
        Admin.GetTreeDeletionStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeDeletionStatus { TreeId = TreeId });
        var hidden = RenderTab<ClusterTreeLifecycle>(Grants.Read | Grants.Admin);
        Assert.That(HasButton(hidden, "Delete tree..."), Is.False);

        Admin.DeleteTreeAsync(TreeId, Arg.Any<CancellationToken>()).ThrowsAsync(new InvalidOperationException("The tree is the source of a view."));
        var cut = RenderTab<ClusterTreeLifecycle>();
        Button(cut, "Delete tree...").Click();
        ConfirmTyping(cut, TreeId);

        cut.WaitUntil(() => Assert.That(Toasts, Does.Contain("The tree is the source of a view.")));
    }

    [Test]
    public void Alias_needs_admin_a_target_and_the_trees_name_typed()
    {
        Admin.GetTreeDeletionStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeDeletionStatus { TreeId = TreeId });
        Admin.SetTreeAliasAsync(TreeId, "a/crm/orders-v2", Arg.Any<CancellationToken>())
            .Returns(new TreeAliasResolution { TreeId = TreeId, PhysicalTreeId = "a/crm/orders-v2", IsAliased = true });
        UseTrees(Tree(TreeId), Tree("a/crm/orders-v2"));
        Assert.That(HasButton(RenderTab<ClusterTreeLifecycle>(Grants.Read | Grants.Lifecycle), "Set alias..."), Is.False);
        var cut = RenderTab<ClusterTreeLifecycle>();

        cut.Find("form[aria-label='Set alias']").Submit();
        Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("Name the tree this name should reach."));
        cut.Find("form[aria-label='Set alias'] input").Input(TreeId);
        cut.Find("form[aria-label='Set alias']").Submit();
        Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("A tree cannot alias itself."));

        cut.Find("form[aria-label='Set alias'] input").Input("a/crm/orders-v2");
        cut.Find("form[aria-label='Set alias']").Submit();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-confirm__consequence").TextContent, Does.Contain("a/crm/orders-v2")));
        ConfirmTyping(cut, TreeId);

        cut.WaitUntil(() => Assert.That(Toasts, Does.Contain("Alias set.")));
    }

    [Test]
    public void Configuration_saves_only_what_changed()
    {
        Admin.GetTreeConfigAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(new TreeConfigurationReport { TreeId = TreeId, Exists = true, ShardCount = 4, PublishEvents = true, WalMaxRetainedBytes = 1024 });
        Admin.GetHistoryRetentionAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(new TreeHistoryRetention { TreeId = TreeId, Mode = TreeHistoryRetentionMode.MetadataOnly });
        Admin.SetTreeConfigAsync(TreeId, Arg.Any<TreeConfigurationUpdate>(), Arg.Any<CancellationToken>())
            .Returns(new TreeConfigurationReport { TreeId = TreeId, Exists = true, PublishEvents = false, WalMaxRetainedBytes = 1024 });
        var cut = RenderTab<ClusterTreeConfiguration>();
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("1.0 KiB")));

        cut.Find("form[aria-label='Change configuration']").Submit();
        Assert.That(Toasts, Does.Contain("Nothing to change."));

        cut.Find("form[aria-label='Change configuration'] select").Change("off");
        cut.Find("form[aria-label='Change configuration']").Submit();

        cut.WaitUntil(() => Assert.That(Toasts, Does.Contain("Configuration saved.")));
        Admin.Received(1).SetTreeConfigAsync(TreeId, Arg.Is<TreeConfigurationUpdate>(update =>
            update.ApplyPublishEvents && update.PublishEvents == false
            && !update.ApplyMaintainProjectionDigest && !update.ApplyWalMaxRetainedBytes), Arg.Any<CancellationToken>());
    }

    [Test]
    public void A_bad_wal_ceiling_is_refused_before_any_call()
    {
        Admin.GetTreeConfigAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeConfigurationReport { TreeId = TreeId, Exists = true });
        var cut = RenderTab<ClusterTreeConfiguration>();
        cut.WaitUntil(() => Assert.That(cut.FindAll("form"), Has.Count.EqualTo(2)));

        cut.Find("form[aria-label='Change configuration'] input").Input("-3");
        cut.Find("form[aria-label='Change configuration']").Submit();

        Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("greater than zero"));
        Admin.DidNotReceive().SetTreeConfigAsync(Arg.Any<string>(), Arg.Any<TreeConfigurationUpdate>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void History_retention_saves_its_mode_and_window()
    {
        Admin.GetTreeConfigAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeConfigurationReport { TreeId = TreeId, Exists = true });
        Admin.GetHistoryRetentionAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(new TreeHistoryRetention { TreeId = TreeId, Mode = TreeHistoryRetentionMode.Hybrid, Window = TimeSpan.FromDays(7) });
        Admin.SetHistoryRetentionAsync(TreeId, Arg.Any<TreeHistoryRetentionMode?>(), Arg.Any<TimeSpan?>(), Arg.Any<CancellationToken>())
            .Returns(new TreeHistoryRetention { TreeId = TreeId, Mode = TreeHistoryRetentionMode.FullValue, Window = TimeSpan.FromHours(1) });
        var cut = RenderTab<ClusterTreeConfiguration>();
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("7 days").And.Contain("Hybrid")));

        var form = cut.Find("form[aria-label='Change history retention']");
        form.QuerySelector("select")!.Change("FullValue");
        cut.Find("form[aria-label='Change history retention'] input").Input("3600");
        cut.Find("form[aria-label='Change history retention']").Submit();

        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("1 hour")));
        Admin.Received(1).SetHistoryRetentionAsync(TreeId, TreeHistoryRetentionMode.FullValue, TimeSpan.FromHours(1), Arg.Any<CancellationToken>());
    }

    [Test]
    public void Configuration_is_read_only_without_admin_and_hidden_without_read()
    {
        Admin.GetTreeConfigAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeConfigurationReport { TreeId = TreeId, Exists = true });
        var readOnly = RenderTab<ClusterTreeConfiguration>(Grants.Read);
        var none = RenderTab<ClusterTreeConfiguration>(Grants.Lifecycle);

        readOnly.WaitUntil(() => Assert.That(readOnly.Markup, Does.Contain("Library default")));
        Assert.Multiple(() =>
        {
            Assert.That(readOnly.FindAll("form"), Is.Empty);
            Assert.That(none.Markup, Does.Contain("cannot read this tree's configuration"));
        });
    }

    [Test]
    public void Shards_join_the_map_diagnostics_and_hotness_and_counting_tombstones_asks_first()
    {
        Admin.InspectShardMapAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new ShardMapInspection
        {
            TreeId = TreeId, PhysicalTreeId = "hidden-physical", PhysicalShardCount = 2, VirtualShardCount = 4, MapVersion = 3,
            PhysicalShardIndices = [0, 0, 1, 1],
        });
        Admin.GetShardMapAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeShardMapView { TreeId = TreeId, HasCustomMap = true, MapVersion = 3 });
        Admin.GetDiagnosticsAsync(TreeId, Arg.Any<bool>(), Arg.Any<CancellationToken>()).Returns(call => new TreeAdminDiagnosticReport
        {
            TreeId = TreeId, Deep = call.ArgAt<bool>(1),
            Shards = [new ShardDiagnosticSnapshot { ShardIndex = 0, Depth = 3, LiveKeys = 1200, Tombstones = 7 }, new ShardDiagnosticSnapshot { ShardIndex = 1, SplitInProgress = true }],
        });
        Admin.GetShardHotnessAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeHotnessReport
        {
            TreeId = TreeId, Shards = [new ShardHotnessSnapshot { ShardIndex = 0, Reads = 10, Writes = 4, OpsPerSecond = 2.5 }],
        });
        var cut = RenderTab<ClusterTreeShards>();

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(2)));
        var first = cut.FindAll("tbody tr")[0].QuerySelectorAll("th, td").Select(cell => cell.TextContent.Trim()).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(new[] { "0", "2", "3", "1,200", "-", "10", "4", "2.5", "Steady" }), "tombstones are counted only by a deep read");
            Assert.That(cut.FindAll("tbody tr")[1].TextContent, Does.Contain("Splitting"));
            Assert.That(cut.Markup, Does.Contain("Custom, version 3"));
            Assert.That(cut.Markup, Does.Not.Contain("hidden-physical"));
        });

        Button(cut, "Count tombstones...").Click();
        ConfirmTyping(cut, TreeId);

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr")[0].TextContent, Does.Contain("7")));
        Admin.Received(1).GetDiagnosticsAsync(TreeId, true, Arg.Any<CancellationToken>());
    }

    [Test]
    public void Shards_read_as_compact_rows_and_report_partial_failures()
    {
        Admin.InspectShardMapAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new ShardMapInspection { TreeId = TreeId, PhysicalTreeId = "p", PhysicalShardIndices = [0] });
        Admin.GetDiagnosticsAsync(TreeId, false, Arg.Any<CancellationToken>()).ThrowsAsync(new TimeoutException());
        Admin.GetShardHotnessAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeHotnessReport { TreeId = TreeId });
        var cut = RenderTab<ClusterTreeShards>(breakpoint: LtBreakpoint.Compact);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-table-list__row .lt-compact-row__primary").TextContent, Is.EqualTo("shard 0"));
            Assert.That(cut.Find(".lt-cluster-error").TextContent, Is.EqualTo("Diagnostics: The cluster did not answer in time."));
        });
    }

    [Test]
    public void Storage_shows_bytes_by_surface_and_each_wal_partitions_provider()
    {
        Admin.GetTreeStatsAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(new TreeStatsReport { TreeId = TreeId, LeafStateBytes = 2048, WalRetainedBytes = 512, TotalBytes = 2560, PartialStorage = true });
        Admin.GetWalPlacementAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeWalPlacement
        {
            TreeId = TreeId, Version = 4, DefaultProviderKey = "blob-a",
            Partitions = [new TreeWalPartitionPlacement { Partition = 0, ProviderKey = "blob-a", ResolvableOnThisSilo = true }, new TreeWalPartitionPlacement { Partition = 1, ProviderKey = "blob-b" }],
        });
        var cut = RenderTab<ClusterTreeStorage>();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Markup, Does.Contain("2.0 KiB").And.Contain("partial"));
            Assert.That(cut.FindAll("tbody tr").Select(row => row.TextContent), Has.Some.Contains("blob-b").And.Some.Contains("Does not resolve"));
            Assert.That(cut.Find("a.lt-cluster-link").GetAttribute("href"), Is.EqualTo("cluster/wal?tree=a%2Fcrm%2Forders"));
        });
    }

    [Test]
    public void Tabs_that_need_read_say_so_without_it()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RenderTab<ClusterTreeShards>(Grants.Admin).Markup, Does.Contain("cannot read this tree's shards"));
            Assert.That(RenderTab<ClusterTreeStorage>(Grants.Admin).Markup, Does.Contain("cannot read this tree's storage"));
            Assert.That(RenderTab<ClusterTreeSummary>(Grants.Admin).Markup, Does.Contain("cannot read this tree's diagnostics"));
        });
    }

    [Test]
    public void Wal_partitions_render_nothing_for_an_empty_placement()
    {
        var cut = Render<ClusterWalPartitions>(parameters => parameters.Add(table => table.Partitions, default(ImmutableArray<TreeWalPartitionPlacement>)));

        Assert.That(cut.Find(".lt-empty h3").TextContent, Is.EqualTo("No partitions reported"));
    }

    private IRenderedComponent<TTab> RenderTab<TTab>(Grants grants = Grants.All, LtBreakpoint? breakpoint = null)
        where TTab : Microsoft.AspNetCore.Components.IComponent
    {
        var capabilities = new LatticeTreeAdminCapabilities
        {
            TreeId = TreeId,
            Schema = new LatticeSchemaCapabilities { TreeId = TreeId },
            CanViewDiagnostics = grants.HasFlag(Grants.Read),
            CanAdministerTree = grants.HasFlag(Grants.Admin),
            CanManageTreeLifecycle = grants.HasFlag(Grants.Lifecycle),
            CanBulkLoad = grants.HasFlag(Grants.BulkLoad),
        };

        void Tab(Microsoft.AspNetCore.Components.Rendering.RenderTreeBuilder builder)
        {
            builder.OpenComponent<TTab>(0);
            builder.AddComponentParameter(1, "TreeId", TreeId);
            builder.AddComponentParameter(2, "Capabilities", capabilities);
            builder.CloseComponent();
        }

        var host = Render(builder =>
        {
            builder.OpenComponent<Microsoft.AspNetCore.Components.CascadingValue<LtBreakpoint?>>(0);
            builder.AddComponentParameter(1, "Name", Orleans.Lattice.Explorer.UI.Design.Components.LtBreakpointCascade.Name);
            builder.AddComponentParameter(2, "Value", breakpoint);
            builder.AddComponentParameter(3, "ChildContent", (Microsoft.AspNetCore.Components.RenderFragment)Tab);
            builder.CloseComponent();
        });

        return host.FindComponent<TTab>();
    }
}