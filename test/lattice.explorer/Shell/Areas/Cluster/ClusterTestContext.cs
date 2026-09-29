using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Shell.Areas.Cluster;
using Orleans.Lattice.Explorer.Shell.Areas.Cluster.Pages;
using Orleans.Lattice.Explorer.Shell.Design.Components;
using Orleans.Lattice.Explorer.Shell.Design.Tokens;
using Orleans.Lattice.Explorer.Shell.Navigation;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;
using Orleans.Lattice.Explorer.Tests.Shell.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Cluster;

/// <summary>
/// The bUnit context for the Cluster area: the chrome's services plus fakes of
/// every facade the area reads - tree administration, the replication peer
/// report, and Core's state connection - so nothing ever dials a cluster.
/// </summary>
public abstract class ClusterTestContext : ShellChromeTestContext
{
    /// <summary>Registers the fakes over the Shell's registrations.</summary>
    protected ClusterTestContext()
    {
        Admin = Substitute.For<ILatticeTreeAdmin>();
        Status = Substitute.For<ILatticeReplicationStatus>();
        Services.AddSingleton(Admin);
        Services.AddSingleton(Status);

        // The area under test is registered as the Shell registers it, so the
        // directory and navigator know it however the chrome context seeds areas.
        Services.AddExplorerArea<ClusterArea>();

        Admin.ProbeCapabilitiesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => Capabilities(call.Arg<string>()));
        Admin.GetTreeConfigAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => new TreeConfigurationReport { TreeId = call.Arg<string>(), Exists = true, ShardCount = 4, MaxLeafKeys = 128, MaxInternalChildren = 64 });
        Status.GetPeerStatusAsync(Arg.Any<ReplicationPeerStatusQuery>(), Arg.Any<CancellationToken>())
            .Returns(ReplicationPeerStatusPage.Empty("eu-west"));
        Explorer.Connection.GetClusterInfoAsync(Arg.Any<ClusterInfoRequest>(), Arg.Any<CancellationToken>())
            .Returns(new ClusterInfo { ClusterId = "lattice-prod", ServiceId = "lattice" });
        UseTrees();
    }

    /// <summary>The tree administration fake.</summary>
    internal ILatticeTreeAdmin Admin { get; }

    /// <summary>The replication peer report fake.</summary>
    internal ILatticeReplicationStatus Status { get; }

    /// <summary>The flags every probe answers, unless a test grants otherwise.</summary>
    internal Grants Granted { get; set; } = Grants.All;

    /// <summary>The toasts posted so far.</summary>
    internal IReadOnlyList<string> Toasts =>
        Services.GetRequiredService<LtToastService>().Toasts.Select(toast => toast.Message).ToArray();

    /// <summary>Answers the tree catalogue with <paramref name="entries"/>.</summary>
    /// <param name="entries">The catalogue.</param>
    internal void UseTrees(params TreeCatalogEntry[] entries) =>
        Explorer.Connection.ListTreesAsync(Arg.Any<CatalogRequest>(), Arg.Any<CancellationToken>())
            .Returns(new TreeCatalogPage { Entries = entries });

    /// <summary>A catalogue entry.</summary>
    /// <param name="treeId">The tree id.</param>
    /// <param name="shards">Its shard count.</param>
    /// <returns>The entry.</returns>
    internal static TreeCatalogEntry Tree(string treeId, int shards = 4) => new()
    {
        TreeId = treeId,
        ShardCount = shards,
        Config = new TreeConfigSummary { ShardCount = shards, VirtualShardCount = 4096, WalPartitions = 2 },
    };

    /// <summary>Renders the area's routed page at <paramref name="address"/>, the Cluster area visible.</summary>
    /// <param name="address">A Cluster address, such as <c>/cluster/trees</c>.</param>
    /// <param name="breakpoint">The measured width band, or <see langword="null"/> for none.</param>
    /// <returns>The rendered page.</returns>
    internal IRenderedComponent<ClusterPage> RenderAt(string address, LtBreakpoint? breakpoint = null)
    {
        var parsed = ExplorerAddress.Parse(address);
        Navigation.NavigateTo(parsed.ToHref());
        var area = Services.GetServices<IExplorerArea>().OfType<ClusterArea>().Single();
        return Render<ClusterPage>(parameters =>
        {
            parameters.AddCascadingValue(new ExplorerLocation(parsed, [new ExplorerAreaEntry(area, AreaAvailability.Visible)], EntriesLoaded: true, TenancyActive: false));
            if (breakpoint is { } band)
            {
                parameters.AddCascadingValue(LtBreakpointCascade.Name, band);
            }
        });
    }

    /// <summary>Types <paramref name="name"/> into the open typed confirmation and submits it.</summary>
    /// <param name="cut">The rendered component.</param>
    /// <param name="name">The name to type.</param>
    internal static void ConfirmTyping<TComponent>(IRenderedComponent<TComponent> cut, string name)
        where TComponent : IComponent
    {
        cut.Find(".lt-confirm input").Input(name);
        cut.Find("form.lt-confirm").Submit();
    }

    /// <summary>Finds the one button whose text is <paramref name="text"/>.</summary>
    /// <param name="cut">The rendered component.</param>
    /// <param name="text">The button text.</param>
    /// <returns>The button.</returns>
    internal static AngleSharp.Dom.IElement Button<TComponent>(IRenderedComponent<TComponent> cut, string text)
        where TComponent : IComponent =>
        cut.FindAll("button").Single(button => button.TextContent.Trim() == text);

    /// <summary>Whether a button with <paramref name="text"/> is rendered.</summary>
    /// <param name="cut">The rendered component.</param>
    /// <param name="text">The button text.</param>
    /// <returns><see langword="true"/> when one is.</returns>
    internal static bool HasButton<TComponent>(IRenderedComponent<TComponent> cut, string text)
        where TComponent : IComponent =>
        cut.FindAll("button").Any(button => button.TextContent.Trim() == text);

    private LatticeTreeAdminCapabilities Capabilities(string treeId) => new()
    {
        TreeId = treeId,
        Schema = new LatticeSchemaCapabilities { TreeId = treeId },
        CanViewDiagnostics = Granted.HasFlag(Grants.Read),
        CanAdministerTree = Granted.HasFlag(Grants.Admin),
        CanManageTreeLifecycle = Granted.HasFlag(Grants.Lifecycle),
        CanBulkLoad = Granted.HasFlag(Grants.BulkLoad),
    };

    /// <summary>The probe flags a test grants.</summary>
    [Flags]
    internal enum Grants
    {
        /// <summary>No grant.</summary>
        None = 0,

        /// <summary>Whole-tree read.</summary>
        Read = 1,

        /// <summary>Whole-tree admin.</summary>
        Admin = 2,

        /// <summary>TreeLifecycle.</summary>
        Lifecycle = 4,

        /// <summary>BulkLoad.</summary>
        BulkLoad = 8,

        /// <summary>Every grant.</summary>
        All = Read | Admin | Lifecycle | BulkLoad,
    }
}
