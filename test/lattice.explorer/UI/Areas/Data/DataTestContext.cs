using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// The bUnit context the Data area is tested under: the Shell registered as a
/// head registers it, the in-memory state API behind the real Core readers, and a
/// substitute tree-administration facade whose capability probe a test sets -
/// so nothing ever dials a cluster.
/// </summary>
public abstract class DataTestContext : ShellChromeTestContext
{
    /// <summary>Registers the fakes over the Shell's own registrations.</summary>
    protected DataTestContext()
    {
        Client = new FakeStateClient();
        Admin = Substitute.For<ILatticeTreeAdmin>();
        Admin.ProbeCapabilitiesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(Capabilities(call.Arg<string>(), AdministeredTrees.Contains(call.Arg<string>()))));

        Services.AddSingleton<ILatticeStateClient>(Client);
        Services.AddSingleton(Admin);
        Services.AddExplorerArea<DataArea>();
    }

    /// <summary>The in-memory state API.</summary>
    internal FakeStateClient Client { get; }

    /// <summary>The substitute tree-administration facade.</summary>
    internal ILatticeTreeAdmin Admin { get; }

    /// <summary>The state ids of the trees the caller may administer.</summary>
    internal HashSet<string> AdministeredTrees { get; } = new(StringComparer.Ordinal);

    /// <summary>The circuit's Data area.</summary>
    internal DataArea Area => Services.GetServices<IExplorerArea>().OfType<DataArea>().Single();

    /// <summary>Turns tenancy on with <paramref name="tenant"/> active and a real ownership-scoping view.</summary>
    /// <param name="tenant">The active tenant.</param>
    internal void UseDataTenancy(string tenant)
    {
        UseTenancy(tenant);
        Services.AddSingleton<IExplorerTenantView>(new FakeTenantView(tenant));
    }

    /// <summary>Navigates to <paramref name="relative"/> and renders the Data page there.</summary>
    /// <param name="relative">The base-relative address, such as <c>data/orders</c>.</param>
    /// <param name="compact">Whether to render at the compact width band.</param>
    internal IRenderedComponent<DataPageHost> RenderAt(string relative, bool compact = false)
    {
        Navigation.NavigateTo(relative);
        return Render<DataPageHost>(parameters => parameters
            .Add(host => host.Compact, compact)
            .Add(host => host.TenancyActive, Services.GetRequiredService<ExplorerTenancy>().IsActive));
    }

    /// <summary>The current address, base-relative.</summary>
    internal string CurrentRelative => "/" + Navigation.ToBaseRelativePath(Navigation.Uri);

    /// <summary>The form control a visible label names.</summary>
    /// <param name="cut">The rendered host.</param>
    /// <param name="label">The label text.</param>
    internal static AngleSharp.Dom.IElement Control(IRenderedComponent<DataPageHost> cut, string label)
    {
        var element = cut.FindAll("label").Single(candidate => candidate.TextContent.Trim() == label);
        return cut.Find("#" + element.GetAttribute("for"));
    }

    /// <summary>The switch whose label is <paramref name="label"/>.</summary>
    /// <param name="cut">The rendered host.</param>
    /// <param name="label">The switch label.</param>
    internal static AngleSharp.Dom.IElement Switch(IRenderedComponent<DataPageHost> cut, string label) =>
        cut.FindAll("button[role=switch]").Single(candidate => candidate.QuerySelector(".lt-switch__label")!.TextContent == label);

    /// <summary>The enabled or disabled button whose text is <paramref name="text"/>.</summary>
    /// <param name="cut">The rendered host.</param>
    /// <param name="text">The button text.</param>
    internal static AngleSharp.Dom.IElement Button(IRenderedComponent<DataPageHost> cut, string text) =>
        cut.FindAll("button").Single(candidate => candidate.TextContent.Trim() == text);

    /// <summary>The rendered booktabs rows (never the virtualiser's spacers).</summary>
    /// <param name="cut">The rendered host.</param>
    internal static IReadOnlyList<AngleSharp.Dom.IElement> Rows(IRenderedComponent<DataPageHost> cut) =>
        cut.FindAll("tbody tr.lt-table__row").Where(row => row.QuerySelector(".lt-table__empty") is null).ToList();

    /// <summary>Types the object name into an open destructive confirmation and confirms it.</summary>
    /// <param name="cut">The rendered host.</param>
    /// <param name="name">The object name to type.</param>
    internal static void Confirm(IRenderedComponent<DataPageHost> cut, string name)
    {
        cut.Find(".lt-confirm input").Input(name);
        cut.Find(".lt-confirm").Submit();
    }

    private static LatticeTreeAdminCapabilities Capabilities(string treeId, bool admin) => new()
    {
        TreeId = treeId,
        CanAdministerTree = admin,
        Schema = new LatticeSchemaCapabilities { TreeId = treeId },
    };
}
