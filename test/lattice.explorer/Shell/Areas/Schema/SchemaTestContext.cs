using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Shell.Areas.Schema;
using Orleans.Lattice.Explorer.Shell.Design.Components;
using Orleans.Lattice.Explorer.Shell.Design.Tokens;
using Orleans.Lattice.Explorer.Shell.Navigation;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;
using Orleans.Lattice.Explorer.Tests.Shell.Navigation;
using Orleans.Lattice.Explorer.Tests.Shell.Session;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Schema;

/// <summary>
/// The bUnit context the Schema area is tested under: the Shell registered as a
/// head registers it, over a fake schema facade and a fake apps facade (so no
/// probe or read ever dials a transport), a scripted tree catalogue, and the
/// manual clock.
/// </summary>
public abstract class SchemaTestContext : ShellChromeTestContext
{
    /// <summary>Registers the fakes after the Shell, so they are what the area resolves.</summary>
    protected SchemaTestContext()
    {
        Schema = new FakeSchemaControl();
        Apps = new FakeSchemaAppsControl();
        Services.AddSingleton<ILatticeSchemaControl>(Schema);
        Services.AddSingleton<ILatticeAppsControl>(Apps);

        // The chrome's test context may clear real areas; this one is under test.
        Services.AddExplorerArea<SchemaArea>();
        UseTrees();
    }

    internal FakeSchemaControl Schema { get; }

    internal FakeSchemaAppsControl Apps { get; }

    internal FakeAuthSession Auth => (FakeAuthSession)Services.GetRequiredService<IExplorerAuthSession>();

    internal LtToastService ToastService => Services.GetRequiredService<LtToastService>();

    internal SchemaDirectory Directory => Services.GetRequiredService<SchemaDirectory>();

    internal SchemaOperations Operations => Services.GetRequiredService<SchemaOperations>();

    internal SchemaComplianceLedger Ledger => Services.GetRequiredService<SchemaComplianceLedger>();

    internal SchemaArea Area => Services.GetServices<IExplorerArea>().OfType<SchemaArea>().Single();

    /// <summary>Makes the catalogue list exactly <paramref name="trees"/>.</summary>
    /// <param name="trees">The logical tree ids.</param>
    internal void UseTrees(params string[] trees) =>
        Explorer.Connection
            .ListTreesAsync(Arg.Any<CatalogRequest>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new TreeCatalogPage { Entries = [.. trees.Select(SchemaTestData.Entry)] }));

    /// <summary>The usual estate: two governed trees, an app tree and an ungoverned one.</summary>
    internal void UseEstate()
    {
        UseTrees("a/crm/orders", "audit", "orders", "scratch");
        Schema.Policies["orders"] = SchemaTestData.Policy();
        Schema.Versions["orders"] = new(7, 3, strictIngest: true);
        Schema.Versions["audit"] = new(2, 1);
        Apps.Add("crm", "2.1.0", "orders");
    }

    /// <summary>
    /// Renders <typeparamref name="TPage"/> at <paramref name="relative"/>, with the
    /// location the layout would cascade and, optionally, a measured width band.
    /// </summary>
    internal IRenderedComponent<TPage> RenderAt<TPage>(string relative, LtBreakpoint? band = null, bool tenancy = false)
        where TPage : IComponent
    {
        Navigation.NavigateTo(relative);
        var address = ExplorerAddress.Parse(relative);
        var location = new ExplorerLocation(address, [], EntriesLoaded: true, TenancyActive: tenancy);
        return Render<TPage>(parameters =>
        {
            parameters.AddCascadingValue(location);
            if (band is { } value)
            {
                parameters.AddCascadingValue(LtBreakpointCascade.Name, value);
            }
        });
    }
}
