using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.UI.Areas.Replication;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Session;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Replication;

/// <summary>
/// The bUnit context the Replication area is tested under: the Shell registered as a
/// head registers it, over fake replication facades (so no availability probe ever
/// dials a transport), a hand-flipped page visibility, and the manual clock.
/// </summary>
public abstract class ReplicationTestContext : ShellChromeTestContext
{
    /// <summary>Registers the fakes after the Shell, so they are what the area resolves.</summary>
    protected ReplicationTestContext()
    {
        Status = new FakeReplicationStatus();
        Control = new FakeReplicationControl();
        Visibility = new FakePageVisibility();
        Services.AddSingleton<ILatticeReplicationStatus>(Status);
        Services.AddSingleton<ILatticeReplicationControl>(Control);
        Services.AddSingleton<IReplicationPageVisibility>(Visibility);

        // The chrome's test context may clear real areas; this one is under test.
        Services.AddExplorerArea<ReplicationArea>();
    }

    internal FakeReplicationStatus Status { get; }

    internal FakeReplicationControl Control { get; }

    internal FakePageVisibility Visibility { get; }

    internal FakeAuthSession Auth => (FakeAuthSession)Services.GetRequiredService<IExplorerAuthSession>();

    internal LtToastService ToastService => Services.GetRequiredService<LtToastService>();

    internal ReplicationDataSource Data => Services.GetRequiredService<ReplicationDataSource>();

    internal ReplicationArea Area => Services.GetServices<IExplorerArea>().OfType<ReplicationArea>().Single();

    /// <summary>Fills the status fake with <see cref="ReplicationTestData.Estate"/>.</summary>
    internal void UseEstate() => Status.Links.AddRange(ReplicationTestData.Estate());

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
