using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.UI.Areas.Tenancy;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Session;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Tenancy;

/// <summary>
/// The bUnit context the Tenancy area is tested under: the Shell registered as a
/// head registers it, with one in-memory <see cref="FakeTenancyCluster"/> behind
/// every tenant facade and the apps facade, and a directly driven sign-in. Tenancy
/// starts off; <see cref="UseTenancyAs"/> switches it on, scoped to a tenant, with
/// the caller's operator standing. No probe or page ever dials gRPC.
/// </summary>
/// <remarks>
/// The fakes are registered after the Shell, so they win over the transport
/// adapters the Shell registers. The chrome context drops the Shell's real areas
/// so its own probe areas stand alone; this context puts the Tenancy area back,
/// idempotently. bUnit locks the service collection at the first render, so
/// fixtures ask for an instance per test case.
/// </remarks>
public abstract class TenancyTestContext : ShellChromeTestContext
{
    /// <summary>Registers the fakes over the Shell.</summary>
    protected TenancyTestContext()
    {
        Cluster = new FakeTenancyCluster();
        Auth = new FakeAuthSession();
        Auth.SignIn(FakeTenancyCluster.Caller);
        Services.AddSingleton<ILatticeTenantSelfService>(Cluster);
        Services.AddSingleton<ILatticeTenantAdmin>(Cluster);
        Services.AddSingleton<ILatticeTenantAccessAdmin>(Cluster);
        Services.AddSingleton<ILatticeTenantGrantAdmin>(Cluster);
        Services.AddSingleton<ILatticeTenantRegionAdmin>(Cluster);
        Services.AddSingleton<ILatticeTenantQuotaUsage>(Cluster);
        Services.AddSingleton<ILatticeAppsControl>(Cluster);
        Services.AddSingleton<IExplorerAuthSession>(Auth);

        // The chrome context keeps only its own probe areas; the area under test is put back.
        Services.AddExplorerArea<TenancyArea>();
    }

    /// <summary>The cluster behind every tenant facade.</summary>
    internal FakeTenancyCluster Cluster { get; }

    /// <summary>The Explorer's sign-in.</summary>
    internal FakeAuthSession Auth { get; }

    /// <summary>
    /// Turns tenancy on, scoped to <paramref name="active"/>, with the switcher
    /// reporting <paramref name="isOperator"/> as the caller's operator standing.
    /// </summary>
    /// <param name="active">The active tenant.</param>
    /// <param name="isOperator">Whether the caller validates as a platform operator.</param>
    /// <param name="allowSwitch">Whether the switcher grants a switch.</param>
    internal void UseTenancyAs(string active = "acme", bool isOperator = true, bool allowSwitch = false)
    {
        UseTenancy(active, allowSwitch, [.. Cluster.Tenants.Keys.Where(tenant => tenant != TenantId.DefaultId)]);
        Switcher!.IsOperatorAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<bool>(isOperator));
    }

    /// <summary>A fresh area over this context's services.</summary>
    internal TenancyArea CreateArea() => new(Services);

    /// <summary>The circuit's tenancy catalogue.</summary>
    internal TenancyCatalog Catalog => Services.GetRequiredService<TenancyCatalog>();

    /// <summary>Navigates to <paramref name="relative"/> and renders <typeparamref name="TPage"/> there, at <paramref name="band"/>.</summary>
    internal IRenderedComponent<TPage> RenderAt<TPage>(string relative, LtBreakpoint? band = null)
        where TPage : IComponent
    {
        Navigation.NavigateTo(relative);
        return Render<TPage>(parameters =>
        {
            if (band is { } value)
            {
                parameters.AddCascadingValue(LtBreakpointCascade.Name, value);
            }
        });
    }

    /// <summary>Renders <typeparamref name="TComponent"/> with <paramref name="configure"/>, at <paramref name="band"/>.</summary>
    internal IRenderedComponent<TComponent> RenderSection<TComponent>(Action<ComponentParameterCollectionBuilder<TComponent>> configure, LtBreakpoint? band = null)
        where TComponent : IComponent
    {
        return Render<TComponent>(parameters =>
        {
            configure(parameters);
            if (band is { } value)
            {
                parameters.AddCascadingValue(LtBreakpointCascade.Name, value);
            }
        });
    }
}
