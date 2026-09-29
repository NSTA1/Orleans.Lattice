using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Session;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access;

/// <summary>
/// The bUnit context the Access area is tested under: the Shell registered as a
/// head registers it, with an in-memory <see cref="FakeAuthAdmin"/> as the auth
/// facade and a directly driven sign-in, so no probe or page ever dials gRPC.
/// </summary>
/// <remarks>
/// The fakes are registered after the Shell, so they win over the transport
/// adapter the Shell registers. The chrome context drops the Shell's real areas
/// so its own probe areas stand alone; this context re-registers the Access area,
/// idempotently. bUnit locks the service collection at the first render, so
/// fixtures ask for an instance per test case.
/// </remarks>
public abstract class AccessTestContext : ShellChromeTestContext
{
    /// <summary>Registers the fakes over the Shell.</summary>
    protected AccessTestContext()
    {
        Admin = new FakeAuthAdmin();
        Auth = new FakeAuthSession();
        Auth.SignIn("ops@example.com");
        Services.AddSingleton<ILatticeAuthAdmin>(Admin);
        Services.AddSingleton<IExplorerAuthSession>(Auth);

        // The chrome context keeps only its own probe areas; the area under test is put back.
        Services.AddExplorerArea<AccessArea>();
    }

    /// <summary>The auth facade.</summary>
    internal FakeAuthAdmin Admin { get; }

    /// <summary>The Explorer's sign-in.</summary>
    internal FakeAuthSession Auth { get; }

    /// <summary>A rule governing <paramref name="tree"/> for a group.</summary>
    internal static LatticeAuthorizationRule Rule(
        string id,
        string tree = "orders",
        string group = "ops",
        LatticeOperation operations = LatticeOperation.Read,
        LatticeEffect effect = LatticeEffect.Allow) =>
        new(id, LatticeSubjectSelector.Group(group), LatticeScope.Tree(tree), operations, effect);

    /// <summary>An app-owned rule, id <c>app:{slug}:{role}:{hash}</c>.</summary>
    internal static LatticeAuthorizationRule AppRule(string slug, string role = "viewer", string tree = "a/crm/orders") =>
        new($"app:{slug}:{role}:0123456789abcdef", LatticeSubjectSelector.Group($"{slug}-{role}"), LatticeScope.Tree(tree), LatticeOperation.Read, LatticeEffect.Allow);

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
}
