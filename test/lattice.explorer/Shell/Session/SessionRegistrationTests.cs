using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Shell;
using Orleans.Lattice.Explorer.Shell.Design.Slots;
using Orleans.Lattice.Explorer.Shell.Session;

namespace Orleans.Lattice.Explorer.Tests.Shell.Session;

/// <summary>
/// The session chrome's registrations: its three slot contributions and the
/// per-circuit isolation of everything it registers.
/// </summary>
[TestFixture]
public sealed class SessionRegistrationTests
{
    [Test]
    public void It_fills_the_three_session_slots()
    {
        var services = new ServiceCollection().AddLatticeExplorerShell();

        var slots = services
            .Where(descriptor => descriptor.ServiceType == typeof(IShellSlot))
            .Select(descriptor => (IShellSlot)descriptor.ImplementationInstance!)
            .Select(slot => (slot.Name, slot.ComponentType))
            .ToArray();

        Assert.That(slots, Is.SupersetOf(new[]
        {
            (ShellSlotNames.HeaderConnection, typeof(ConnectionIndicator)),
            (ShellSlotNames.HeaderIdentity, typeof(IdentityMenu)),
            (ShellSlotNames.OverlaySession, typeof(SessionOverlay)),
        }));
    }

    [Test]
    public void Everything_that_holds_state_is_scoped_per_circuit()
    {
        var services = new ServiceCollection().AddLatticeExplorerShell();

        Assert.Multiple(() =>
        {
            Assert.That(Lifetime(services, typeof(SessionChromeState)), Is.EqualTo(ServiceLifetime.Scoped));
            Assert.That(Lifetime(services, typeof(IConnectionTester)), Is.EqualTo(ServiceLifetime.Scoped));
            Assert.That(Lifetime(services, typeof(SessionSignInOptions)), Is.EqualTo(ServiceLifetime.Singleton));
            Assert.That(
                typeof(SessionSignInOptions).GetProperties().Where(property => property.SetMethod is { IsPublic: true } setter
                    && !setter.ReturnParameter.GetRequiredCustomModifiers().Contains(typeof(System.Runtime.CompilerServices.IsExternalInit))),
                Is.Empty,
                "the one singleton must be immutable once registered");
        });
    }

    [Test]
    public void Two_circuits_never_share_session_state()
    {
        using var provider = new ServiceCollection()
            .AddSingleton<IExplorerSession>(_ => new FakeExplorerSession(new FakeStateConnection()))
            .AddScoped<IExplorerAuthSession, FakeAuthSession>()
            // The navigation chrome registered beside the session chrome reads the
            // circuit's navigation manager and JavaScript runtime, which a head provides.
            .AddScoped<Microsoft.AspNetCore.Components.NavigationManager>(_ => new Orleans.Lattice.Explorer.Tests.Shell.Navigation.TestNavigationManager())
            .AddScoped(_ => NSubstitute.Substitute.For<Microsoft.JSInterop.IJSRuntime>())
            .AddLatticeExplorerShell()
            .BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });

        using var first = provider.CreateScope();
        using var second = provider.CreateScope();

        var one = first.ServiceProvider.GetRequiredService<SessionChromeState>();
        var two = second.ServiceProvider.GetRequiredService<SessionChromeState>();
        one.OpenSignIn();

        Assert.Multiple(() =>
        {
            Assert.That(two, Is.Not.SameAs(one));
            Assert.That(two.Overlay, Is.EqualTo(SessionOverlayKind.None));
            Assert.That(first.ServiceProvider.GetRequiredService<IConnectionTester>(), Is.InstanceOf<LatticeConnectionTester>());
        });
    }

    [Test]
    public void A_head_supplied_sign_in_options_instance_wins()
    {
        var head = new SessionSignInOptions { LoginPath = "explorer/auth/login" };
        var services = new ServiceCollection().AddSingleton(head).AddLatticeExplorerShell();

        using var provider = services.BuildServiceProvider();

        Assert.That(provider.GetRequiredService<SessionSignInOptions>(), Is.SameAs(head));
    }

    private static ServiceLifetime Lifetime(IServiceCollection services, Type type) =>
        services.Single(descriptor => descriptor.ServiceType == type).Lifetime;
}
