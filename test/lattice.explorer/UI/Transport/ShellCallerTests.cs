using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Tests.Connection;
using Orleans.Lattice.Explorer.Tests.UI.Session;
using Orleans.Lattice.Explorer.UI;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Transport;

/// <summary>
/// The caller key every per-circuit memo is filed under (issue #4019): it names the
/// sign-in, the endpoint and the asserted tenant, and moves on every sign-in or
/// connection change, so no memo can outlive the caller it was read for.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ShellCallerTests
{
    private readonly FakeAuthSession _auth = new();
    private readonly FakeExplorerSession _session = new FakeExplorerSession(new FakeStateConnection())
        .Configured(new ExplorerConfiguration { Endpoint = "https://cluster.example:5001" });

    [Test]
    public void The_key_names_the_sign_in_the_endpoint_and_the_asserted_tenant()
    {
        var tenant = new FakeActiveTenantProvider("acme");
        using var caller = new ShellCaller(_auth, _session, tenant);
        var anonymous = caller.Current;

        _auth.SignIn("alice", "oidc");
        var alice = caller.Current;

        Assert.Multiple(() =>
        {
            Assert.That(anonymous.Authenticated, Is.False);
            Assert.That(alice.Authenticated, Is.True);
            Assert.That(alice.Scheme, Is.EqualTo("oidc"));
            Assert.That(alice.User, Is.EqualTo("alice"));
            Assert.That(alice.Endpoint, Is.EqualTo("https://cluster.example:5001"));
            Assert.That(alice.Tenant, Is.EqualTo("acme"));
            Assert.That(alice, Is.Not.EqualTo(anonymous));
        });
    }

    [Test]
    public async Task Every_sign_in_or_connection_change_moves_the_key_even_for_the_same_name()
    {
        using var caller = new ShellCaller(_auth, _session);
        _auth.SignIn("alex");
        var first = caller.Current;

        // Another identity that happens to share the display name.
        _auth.SignIn("alex");
        var second = caller.Current;
        await _session.ApplyAsync(new ExplorerConfiguration { Endpoint = "https://cluster.example:5001" });

        Assert.Multiple(() =>
        {
            Assert.That(second, Is.Not.EqualTo(first));
            Assert.That(caller.Current, Is.Not.EqualTo(second), "a connection change moves it too");
            Assert.That(caller.Current, Is.EqualTo(caller.Current), "reading it twice is the same key");
        });
    }

    [Test]
    public void The_identity_drops_only_the_tenant()
    {
        var tenant = new FakeActiveTenantProvider("acme");
        using var caller = new ShellCaller(_auth, _session, tenant);
        _auth.SignIn("alice");
        var acme = caller.Current;

        tenant.Set("globex");

        Assert.Multiple(() =>
        {
            Assert.That(caller.Current, Is.Not.EqualTo(acme));
            Assert.That(caller.Current.Identity, Is.EqualTo(acme.Identity));
            Assert.That(acme.Identity.Tenant, Is.Null);
        });
    }

    [Test]
    public void Dispose_stops_listening_to_the_sessions()
    {
        var caller = new ShellCaller(_auth, _session);
        Assert.That(_auth.AuthenticationSubscribers, Is.EqualTo(1));

        caller.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(_auth.AuthenticationSubscribers, Is.Zero);
            Assert.That(_session.ConfigurationSubscribers, Is.Zero);
        });
    }

    [Test]
    public void Of_returns_the_registered_caller()
    {
        using var services = new ServiceCollection()
            .AddSingleton<IExplorerAuthSession>(_auth)
            .AddSingleton<IExplorerSession>(_session)
            .AddScoped(ShellCaller.Create)
            .BuildServiceProvider();
        using var scope = services.CreateScope();

        Assert.That(ShellCaller.Of(scope.ServiceProvider), Is.SameAs(scope.ServiceProvider.GetRequiredService<ShellCaller>()));
    }

    [Test]
    public void Of_without_a_registration_reads_the_sessions_and_subscribes_to_nothing()
    {
        using var services = new ServiceCollection()
            .AddSingleton<IExplorerAuthSession>(_auth)
            .AddSingleton<IExplorerSession>(_session)
            .BuildServiceProvider();

        var caller = ShellCaller.Of(services);
        _auth.SignIn("alice");

        Assert.Multiple(() =>
        {
            Assert.That(caller.Current.User, Is.EqualTo("alice"));
            Assert.That(_auth.AuthenticationSubscribers, Is.Zero, "nothing would ever dispose it");
            Assert.That(() => ShellCaller.Of(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void The_shell_registers_one_scoped_caller_per_circuit()
    {
        var services = new ServiceCollection().AddLatticeExplorerShell();

        Assert.That(services.Single(descriptor => descriptor.ServiceType == typeof(ShellCaller)).Lifetime, Is.EqualTo(ServiceLifetime.Scoped));
    }
}
