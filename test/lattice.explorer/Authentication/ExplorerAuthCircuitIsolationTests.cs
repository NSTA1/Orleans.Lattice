using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Authentication;

/// <summary>
/// Security regression for issue #1264: the auth session and the cluster
/// connection must be scoped per Blazor circuit, so one operator's sign-in can
/// never be inherited by another browser's circuit (cross-user auth bypass /
/// privilege escalation). Each DI scope models one circuit; a per-scope
/// credential store models the per-browser sign-in cookie.
/// </summary>
[TestFixture]
public class ExplorerAuthCircuitIsolationTests
{
    private static ServiceProvider BuildProvider()
    {
        var services = new ServiceCollection();
        // A path that does not exist, so the config store loads no endpoint and
        // sign-in never opens a real connection.
        services.AddExplorerConfiguration(options =>
            options.FilePath = Path.Combine(Path.GetTempPath(), $"lattice-explorer-{Guid.NewGuid():N}.json"));
        services.AddExplorerCatalog();
        // Model the per-browser sign-in cookie: each circuit (scope) gets its own
        // credential store rather than a process-global one. Registered before
        // AddExplorerAuth so its TryAdd default (a singleton in-memory store) is
        // skipped.
        services.AddScoped<ICredentialStore, InMemoryCredentialStore>();
        services.AddExplorerAuth();
        return services.BuildServiceProvider();
    }

    [Test]
    public async Task Two_circuits_get_independent_auth_sessions_and_connections()
    {
        await using var provider = BuildProvider();

        await using var circuitA = provider.CreateAsyncScope();
        await using var circuitB = provider.CreateAsyncScope();

        var authA = circuitA.ServiceProvider.GetRequiredService<IExplorerAuthSession>();
        var authB = circuitB.ServiceProvider.GetRequiredService<IExplorerAuthSession>();
        var connectionA = circuitA.ServiceProvider.GetRequiredService<ILatticeStateConnection>();
        var connectionB = circuitB.ServiceProvider.GetRequiredService<ILatticeStateConnection>();

        Assert.Multiple(() =>
        {
            // A circuit resolves the same instances throughout its own lifetime.
            Assert.That(circuitA.ServiceProvider.GetRequiredService<IExplorerAuthSession>(), Is.SameAs(authA));
            Assert.That(circuitA.ServiceProvider.GetRequiredService<ILatticeStateConnection>(), Is.SameAs(connectionA));

            // Distinct circuits never share the auth session or the connection.
            Assert.That(authB, Is.Not.SameAs(authA));
            Assert.That(connectionB, Is.Not.SameAs(connectionA));
        });
    }

    [Test]
    public async Task Sign_in_in_one_circuit_does_not_authenticate_another_circuit()
    {
        await using var provider = BuildProvider();

        await using var circuitA = provider.CreateAsyncScope();
        await using var circuitB = provider.CreateAsyncScope();

        var authA = circuitA.ServiceProvider.GetRequiredService<IExplorerAuthSession>();
        var authB = circuitB.ServiceProvider.GetRequiredService<IExplorerAuthSession>();

        // The second circuit initialises from its own (empty) cookie first, so
        // the check proves the absence of a shared process-global sign-in rather
        // than mere ordering.
        await authB.InitializeAsync();

        await authA.LoginAsync("alice", "Password1");

        // ...and again after the first circuit has signed in.
        await authB.InitializeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(authA.IsAuthenticated, Is.True, "the signing-in circuit is authenticated");
            Assert.That(authA.Username, Is.EqualTo("alice"));
            Assert.That(authB.IsAuthenticated, Is.False, "a second circuit must not inherit the sign-in");
            Assert.That(authB.Username, Is.Null);
        });
    }

    [Test]
    public async Task Sign_in_credential_does_not_leak_to_another_circuit()
    {
        await using var provider = BuildProvider();

        await using var circuitA = provider.CreateAsyncScope();
        await using var circuitB = provider.CreateAsyncScope();

        var authA = circuitA.ServiceProvider.GetRequiredService<IExplorerAuthSession>();
        var authB = circuitB.ServiceProvider.GetRequiredService<IExplorerAuthSession>();

        await authA.LoginAsync("alice", "Password1");
        await authB.InitializeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(authA.CurrentAuthentication, Is.Not.Null, "the signing-in circuit carries a credential");
            Assert.That(authB.CurrentAuthentication, Is.Null, "the credential must not leak to another circuit");
        });
    }

    [Test]
    public async Task Sign_out_in_one_circuit_does_not_affect_another_signed_in_circuit()
    {
        await using var provider = BuildProvider();

        await using var circuitA = provider.CreateAsyncScope();
        await using var circuitB = provider.CreateAsyncScope();

        var authA = circuitA.ServiceProvider.GetRequiredService<IExplorerAuthSession>();
        var authB = circuitB.ServiceProvider.GetRequiredService<IExplorerAuthSession>();

        await authA.LoginAsync("alice", "Password1");
        await authB.LoginAsync("bob", "Password2");

        await authA.LogoutAsync();

        Assert.Multiple(() =>
        {
            Assert.That(authA.IsAuthenticated, Is.False, "the signing-out circuit is anonymous");
            Assert.That(authB.IsAuthenticated, Is.True, "an independent circuit stays signed in");
            Assert.That(authB.Username, Is.EqualTo("bob"));
        });
    }
}
