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
    private static ServiceProvider BuildProvider(IExplorerCredentialSeed? credentialSeed = null)
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
        if (credentialSeed is not null)
        {
            services.AddSingleton(credentialSeed);
        }

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

    [Test]
    public async Task An_anonymous_circuit_is_not_signed_in_by_a_withheld_credential_seed()
    {
        // Security regression: the launcher credential seed applied whenever the
        // credential store was empty, which in a multi-user head is true for every
        // anonymous visitor - so each of them was silently signed in with the
        // process-wide operator credential. A head that withholds the seed must
        // leave such a circuit anonymous. The paired test below shows the seed is
        // genuinely the mechanism, so this assertion is not vacuous.
        await using var provider = BuildProvider(new WithheldCredentialSeed());

        await using var circuit = provider.CreateAsyncScope();
        var auth = circuit.ServiceProvider.GetRequiredService<IExplorerAuthSession>();

        await auth.InitializeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(auth.IsAuthenticated, Is.False, "an anonymous visitor must stay anonymous");
            Assert.That(auth.CurrentAuthentication, Is.Null);
            Assert.That(auth.Username, Is.Null);
        });
    }

    [Test]
    public async Task An_anonymous_circuit_is_signed_in_by_a_present_credential_seed()
    {
        // The counterpart that proves the seed drives the sign-in whose absence the
        // test above asserts. A single-operator head may opt into this; a
        // multi-user head must not.
        await using var provider = BuildProvider(new SeededCredentialSeed("operator", "Password1"));

        await using var circuit = provider.CreateAsyncScope();
        var auth = circuit.ServiceProvider.GetRequiredService<IExplorerAuthSession>();

        await auth.InitializeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(auth.IsAuthenticated, Is.True);
            Assert.That(auth.Username, Is.EqualTo("operator"));
        });
    }

    private sealed class WithheldCredentialSeed : IExplorerCredentialSeed
    {
        public StoredCredential? TrySeed() => null;
    }

    private sealed class SeededCredentialSeed(string username, string password) : IExplorerCredentialSeed
    {
        public StoredCredential? TrySeed() => new(username, password);
    }

    // ---- the default store's own lifetime ---------------------------------

    /// <summary>
    /// Builds a provider exactly as a head that calls <see cref="AddExplorerAuth"/>
    /// and registers <b>no</b> credential store of its own - deliberately without
    /// the per-circuit override the other fixtures install, so what is exercised is
    /// the package default rather than the test's substitute for it.
    /// </summary>
    private static ServiceProvider BuildProviderOnPackageDefaults()
    {
        var services = new ServiceCollection();
        services.AddExplorerConfiguration(options =>
            options.FilePath = Path.Combine(Path.GetTempPath(), $"lattice-explorer-{Guid.NewGuid():N}.json"));
        services.AddExplorerCatalog();
        services.AddExplorerAuth();
        return services.BuildServiceProvider();
    }

    /// <summary>
    /// Security regression: the isolation this fixture pins must hold on
    /// <see cref="AddExplorerAuth"/>'s own default, not only when a test (or a
    /// head) happens to register a per-circuit store first.
    /// <para>
    /// The default was a <b>singleton</b> <see cref="InMemoryCredentialStore"/>,
    /// which holds one credential in a field - so it is a process-global sign-in.
    /// Every fixture above sidestepped it by registering a scoped store, which is
    /// precisely why the isolation suite passed while the shipped default did not
    /// isolate: any head that did not supply a platform store served one
    /// operator's credential to every browser's circuit.
    /// </para>
    /// </summary>
    [Test]
    public void The_default_credential_store_is_scoped_to_the_circuit()
    {
        var services = new ServiceCollection();
        services.AddExplorerAuth();

        var descriptor = services.Single(d => d.ServiceType == typeof(ICredentialStore));

        Assert.That(
            descriptor.Lifetime,
            Is.EqualTo(ServiceLifetime.Scoped),
            "a store holding one credential must not be process-global");
    }

    [Test]
    public async Task Sign_in_credential_does_not_leak_to_another_circuit_on_the_package_default_store()
    {
        await using var provider = BuildProviderOnPackageDefaults();

        await using var circuitA = provider.CreateAsyncScope();
        await using var circuitB = provider.CreateAsyncScope();

        var authA = circuitA.ServiceProvider.GetRequiredService<IExplorerAuthSession>();
        var authB = circuitB.ServiceProvider.GetRequiredService<IExplorerAuthSession>();

        // The second circuit reads its own store first, so a pass cannot be an
        // artefact of ordering.
        await authB.InitializeAsync();
        await authA.LoginAsync("alice", "Password1");
        await authB.InitializeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(
                circuitB.ServiceProvider.GetRequiredService<ICredentialStore>(),
                Is.Not.SameAs(circuitA.ServiceProvider.GetRequiredService<ICredentialStore>()),
                "each circuit must hold its own credential store");
            Assert.That(authA.IsAuthenticated, Is.True, "the signing-in circuit is authenticated");
            Assert.That(authB.IsAuthenticated, Is.False, "a second circuit must not inherit the sign-in");
            Assert.That(authB.CurrentAuthentication, Is.Null, "the credential must not leak to another circuit");
        });
    }

    /// <summary>
    /// The scoping must not displace a head-supplied store: the web head registers
    /// an encrypted-cookie store before calling <see cref="AddExplorerAuth"/>, and
    /// <c>TryAdd</c> matches on service type regardless of lifetime, so that
    /// registration still wins at whatever lifetime the head chose.
    /// </summary>
    [Test]
    public void A_head_supplied_credential_store_still_wins_over_the_scoped_default()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ICredentialStore, HeadSuppliedCredentialStore>();
        services.AddExplorerAuth();

        var descriptor = services.Single(d => d.ServiceType == typeof(ICredentialStore));

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.ImplementationType, Is.EqualTo(typeof(HeadSuppliedCredentialStore)));
            Assert.That(descriptor.Lifetime, Is.EqualTo(ServiceLifetime.Singleton));
        });
    }

    private sealed class HeadSuppliedCredentialStore : ICredentialStore
    {
        public Task<StoredCredential?> GetAsync(CancellationToken cancellationToken = default)
            => Task.FromResult<StoredCredential?>(null);

        public Task SetAsync(StoredCredential credential, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task ClearAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
    }
}
