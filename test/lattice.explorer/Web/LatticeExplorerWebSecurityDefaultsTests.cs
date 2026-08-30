using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Web;

namespace Orleans.Lattice.Explorer.Tests.Web;

/// <summary>
/// Security regressions for the two seams the multi-user web head must close that
/// the single-operator desktop head may leave open.
/// </summary>
/// <remarks>
/// <para>
/// First, the launcher credential seed. The web head's credential store is per
/// browser, so it is empty for every anonymous visitor; seeding the process-wide
/// <c>LATTICE_EXPLORER_USERNAME</c> / <c>LATTICE_EXPLORER_PASSWORD</c> there signed
/// each of them in with the operator's full cluster authority, re-globalising the
/// per-circuit auth state <see cref="Authentication.ExplorerAuthCircuitIsolationTests"/>
/// pins. The web head now withholds the credential seed unless explicitly opted in,
/// while keeping the secret-free endpoint seed.
/// </para>
/// <para>
/// Second, the shared configuration store. It is one process-wide document naming
/// the endpoint every circuit dials and every sign-in is challenged against, and
/// nothing upstream authenticates the caller who writes it, so an anonymous visitor
/// could repoint the head at a host they control. The web head's store now refuses
/// writes unless explicitly opted in; reads are untouched.
/// </para>
/// </remarks>
[TestFixture]
public class LatticeExplorerWebSecurityDefaultsTests
{
    private static readonly ExplorerConfiguration AnyConfiguration = new()
    {
        Endpoint = "https://attacker.example.com",
        TransportMode = ExplorerTransportMode.Secure,
    };

    private static ServiceProvider BuildWebProvider(Action<LatticeExplorerWebOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddLatticeExplorerWeb(options =>
        {
            // A path that does not exist, so no real document is read or written.
            options.ConfigFilePath = Path.Combine(Path.GetTempPath(), $"lattice-explorer-{Guid.NewGuid():N}.json");
            configure?.Invoke(options);
        });
        return services.BuildServiceProvider();
    }

    [Test]
    public async Task Web_head_withholds_the_environment_credential_seed_by_default()
    {
        await using var provider = BuildWebProvider();

        var seed = provider.GetRequiredService<IExplorerCredentialSeed>();

        Assert.Multiple(() =>
        {
            Assert.That(seed, Is.InstanceOf<NullExplorerCredentialSeed>());
            Assert.That(
                seed.TrySeed(),
                Is.Null,
                "an anonymous circuit must never be handed the process-wide operator credential");
        });
    }

    [Test]
    public async Task Web_head_keeps_the_endpoint_seed_when_the_credential_seed_is_withheld()
    {
        // The endpoint seed carries no secret and is the head's out-of-band
        // configuration channel, so withholding the credential seed must not take
        // it with it.
        await using var provider = BuildWebProvider();

        var configurationSeed = provider.GetService<IExplorerConfigurationSeed>();

        Assert.That(configurationSeed, Is.InstanceOf<EnvironmentExplorerBootstrap>());
    }

    [Test]
    public async Task Web_head_honours_an_explicit_environment_credential_seed_opt_in()
    {
        await using var provider = BuildWebProvider(options => options.AllowEnvironmentCredentialSeed = true);

        var seed = provider.GetRequiredService<IExplorerCredentialSeed>();

        Assert.That(seed, Is.InstanceOf<EnvironmentExplorerBootstrap>());
    }

    [Test]
    public async Task Web_head_credential_seed_stays_withheld_when_the_bootstrap_is_off()
    {
        await using var provider = BuildWebProvider(options => options.UseEnvironmentBootstrap = false);

        Assert.That(provider.GetRequiredService<IExplorerCredentialSeed>().TrySeed(), Is.Null);
    }

    [Test]
    public async Task Web_head_configuration_store_refuses_a_browser_driven_write_by_default()
    {
        await using var provider = BuildWebProvider();

        var store = provider.GetRequiredService<IExplorerConfigStore>();

        Assert.Multiple(() =>
        {
            Assert.That(store, Is.InstanceOf<ReadOnlyExplorerConfigStore>());
            Assert.That(
                async () => await store.SaveAsync(AnyConfiguration),
                Throws.InvalidOperationException,
                "an unauthenticated visitor must not be able to repoint the shared head");
        });
    }

    [Test]
    public async Task Web_head_configuration_store_still_reads_the_persisted_document()
    {
        // Refusing writes must not break the read path the environment and
        // pre-provisioned document channels depend on.
        var path = Path.Combine(Path.GetTempPath(), $"lattice-explorer-{Guid.NewGuid():N}.json");
        try
        {
            var seedStore = new JsonExplorerConfigStore(new ExplorerConfigStoreOptions { FilePath = path });
            await seedStore.SaveAsync(new ExplorerConfiguration
            {
                Endpoint = "https://cluster.example.com",
                TransportMode = ExplorerTransportMode.Secure,
            });

            await using var provider = BuildWebProvider(options => options.ConfigFilePath = path);
            var store = provider.GetRequiredService<IExplorerConfigStore>();

            var loaded = await store.LoadAsync();

            Assert.Multiple(() =>
            {
                Assert.That(store.Exists, Is.True);
                Assert.That(store.FilePath, Is.EqualTo(path));
                Assert.That(loaded, Is.Not.Null);
                Assert.That(loaded!.Endpoint, Is.EqualTo("https://cluster.example.com"));
            });
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Test]
    public async Task Web_head_session_apply_is_refused_and_persists_nothing_by_default()
    {
        // End to end through the seam the settings dialog actually calls: the
        // scoped session's ApplyAsync must not reach the shared document.
        await using var provider = BuildWebProvider();
        await using var circuit = provider.CreateAsyncScope();

        var session = circuit.ServiceProvider.GetRequiredService<IExplorerSession>();
        var store = provider.GetRequiredService<IExplorerConfigStore>();

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await session.ApplyAsync(AnyConfiguration),
                Throws.InvalidOperationException);
            Assert.That(store.Exists, Is.False, "nothing may be written to the shared document");
        });
    }

    [Test]
    public async Task Web_head_honours_an_explicit_interactive_configuration_opt_in()
    {
        await using var provider = BuildWebProvider(options => options.AllowInteractiveEndpointConfiguration = true);

        var store = provider.GetRequiredService<IExplorerConfigStore>();

        Assert.That(store, Is.InstanceOf<JsonExplorerConfigStore>());
    }

    [Test]
    public async Task Web_head_withholds_the_connection_settings_affordance_when_writes_are_refused()
    {
        await using var refusing = BuildWebProvider();
        await using var allowing = BuildWebProvider(options => options.AllowInteractiveEndpointConfiguration = true);

        Assert.Multiple(() =>
        {
            Assert.That(
                refusing.GetRequiredService<ExplorerNavigationOptions>().AllowEndpointConfiguration,
                Is.False);
            Assert.That(
                allowing.GetRequiredService<ExplorerNavigationOptions>().AllowEndpointConfiguration,
                Is.True);
        });
    }

    [Test]
    public void Read_only_store_rejects_a_null_inner_store()
        => Assert.That(() => new ReadOnlyExplorerConfigStore(null!), Throws.ArgumentNullException);

    [Test]
    public void Read_only_store_rejects_a_null_configuration()
    {
        var store = new ReadOnlyExplorerConfigStore(Substitute.For<IExplorerConfigStore>());

        Assert.That(async () => await store.SaveAsync(null!), Throws.ArgumentNullException);
    }
}
