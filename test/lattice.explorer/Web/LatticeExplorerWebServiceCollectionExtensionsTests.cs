using Microsoft.AspNetCore.Components.Server;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Shell.Session;
using Orleans.Lattice.Explorer.Web;

namespace Orleans.Lattice.Explorer.Tests.Web;

/// <summary>
/// Unit tests for <see cref="LatticeExplorerWebServiceCollectionExtensions"/>:
/// the single opt-in <c>AddLatticeExplorerWeb</c> call must register everything
/// the standalone web head wires up, and honour its options.
/// </summary>
[TestFixture]
public class LatticeExplorerWebServiceCollectionExtensionsTests
{
    [Test]
    public void AddLatticeExplorerWeb_null_services_throws()
    {
        Assert.That(
            () => ((IServiceCollection)null!).AddLatticeExplorerWeb(),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeExplorerWeb_registers_the_options_singleton()
    {
        var services = new ServiceCollection();

        services.AddLatticeExplorerWeb(o => o.BasePath = "/explorer");

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<LatticeExplorerWebOptions>();
        Assert.That(options.BasePath, Is.EqualTo("/explorer"));
    }

    [Test]
    public void AddLatticeExplorerWeb_registers_the_expected_services()
    {
        var services = new ServiceCollection();

        services.AddLatticeExplorerWeb();

        Assert.Multiple(() =>
        {
            Assert.That(services.Any(d => d.ServiceType == typeof(LatticeExplorerWebOptions)), Is.True);
            Assert.That(services.Any(d => d.ServiceType == typeof(IExplorerConfigStore)), Is.True);
            Assert.That(services.Any(d => d.ServiceType == typeof(IExplorerSession)), Is.True);
            Assert.That(services.Any(d => d.ServiceType == typeof(ICredentialStore)), Is.True);
            Assert.That(services.Any(d => d.ServiceType == typeof(SessionSignInOptions)), Is.True);
            Assert.That(services.Any(d => d.ServiceType == typeof(IUiPreferenceBackingStore)), Is.True);
        });
    }

    [Test]
    public void AddLatticeExplorerWeb_preference_store_is_the_protected_local_storage_backing_store()
    {
        var services = new ServiceCollection();

        services.AddLatticeExplorerWeb();

        var descriptor = services.Last(d => d.ServiceType == typeof(IUiPreferenceBackingStore));
        Assert.Multiple(() =>
        {
            Assert.That(descriptor.Lifetime, Is.EqualTo(ServiceLifetime.Scoped));
            Assert.That(descriptor.ImplementationType, Is.EqualTo(typeof(ProtectedLocalStoragePreferenceBackingStore)));
        });
    }

    [Test]
    public void AddLatticeExplorerWeb_credential_store_is_the_cookie_store()
    {
        var services = new ServiceCollection();

        services.AddLatticeExplorerWeb();

        var descriptor = services.Single(d => d.ServiceType == typeof(ICredentialStore));
        Assert.That(descriptor.ImplementationType, Is.EqualTo(typeof(CookieCredentialStore)));
    }

    [Test]
    public void AddLatticeExplorerWeb_defaults_auth_ui_to_root_relative_server_form_post()
    {
        var services = new ServiceCollection();

        services.AddLatticeExplorerWeb();

        using var provider = services.BuildServiceProvider();
        var authUi = provider.GetRequiredService<SessionSignInOptions>();
        Assert.Multiple(() =>
        {
            Assert.That(authUi.UseServerFormPost, Is.True);
            Assert.That(authUi.LoginPath, Is.EqualTo("/auth/login"));
            Assert.That(authUi.LogoutPath, Is.EqualTo("/auth/logout"));
        });
    }

    [Test]
    public void AddLatticeExplorerWeb_prefixes_auth_ui_paths_with_the_base_path()
    {
        var services = new ServiceCollection();

        services.AddLatticeExplorerWeb(o => o.BasePath = "/explorer");

        using var provider = services.BuildServiceProvider();
        var authUi = provider.GetRequiredService<SessionSignInOptions>();
        Assert.Multiple(() =>
        {
            Assert.That(authUi.LoginPath, Is.EqualTo("/explorer/auth/login"));
            Assert.That(authUi.LogoutPath, Is.EqualTo("/explorer/auth/logout"));
        });
    }

    [Test]
    public void AddLatticeExplorerWeb_registers_the_server_side_blazor_state()
    {
        var services = new ServiceCollection();

        services.AddLatticeExplorerWeb();

        // AddInteractiveServerComponents brings in the circuit options, which is a
        // reliable, framework-owned marker that the interactive server host was
        // registered (as opposed to a bare AddRazorComponents call).
        Assert.That(
            services.Any(d => d.ServiceType == typeof(Microsoft.Extensions.Options.IConfigureOptions<CircuitOptions>)),
            Is.True);
    }

    [Test]
    public void AddLatticeExplorerWeb_raises_the_circuit_receive_limit_for_frame_requests()
    {
        var services = new ServiceCollection();

        services.AddLatticeExplorerWeb();

        using var provider = services.BuildServiceProvider();
        // The circuit hub type is internal to the framework, so its options type is built by name.
        var componentHub = typeof(CircuitOptions).Assembly.GetType("Microsoft.AspNetCore.Components.Server.ComponentHub", throwOnError: true)!;
        var optionsType = typeof(Microsoft.Extensions.Options.IOptions<>).MakeGenericType(typeof(Microsoft.AspNetCore.SignalR.HubOptions<>).MakeGenericType(componentHub));
        var hub = (Microsoft.AspNetCore.SignalR.HubOptions)optionsType.GetProperty("Value")!.GetValue(provider.GetRequiredService(optionsType))!;
        Assert.Multiple(() =>
        {
            Assert.That(LatticeExplorerWebServiceCollectionExtensions.MinimumCircuitMessageBytes, Is.GreaterThanOrEqualTo(160 * 1024));
            Assert.That(hub.MaximumReceiveMessageSize, Is.GreaterThanOrEqualTo(LatticeExplorerWebServiceCollectionExtensions.MinimumCircuitMessageBytes));
        });
    }

    [Test]
    public void AddLatticeExplorerWeb_registers_the_shell_areas_and_the_app_frame()
    {
        var services = new ServiceCollection();

        services.AddLatticeExplorerWeb();

        var areas = services
            .Where(d => d.ServiceType == typeof(Orleans.Lattice.Explorer.Shell.Navigation.IExplorerArea))
            .Select(d => d.ImplementationType?.Name)
            .ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(areas, Is.EquivalentTo(new[]
            {
                "DataArea", "AppsArea", "AccessArea", "SchemaArea", "TenancyArea",
                "ReplicationArea", "BackupsArea", "TelemetryArea", "ClusterArea",
            }));
            Assert.That(services.Any(d => d.ServiceType == typeof(Orleans.Lattice.Explorer.Shell.Framing.Broker.AppBridgeBroker)), Is.True);
        });
    }

    [Test]
    public void AddLatticeExplorerWeb_registers_the_app_bridge_client_scoped_over_the_circuit()
    {
        var services = new ServiceCollection();

        services.AddLatticeExplorerWeb();

        var descriptor = services.Single(d => d.ServiceType == typeof(Orleans.Lattice.Api.Apps.ILatticeAppBridge));
        Assert.Multiple(() =>
        {
            Assert.That(descriptor.Lifetime, Is.EqualTo(ServiceLifetime.Scoped));
            Assert.That(descriptor.ImplementationFactory, Is.Not.Null);
        });
    }

    [Test]
    public void AddLatticeExplorerWeb_lets_the_shell_tenancy_seams_win_over_cores_fallbacks()
    {
        var services = new ServiceCollection();

        services.AddLatticeExplorerWeb();

        var gate = services.Single(d => d.ServiceType == typeof(Orleans.Lattice.Explorer.Core.Tenancy.IExplorerTenantOperatorGate));
        var tenants = services.Single(d => d.ServiceType == typeof(Orleans.Lattice.Explorer.Core.Tenancy.IExplorerAccessibleTenantSource));
        Assert.Multiple(() =>
        {
            Assert.That(gate.ImplementationType, Is.EqualTo(typeof(Orleans.Lattice.Explorer.Shell.Navigation.ShellTenantOperatorGate)));
            Assert.That(gate.Lifetime, Is.EqualTo(ServiceLifetime.Scoped));
            Assert.That(tenants.ImplementationFactory, Is.Not.Null, "the Tenancy area's list is registered by factory");
            Assert.That(services.Any(d => d.ServiceType == typeof(Orleans.Lattice.Explorer.Core.Tenancy.IExplorerTenantView)), Is.True);
        });
    }

    [Test]
    public void AddLatticeExplorerWeb_registers_no_singleton_that_captures_circuit_scoped_state()
    {
        var services = new ServiceCollection();
        services.AddLatticeExplorerWeb();

        var scoped = services
            .Where(d => d.Lifetime == ServiceLifetime.Scoped)
            .Select(d => d.ServiceType)
            .ToHashSet();
        var captives = services
            .Where(d => d.Lifetime == ServiceLifetime.Singleton && d.ImplementationType is { IsGenericTypeDefinition: false } && d.ImplementationType.Assembly.GetName().Name!.StartsWith("Orleans.Lattice.Explorer", StringComparison.Ordinal))
            .SelectMany(d => d.ImplementationType!.GetConstructors().SelectMany(c => c.GetParameters())
                .Where(p => scoped.Contains(p.ParameterType))
                .Select(p => $"{d.ImplementationType!.Name} -> {p.ParameterType.Name}"))
            .ToArray();

        Assert.That(captives, Is.Empty);
    }

    [Test]
    public void AddLatticeExplorerWeb_registers_data_protection_by_default()
    {
        var services = new ServiceCollection();

        services.AddLatticeExplorerWeb();

        using var provider = services.BuildServiceProvider();
        Assert.That(provider.GetService<IDataProtectionProvider>(), Is.Not.Null);
    }

    [Test]
    public void AddLatticeExplorerWeb_blob_key_ring_without_credential_throws()
    {
        var services = new ServiceCollection();

        Assert.That(
            () => services.AddLatticeExplorerWeb(o =>
                o.DataProtectionKeyRingBlobUri = new Uri("https://estate.blob.core.windows.net/keys/keyring.xml")),
            Throws.InvalidOperationException.With.Message.Contains(nameof(LatticeExplorerWebOptions.DataProtectionKeyRingCredential)));
    }

    [Test]
    public void AddLatticeExplorerWeb_shared_key_ring_builds_a_data_protection_provider()
    {
        var services = new ServiceCollection();

        services.AddLatticeExplorerWeb(o =>
        {
            o.DataProtectionKeyRingBlobUri = new Uri("https://estate.blob.core.windows.net/keys/keyring.xml");
            o.DataProtectionKeyRingCredential = new FakeTokenCredential();
            o.DataProtectionApplicationName = "lattice-explorer";
        });

        using var provider = services.BuildServiceProvider();
        Assert.That(provider.GetService<IDataProtectionProvider>(), Is.Not.Null);
    }

    [Test]
    public void AddLatticeExplorerWeb_invokes_the_configure_data_protection_hook()
    {
        var services = new ServiceCollection();
        var invoked = false;

        services.AddLatticeExplorerWeb(o => o.ConfigureDataProtection = _ => invoked = true);

        Assert.That(invoked, Is.True);
    }

    private sealed class FakeTokenCredential : Azure.Core.TokenCredential
    {
        public override Azure.Core.AccessToken GetToken(Azure.Core.TokenRequestContext requestContext, CancellationToken cancellationToken)
            => new("fake-token", DateTimeOffset.UtcNow.AddHours(1));

        public override ValueTask<Azure.Core.AccessToken> GetTokenAsync(Azure.Core.TokenRequestContext requestContext, CancellationToken cancellationToken)
            => new(GetToken(requestContext, cancellationToken));
    }
}
