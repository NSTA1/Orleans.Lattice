using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Explorer.Backup;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins.MyTenant;
using Orleans.Lattice.Explorer.Plugins.Telemetry;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Explorer.Plugins.Tenants;
using Orleans.Lattice.Explorer.Web;

namespace Orleans.Lattice.Explorer.Tests.Hosting;

/// <summary>
/// End-to-end smoke / regression test for the <em>assembled</em> explorer web
/// head. Every area has its own unit tests; this fixture is the one place that
/// asserts the whole assembled by a single
/// <see cref="LatticeExplorerWebServiceCollectionExtensions.AddLatticeExplorerWeb"/>
/// / <see cref="LatticeExplorerWebEndpointRouteBuilderExtensions.MapLatticeExplorer"/>
/// wiring: the head's chosen area plugins come up together, in order, each
/// carrying its own gate, each gate resolving from the one provider and failing
/// closed when no endpoint is configured. A future area that forgets its gate,
/// or drops out of the assembled wiring, fails here.
/// </summary>
[TestFixture]
public class AssembledExplorerHostSmokeTests
{
    // ---- 1. Navigation: the assembled area switcher --------------------------

    [Test]
    public async Task Assembled_area_plugins_project_to_backups_access_tenants_my_tenant_then_telemetry_in_order()
    {
        await using var provider = BuildAssembledProvider();
        await using var scope = provider.CreateAsyncScope();

        var ids = AreaPluginIds(scope.ServiceProvider);

        Assert.That(ids, Is.EqualTo(new[]
        {
            BackupsPluginKeys.PluginId,
            AccessPluginKeys.PluginId,
            TenantsPluginKeys.PluginId,
            MyTenantPluginKeys.PluginId,
            TelemetryPluginKeys.PluginId,
        }));
    }

    [Test]
    public async Task Assembled_web_head_supplies_the_tenants_area_a_real_operator_gate()
    {
        // The Tenants area is reserved to platform operators, and the tenant-view
        // seam registers a fail-closed operator gate with TryAdd. A head that
        // enabled tenant scoping before registering the surface that supplies the
        // real gate would ship an area nobody could ever open, so composing the
        // area at all refuses in that case - which makes this fixture booting the
        // proof that the assembled head has the order right.
        await using var provider = BuildAssembledProvider();
        await using var scope = provider.CreateAsyncScope();

        var gate = scope.ServiceProvider.GetService<IExplorerTenantOperatorGate>();

        Assert.That(gate, Is.Not.Null.And.TypeOf<AccessExplorerTenantOperatorGate>());
    }

    [Test]
    public async Task Assembled_area_plugins_all_declare_a_distinct_order()
    {
        // Two areas sharing an Order compiles, renders, and passes every
        // per-plugin test, because each plugin package only ever sees itself.
        // What it actually does is hand the relative position of two tabs to
        // whatever tie-break the catalogue's sort happens to use, so the strip
        // order stops being a design decision. Registering the schema plugin
        // too so the guard covers every area a head can compose at once.
        await using var provider = BuildAssembledProvider(
            registerPlugins: services => services.AddExplorerSchemaPlugin());
        await using var scope = provider.CreateAsyncScope();

        var byOrder = scope.ServiceProvider
            .GetRequiredService<IExplorerPluginCatalog>()
            .ForSurface(ExplorerPluginSurface.Area)
            .GroupBy(plugin => plugin.Descriptor.Order)
            .Where(group => group.Count() > 1)
            .Select(group => $"{group.Key}: {string.Join(", ", group.Select(p => p.Descriptor.PluginId))}")
            .ToArray();

        Assert.That(
            byOrder,
            Is.Empty,
            "every area plugin must claim its own Order so the tab strip is ordered by intent");
    }

    [Test]
    public async Task Assembled_web_head_registers_no_schema_plugin_by_default()
    {
        await using var provider = BuildAssembledProvider();
        await using var scope = provider.CreateAsyncScope();

        Assert.Multiple(() =>
        {
            Assert.That(
                AreaPluginIds(scope.ServiceProvider),
                Does.Not.Contain(SchemaPluginKeys.PluginId),
                "withholding an area is simply not registering its plugin");

            // Not registered is not deleted: the schema control services stay wired
            // so the head can surface the area by registering the plugin.
            Assert.That(scope.ServiceProvider.GetService<ISchemaAdminCapabilityService>(), Is.Not.Null);
        });
    }

    [Test]
    public async Task Assembled_web_head_registers_the_schema_plugin_when_the_head_opts_in()
    {
        await using var provider = BuildAssembledProvider(
            registerPlugins: services => services.AddExplorerSchemaPlugin());
        await using var scope = provider.CreateAsyncScope();

        Assert.That(
            AreaPluginIds(scope.ServiceProvider),
            Is.EqualTo(new[]
            {
                BackupsPluginKeys.PluginId,
                AccessPluginKeys.PluginId,
                SchemaPluginKeys.PluginId,
                TenantsPluginKeys.PluginId,
                MyTenantPluginKeys.PluginId,
                TelemetryPluginKeys.PluginId,
            }));
    }

    // ---- 2. Gating parity across ALL areas at once ---------------------------

    [Test]
    public async Task Before_any_probe_every_area_plugin_is_denied()
    {
        await using var provider = BuildAssembledProvider(
            registerPlugins: services => services.AddExplorerSchemaPlugin());
        await using var scope = provider.CreateAsyncScope();

        var store = scope.ServiceProvider.GetRequiredService<IExplorerPluginAccessStore>();
        var catalog = scope.ServiceProvider.GetRequiredService<IExplorerPluginCatalog>();

        Assert.Multiple(() =>
        {
            foreach (var plugin in catalog.ForSurface(ExplorerPluginSurface.Area))
            {
                var access = store.Get(plugin.Descriptor.PluginId);
                Assert.That(
                    access.IsAllowed,
                    Is.False,
                    $"{plugin.Descriptor.PluginId} must gate closed before it is probed");
                Assert.That(
                    access.IsVisible,
                    Is.True,
                    $"{plugin.Descriptor.PluginId} greys out rather than hides");
            }
        });
    }

    [Test]
    public async Task Each_area_plugin_carries_its_own_gate_and_no_two_share_one()
    {
        await using var provider = BuildAssembledProvider(
            registerPlugins: services => services.AddExplorerSchemaPlugin());
        await using var scope = provider.CreateAsyncScope();

        var plugins = scope.ServiceProvider
            .GetRequiredService<IExplorerPluginCatalog>()
            .ForSurface(ExplorerPluginSurface.Area);

        var gates = plugins.Select(plugin => plugin.AccessGate).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(gates, Has.None.Null);
            Assert.That(gates.Distinct().Count(), Is.EqualTo(gates.Length), "one gate per plugin");
        });
    }

    // ---- 3. Service assembly: every area's seam, from ONE provider ----------

    [Test]
    public async Task Every_area_capability_service_resolves_from_the_assembled_provider()
    {
        await using var provider = BuildAssembledProvider();
        await using var scope = provider.CreateAsyncScope();

        Assert.Multiple(() =>
        {
            Assert.That(scope.ServiceProvider.GetService<IExplorerPluginAccessStore>(), Is.Not.Null);
            Assert.That(scope.ServiceProvider.GetService<IExplorerPluginCatalog>(), Is.Not.Null);
            Assert.That(scope.ServiceProvider.GetService<IExplorerPluginAccessRefresher>(), Is.Not.Null);
            Assert.That(scope.ServiceProvider.GetService<IExplorerPluginHostState>(), Is.Not.Null);
            Assert.That(scope.ServiceProvider.GetService<IExplorerPluginPreferences>(), Is.Not.Null);
            Assert.That(scope.ServiceProvider.GetService<IBackupCapabilityService>(), Is.Not.Null);
            Assert.That(scope.ServiceProvider.GetService<IAuthAdminCapabilityService>(), Is.Not.Null);
            Assert.That(scope.ServiceProvider.GetService<ISchemaAdminCapabilityService>(), Is.Not.Null);
        });
    }

    [Test]
    public async Task The_shared_refresh_fails_every_gate_closed_when_no_endpoint_is_configured()
    {
        await using var provider = BuildAssembledProvider(
            registerPlugins: services => services.AddExplorerSchemaPlugin());
        await using var scope = provider.CreateAsyncScope();

        var store = scope.ServiceProvider.GetRequiredService<IExplorerPluginAccessStore>();
        var refresher = scope.ServiceProvider.GetRequiredService<IExplorerPluginAccessRefresher>();

        // No endpoint is configured on the assembled provider, so each area's
        // gate must fail closed (yield not-allowed) rather than throw, and the
        // shared refresh must contain every one of those faults.
        await refresher.RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(
                store.Get(BackupsPluginKeys.PluginId).IsAllowed,
                Is.False,
                "Backups must fail closed with no endpoint");
            Assert.That(
                store.Get(AccessPluginKeys.PluginId).IsAllowed,
                Is.False,
                "Access must fail closed with no endpoint");
            Assert.That(
                store.Get(SchemaPluginKeys.PluginId).IsAllowed,
                Is.False,
                "Schema must fail closed with no endpoint");
            Assert.That(
                store.Get(MyTenantPluginKeys.PluginId).IsAllowed,
                Is.False,
                "My Tenant must fail closed with no endpoint");
            Assert.That(
                store.Get(TelemetryPluginKeys.PluginId).IsAllowed,
                Is.False,
                "Telemetry must fail closed with no endpoint");
        });
    }

    // ---- 4. Endpoint mapping: the assembled host boots and maps -------------

    [Test]
    [Category("Integration")]
    public async Task Assembled_host_maps_and_all_area_seams_resolve_at_root_and_under_a_base_path()
    {
        foreach (var basePath in new string?[] { null, "/explorer" })
        {
            await using var app = await CreateHostAsync(basePath);

            // Coherent: the mapped host resolves every assembled area's seam from
            // its live service provider.
            await using (var scope = app.Services.CreateAsyncScope())
            {
                Assert.Multiple(() =>
                {
                    Assert.That(scope.ServiceProvider.GetService<IExplorerPluginAccessStore>(), Is.Not.Null, $"store missing (basePath={basePath})");
                    Assert.That(scope.ServiceProvider.GetService<IExplorerPluginCatalog>(), Is.Not.Null, $"catalog missing (basePath={basePath})");
                    Assert.That(scope.ServiceProvider.GetService<IBackupCapabilityService>(), Is.Not.Null, $"backups missing (basePath={basePath})");
                    Assert.That(scope.ServiceProvider.GetService<IAuthAdminCapabilityService>(), Is.Not.Null, $"access missing (basePath={basePath})");
                    Assert.That(scope.ServiceProvider.GetService<ISchemaAdminCapabilityService>(), Is.Not.Null, $"schema missing (basePath={basePath})");
                });
            }

            // Mapped: the server-side auth endpoint lands under the configured base
            // path (a missing antiforgery token rejects with 400, not 404).
            using var client = app.GetTestServer().CreateClient();
            var prefix = basePath ?? string.Empty;
            var response = await client.PostAsync($"{prefix}/auth/login", EmptyForm());
            Assert.That(
                response.StatusCode,
                Is.EqualTo(HttpStatusCode.BadRequest),
                $"auth endpoint should be mapped (basePath={basePath})");
        }
    }

    // ---- harness ------------------------------------------------------------

    private static string[] AreaPluginIds(IServiceProvider provider) => provider
        .GetRequiredService<IExplorerPluginCatalog>()
        .ForSurface(ExplorerPluginSurface.Area)
        .Select(plugin => plugin.Descriptor.PluginId)
        .ToArray();

    private static ServiceProvider BuildAssembledProvider(
        Action<LatticeExplorerWebOptions>? configure = null,
        Action<IServiceCollection>? registerPlugins = null)
    {
        var services = new ServiceCollection();
        services.AddLatticeExplorerWeb(configure);

        // An area that ships as its own plugin package (Schema) is opted in by the
        // head registering it, which is the whole of the opt-in now that the
        // per-area option flag is retired.
        registerPlugins?.Invoke(services);

        // Replace the real auth session with a substitute so resolving the area
        // clients never opens a gRPC channel; the fail-closed probe throws on the
        // unconfigured session before it ever reads the auth session. The durable
        // preference store is substituted for the same reason: its web-head
        // backing store is a Blazor circuit service that only exists inside a
        // real web host.
        services.AddSingleton(Substitute.For<IExplorerAuthSession>());
        services.AddScoped(_ => Substitute.For<IUiPreferenceStore>());

        return services.BuildServiceProvider();
    }

    private static FormUrlEncodedContent EmptyForm() =>
        new(new Dictionary<string, string>());

    private static async Task<WebApplication> CreateHostAsync(string? basePath)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddLatticeExplorerWeb(options =>
        {
            if (basePath is not null)
            {
                options.BasePath = basePath;
            }
        });
        builder.Services.AddSingleton(Substitute.For<IExplorerAuthSession>());

        var app = builder.Build();
        app.UseAntiforgery();
        app.MapLatticeExplorer();

        await app.StartAsync();
        return app;
    }
}
