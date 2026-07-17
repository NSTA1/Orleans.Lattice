using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Explorer.Backup;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Explorer.Web;

namespace Orleans.Lattice.Explorer.Tests.Hosting;

/// <summary>
/// End-to-end smoke / regression test for the <em>assembled</em> explorer web
/// head. Every area (Explore, Backups, Access, Schema) has its own unit tests;
/// this fixture is the one place that asserts the whole assembled by a single
/// <see cref="LatticeExplorerWebServiceCollectionExtensions.AddLatticeExplorerWeb"/>
/// / <see cref="LatticeExplorerWebEndpointRouteBuilderExtensions.MapLatticeExplorer"/>
/// wiring: all four areas come up together, in order, each correctly
/// capability-gated, each area's capability seam resolves from the one provider
/// and fails closed when no endpoint is configured. A future area that forgets
/// its gate, or drops out of the assembled wiring, fails here.
/// </summary>
[TestFixture]
public class AssembledExplorerHostSmokeTests
{
    // ---- 1. Navigation: the assembled area switcher --------------------------

    [Test]
    public void Assembled_areas_project_to_explore_backups_access_schema_in_order()
    {
        var areas = AppAreas.Ordered.Select(a => a.Area).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(
                areas,
                Is.EqualTo(new[] { AppArea.Explore, AppArea.Backups, AppArea.Access, AppArea.Schema }));
            Assert.That(AppAreas.Default, Is.EqualTo(AppArea.Explore));
        });
    }

    // ---- 2. Capability-gating parity across ALL areas at once ----------------

    [Test]
    public void Under_empty_capabilities_only_explore_is_enabled()
    {
        var empty = ExplorerCapabilities.Empty;

        Assert.Multiple(() =>
        {
            Assert.That(AppAreas.IsEnabled(AppArea.Explore, empty), Is.True, "Explore is always on");
            Assert.That(AppAreas.IsEnabled(AppArea.Backups, empty), Is.False, "Backups must gate closed");
            Assert.That(AppAreas.IsEnabled(AppArea.Access, empty), Is.False, "Access must gate closed");
            Assert.That(AppAreas.IsEnabled(AppArea.Schema, empty), Is.False, "Schema must gate closed");
        });
    }

    [Test]
    public void Each_gated_area_flips_on_only_under_its_own_capability()
    {
        var backupOnly = ExplorerCapabilities.Empty with { BackupListAllowed = true };
        var accessOnly = ExplorerCapabilities.Empty with { AuthAdminAllowed = true };
        var schemaOnly = ExplorerCapabilities.Empty with { SchemaAllowed = true };

        Assert.Multiple(() =>
        {
            // Backups flips on under its own gate, and leaves the sibling areas closed.
            Assert.That(AppAreas.IsEnabled(AppArea.Backups, backupOnly), Is.True);
            Assert.That(AppAreas.IsEnabled(AppArea.Access, backupOnly), Is.False);
            Assert.That(AppAreas.IsEnabled(AppArea.Schema, backupOnly), Is.False);

            // Access flips on under its own gate, and leaves the sibling areas closed.
            Assert.That(AppAreas.IsEnabled(AppArea.Access, accessOnly), Is.True);
            Assert.That(AppAreas.IsEnabled(AppArea.Backups, accessOnly), Is.False);
            Assert.That(AppAreas.IsEnabled(AppArea.Schema, accessOnly), Is.False);

            // Schema flips on under its own gate, and leaves the sibling areas closed.
            Assert.That(AppAreas.IsEnabled(AppArea.Schema, schemaOnly), Is.True);
            Assert.That(AppAreas.IsEnabled(AppArea.Backups, schemaOnly), Is.False);
            Assert.That(AppAreas.IsEnabled(AppArea.Access, schemaOnly), Is.False);
        });
    }

    // ---- 3. Service assembly: every area's capability seam, from ONE provider

    [Test]
    public async Task Every_area_capability_service_resolves_from_the_assembled_provider()
    {
        await using var provider = BuildAssembledProvider();

        Assert.Multiple(() =>
        {
            Assert.That(provider.GetService<IExplorerCapabilityStore>(), Is.Not.Null);
            Assert.That(provider.GetService<IBackupCapabilityService>(), Is.Not.Null);
            Assert.That(provider.GetService<IAuthAdminCapabilityService>(), Is.Not.Null);
            Assert.That(provider.GetService<ISchemaAdminCapabilityService>(), Is.Not.Null);
        });
    }

    [Test]
    public async Task Every_area_capability_service_fails_closed_when_no_endpoint_is_configured()
    {
        await using var provider = BuildAssembledProvider();

        var store = provider.GetRequiredService<IExplorerCapabilityStore>();
        var backup = provider.GetRequiredService<IBackupCapabilityService>();
        var access = provider.GetRequiredService<IAuthAdminCapabilityService>();
        var schema = provider.GetRequiredService<ISchemaAdminCapabilityService>();

        // No endpoint is configured on the assembled provider, so each area's
        // coarse probe must fail closed (yield not-allowed) rather than throw.
        await backup.RefreshAsync();
        await access.RefreshAsync();
        await schema.RefreshAsync();

        var caps = store.Current;
        Assert.Multiple(() =>
        {
            Assert.That(caps.BackupListAllowed, Is.False, "Backups must fail closed with no endpoint");
            Assert.That(caps.AuthAdminAllowed, Is.False, "Access must fail closed with no endpoint");
            Assert.That(caps.SchemaAllowed, Is.False, "Schema must fail closed with no endpoint");

            // The published map must gate every non-Explore area off.
            Assert.That(AppAreas.IsEnabled(AppArea.Explore, caps), Is.True);
            Assert.That(AppAreas.IsEnabled(AppArea.Backups, caps), Is.False);
            Assert.That(AppAreas.IsEnabled(AppArea.Access, caps), Is.False);
            Assert.That(AppAreas.IsEnabled(AppArea.Schema, caps), Is.False);
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

            // Coherent: the mapped host resolves every assembled area's capability
            // seam from its live service provider.
            Assert.Multiple(() =>
            {
                Assert.That(app.Services.GetService<IExplorerCapabilityStore>(), Is.Not.Null, $"store missing (basePath={basePath})");
                Assert.That(app.Services.GetService<IBackupCapabilityService>(), Is.Not.Null, $"backups missing (basePath={basePath})");
                Assert.That(app.Services.GetService<IAuthAdminCapabilityService>(), Is.Not.Null, $"access missing (basePath={basePath})");
                Assert.That(app.Services.GetService<ISchemaAdminCapabilityService>(), Is.Not.Null, $"schema missing (basePath={basePath})");
            });

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

    private static ServiceProvider BuildAssembledProvider()
    {
        var services = new ServiceCollection();
        services.AddLatticeExplorerWeb();

        // Replace the real auth session with a substitute so resolving the area
        // clients never opens a gRPC channel; the fail-closed probe throws on the
        // unconfigured session before it ever reads the auth session.
        services.AddSingleton(Substitute.For<IExplorerAuthSession>());

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
