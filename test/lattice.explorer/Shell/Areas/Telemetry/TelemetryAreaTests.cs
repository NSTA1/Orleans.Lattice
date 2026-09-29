using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Shell;
using Orleans.Lattice.Explorer.Shell.Areas.Telemetry;
using Orleans.Lattice.Explorer.Shell.Design;
using Orleans.Lattice.Explorer.Shell.Navigation;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;
using Orleans.Lattice.Explorer.Shell.Transport;
using Orleans.Lattice.Explorer.Tests.Shell.Design;
using Orleans.Lattice.Explorer.Tests.Shell.Session;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Telemetry;

/// <summary>
/// The Telemetry area's contract with the chrome: its stop, its fail-closed
/// availability over one shared catalogue read, its Home status, its completions and
/// command, its registration and its stylesheet asset.
/// </summary>
[TestFixture]
public sealed class TelemetryAreaTests
{
    [Test]
    public void The_area_is_the_telemetry_stop_at_the_eighth_position()
    {
        var (area, _, _) = Create();

        Assert.Multiple(() =>
        {
            Assert.That(area.Key, Is.EqualTo("telemetry"));
            Assert.That(area.DisplayName, Is.EqualTo("Telemetry"));
            Assert.That(area.DirectoryOrder, Is.EqualTo(80));
            Assert.That(((IExplorerArea)area).IsTenantScoped, Is.True, "under tenancy the area is rooted at the active tenant");
            Assert.That(area.Completions, Is.InstanceOf<TelemetryCompletionSource>());
        });
    }

    [Test]
    public async Task A_catalogue_read_makes_the_area_visible_even_when_it_is_empty()
    {
        var (area, telemetry, _) = Create();
        telemetry.Catalog = TelemetryQueryCatalog.Empty;

        Assert.Multiple(async () =>
        {
            Assert.That(await area.GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Visible));
            Assert.That(await area.GetHomeStatusAsync(CancellationToken.None), Is.EqualTo("No metrics are available to you."));
        });
    }

    [Test]
    public async Task A_refused_unserved_or_unreachable_catalogue_hides_the_area()
    {
        foreach (var failure in new Exception[]
        {
            new UnauthorizedAccessException("denied"),
            new NotSupportedException("not served"),
            new ShellTransportException("down", isTransient: true, new InvalidOperationException()),
            new InvalidOperationException("not configured"),
        })
        {
            var (area, telemetry, _) = Create();
            telemetry.CatalogFailure = failure;

            Assert.That(await area.GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Hidden), failure.GetType().Name);
        }
    }

    [Test]
    public async Task Probes_share_one_read_until_the_sign_in_or_connection_changes()
    {
        var auth = new FakeAuthSession();
        var session = new FakeExplorerSession(new FakeStateConnection());
        var (area, telemetry, cache) = Create(auth, session);

        await area.GetAvailabilityAsync(CancellationToken.None);
        await area.GetAvailabilityAsync(CancellationToken.None);
        await area.GetHomeStatusAsync(CancellationToken.None);
        Assert.That(telemetry.CatalogReads, Is.EqualTo(1));

        auth.SignIn("dana");
        await area.GetAvailabilityAsync(CancellationToken.None);
        Assert.That(telemetry.CatalogReads, Is.EqualTo(2));

        await session.ApplyAsync(SessionTestContext.RemoteConfiguration());
        await area.GetAvailabilityAsync(CancellationToken.None);
        Assert.That(telemetry.CatalogReads, Is.EqualTo(3));

        cache.Dispose();
        Assert.Multiple(() =>
        {
            Assert.That(auth.AuthenticationSubscribers, Is.Zero);
            Assert.That(session.ConfigurationSubscribers, Is.Zero);
        });
    }

    [Test]
    public async Task A_fault_is_remembered_too_so_a_denied_caller_costs_one_read()
    {
        var (area, telemetry, cache) = Create();
        telemetry.CatalogFailure = new UnauthorizedAccessException("denied");

        await area.GetAvailabilityAsync(CancellationToken.None);
        await area.GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(telemetry.CatalogReads, Is.EqualTo(1));
            Assert.That(cache.Current, Is.Null);
        });
    }

    [Test]
    public async Task A_cancelled_probe_abandons_only_its_own_wait()
    {
        var (area, telemetry, cache) = Create();
        telemetry.PendingCatalog = new TaskCompletionSource<TelemetryQueryCatalog>();
        using var cancel = new CancellationTokenSource();

        var probe = area.GetAvailabilityAsync(cancel.Token).AsTask();
        cancel.Cancel();

        Assert.That(async () => await probe, Throws.InstanceOf<OperationCanceledException>());

        telemetry.PendingCatalog.SetResult(TelemetryTestData.FullCatalog());
        Assert.That(await cache.GetAsync(), Is.Not.Null, "the shared read carried on");
        Assert.That(telemetry.CatalogReads, Is.EqualTo(1), "and the next caller joined it rather than asking again");
    }

    [Test]
    public async Task The_home_status_counts_the_charts_and_boards_the_caller_can_read()
    {
        var (area, telemetry, _) = Create();
        Assert.That(await area.GetHomeStatusAsync(CancellationToken.None), Is.EqualTo("13 charts on 4 boards."));

        var (single, narrowed, _) = Create();
        narrowed.Catalog = TelemetryTestData.CatalogOf(TelemetryTestData.Range("tree.read.operation_rate", "Read operations"));
        Assert.That(await single.GetHomeStatusAsync(CancellationToken.None), Is.EqualTo("1 chart on 1 board."));
        Assert.That(telemetry.CatalogReads, Is.EqualTo(1));
    }

    [Test]
    public async Task Free_text_completes_to_boards_and_to_the_board_holding_a_chart()
    {
        var (area, _, _) = Create();
        var source = area.Completions!;

        var latency = await source.CompleteAsync(new AddressQuery("latency", AddressQueryMode.Search, ExplorerAddress.Home), CancellationToken.None);
        var tombstones = await source.CompleteAsync(new AddressQuery("tombstones", AddressQueryMode.Search, ExplorerAddress.Home), CancellationToken.None);
        var address = await source.CompleteAsync(new AddressQuery("/telemetry", AddressQueryMode.Address, ExplorerAddress.Home), CancellationToken.None);
        var blank = await source.CompleteAsync(new AddressQuery("  ", AddressQueryMode.Search, ExplorerAddress.Home), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(latency[0].Label, Is.EqualTo("telemetry/latency"));
            Assert.That(latency[0].Target, Is.EqualTo(ExplorerAddress.ForArea("telemetry", "latency")));
            Assert.That(latency[0].Detail, Does.StartWith("Latency board"));
            Assert.That(latency.Skip(1).Select(completion => completion.Detail), Is.All.EndsWith("on the Latency board"));
            Assert.That(tombstones.Select(completion => completion.Target.Path[0]), Is.All.EqualTo("storage"));
            Assert.That(tombstones, Has.Count.EqualTo(2));
            Assert.That(address, Is.Empty);
            Assert.That(blank, Is.Empty);
        });
    }

    [Test]
    public async Task Completions_never_exceed_the_limit()
    {
        var (area, telemetry, _) = Create();
        telemetry.Catalog = TelemetryTestData.CatalogOf([.. Enumerable.Range(0, 40).Select(index => TelemetryTestData.Range($"host.q{index}", $"Custom {index}"))]);

        var results = await area.Completions!.CompleteAsync(new AddressQuery("custom", AddressQueryMode.Search, ExplorerAddress.Home), CancellationToken.None);

        Assert.That(results, Has.Count.EqualTo(AddressQuery.MaximumResults));
    }

    [Test]
    public async Task The_refresh_command_targets_the_area_and_drops_the_shared_read()
    {
        var (area, telemetry, cache) = Create();
        await cache.GetAsync();
        var changed = 0;
        cache.Changed += () => changed++;

        var command = area.Commands.Single();
        await command.InvokeAsync!.Invoke(CancellationToken.None);
        await cache.GetAsync();

        Assert.Multiple(() =>
        {
            Assert.That(command.Id, Is.EqualTo(TelemetryArea.RefreshCommandId));
            Assert.That(ExplorerCommand.IsValidId(command.Id), Is.True);
            Assert.That(command.Target, Is.EqualTo(ExplorerAddress.ForArea("telemetry")));
            Assert.That(changed, Is.EqualTo(1));
            Assert.That(telemetry.CatalogReads, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task The_shell_registers_the_area_once_per_circuit_and_it_resolves_with_the_real_transport()
    {
        var services = new ServiceCollection()
            .AddLogging()
            .AddScoped<IExplorerSession>(_ => new FakeExplorerSession(new FakeStateConnection()))
            .AddScoped<IExplorerAuthSession, FakeAuthSession>()
            .AddLatticeExplorerShell()
            .AddLatticeExplorerShell();

        Assert.Multiple(() =>
        {
            Assert.That(services.Count(descriptor => descriptor.ImplementationType == typeof(TelemetryArea)), Is.EqualTo(1));
            Assert.That(services.Single(descriptor => descriptor.ImplementationType == typeof(TelemetryArea)).Lifetime, Is.EqualTo(ServiceLifetime.Scoped));
            Assert.That(services.Single(descriptor => descriptor.ServiceType == typeof(TelemetryCatalogCache)).Lifetime, Is.EqualTo(ServiceLifetime.Scoped));
        });

        using var provider = services.BuildServiceProvider(validateScopes: true);
        using var scope = provider.CreateScope();
        var cache = scope.ServiceProvider.GetRequiredService<TelemetryCatalogCache>();

        // The session is unconfigured, so the real transport refuses and the area fails closed.
        var area = scope.ServiceProvider.GetServices<IExplorerArea>().OfType<TelemetryArea>().Single();
        Assert.That(await area.GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Hidden));
        Assert.That(cache.Current, Is.Null);
    }

    [Test]
    public void The_stylesheet_is_derived_from_the_shell_content_path_and_ships()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TelemetryAssets.BasePath, Is.EqualTo(ShellDesignAssets.ContentBasePath + "telemetry/"));
            Assert.That(TelemetryAssets.Stylesheet, Is.EqualTo(ShellDesignAssets.ContentBasePath + "telemetry/telemetry.css"));
            Assert.That(File.Exists(ShellStylesheets.Absolute(ShellStylesheets.WebRoot + "/telemetry/telemetry.css")), Is.True);
        });
    }

    private static (TelemetryArea Area, FakeTelemetry Telemetry, TelemetryCatalogCache Cache) Create(
        IExplorerAuthSession? auth = null,
        IExplorerSession? session = null)
    {
        var telemetry = new FakeTelemetry();
        var cache = new TelemetryCatalogCache(telemetry, auth, session);
        return (new TelemetryArea(cache, new ExplorerTenancy()), telemetry, cache);
    }
}
