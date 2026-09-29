using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Shell.Areas.Backups;
using Orleans.Lattice.Explorer.Shell.Navigation;
using Orleans.Lattice.Explorer.Tests.Shell.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Backups;

/// <summary>
/// The Backups area's contract: its identity and spine position, its
/// fail-closed availability from the capability probe, its Home status line,
/// and its one palette command.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class BackupsAreaTests : BackupsTestContext
{
    [Test]
    public void The_area_is_backups_at_directory_order_seventy_and_follows_the_tenant()
    {
        var area = Area();

        Assert.Multiple(() =>
        {
            Assert.That(area.Key, Is.EqualTo("backups"));
            Assert.That(area.DisplayName, Is.EqualTo("Backups"));
            Assert.That(area.DirectoryOrder, Is.EqualTo(70));
            Assert.That(((IExplorerArea)area).IsTenantScoped, Is.True);
            Assert.That(area.Completions, Is.InstanceOf<BackupsCompletionSource>());
        });
    }

    [Test]
    public void AddLatticeExplorerShell_registers_the_area_and_its_services_once_per_circuit()
    {
        var services = new ServiceCollection();
        Orleans.Lattice.Explorer.Shell.ShellServiceCollectionExtensions.AddLatticeExplorerShell(services);
        Orleans.Lattice.Explorer.Shell.ShellServiceCollectionExtensions.AddLatticeExplorerShell(services);

        Assert.Multiple(() =>
        {
            Assert.That(services.Count(descriptor => descriptor.ServiceType == typeof(IExplorerArea) && descriptor.ImplementationType == typeof(BackupsArea)), Is.EqualTo(1));
            foreach (var type in new[] { typeof(BackupsAccess), typeof(BackupsCompletionSource), typeof(BackupAppTrees), typeof(BackupOperations), typeof(BackupActions), typeof(BackupsInterop) })
            {
                var descriptor = services.Single(candidate => candidate.ServiceType == type);
                Assert.That(descriptor.Lifetime, Is.EqualTo(ServiceLifetime.Scoped), type.Name);
            }
        });
    }

    [Test]
    public async Task A_caller_who_may_list_backups_sees_the_area()
    {
        var availability = await Area().GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(availability, Is.EqualTo(AreaAvailability.Visible));
            Assert.That(Backups.LastOf<BackupScopeSelector>(nameof(ILatticeBackupControl.ProbeCapabilitiesAsync)).TreeId, Is.EqualTo(BackupsAccess.ProbeTreeId));
        });
    }

    [Test]
    public async Task A_restricted_identity_sees_the_area_unavailable_with_the_grant_it_needs()
    {
        Backups.Probe = scope => Task.FromResult(FakeBackupControl.DenyAll(scope));

        var availability = await Area().GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(availability.Kind, Is.EqualTo(AreaAvailabilityKind.Unavailable));
            Assert.That(availability.Reason, Is.EqualTo(BackupsAccess.NoGrantReason));
        });
    }

    [Test]
    public async Task A_denial_reads_as_unavailable()
    {
        Backups.Probe = _ => Task.FromException<BackupScopeCapabilities>(new LatticeAuthorizationDeniedException("no"));

        var availability = await Area().GetAvailabilityAsync(CancellationToken.None);

        Assert.That(availability.Kind, Is.EqualTo(AreaAvailabilityKind.Unavailable));
    }

    [Test]
    public async Task A_cluster_that_does_not_serve_backup_control_hides_the_area()
    {
        Backups.Probe = _ => Task.FromException<BackupScopeCapabilities>(new NotSupportedException());

        Assert.That(await Area().GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Hidden));
    }

    [Test]
    public async Task A_fault_hides_the_area_and_is_asked_again_next_time()
    {
        var calls = 0;
        Backups.Probe = scope => ++calls == 1
            ? Task.FromException<BackupScopeCapabilities>(new InvalidOperationException("not configured"))
            : Task.FromResult(FakeBackupControl.AllowAll(scope));
        var area = Area();

        var first = await area.GetAvailabilityAsync(CancellationToken.None);
        var second = await area.GetAvailabilityAsync(CancellationToken.None);
        var third = await area.GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(AreaAvailability.Hidden));
            Assert.That(second, Is.EqualTo(AreaAvailability.Visible));
            Assert.That(third, Is.EqualTo(AreaAvailability.Visible));
            Assert.That(calls, Is.EqualTo(2), "a definite answer is remembered for the circuit");
        });
    }

    [Test]
    public void The_directory_cancelling_the_probe_propagates_rather_than_reading_as_visible()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        Backups.Probe = _ => Task.FromCanceled<BackupScopeCapabilities>(cancellation.Token);

        Assert.That(async () => await Area().GetAvailabilityAsync(cancellation.Token), Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task Home_reports_the_inventory_when_it_is_served()
    {
        Backups.Inventory = () => Task.FromResult(new BackupInventoryReport(1204, 10, 1000, 204, null, new DateTimeOffset(2026, 9, 28, 14, 2, 11, TimeSpan.Zero), 0, 0, 0));

        Assert.That(await Area().GetHomeStatusAsync(CancellationToken.None), Is.EqualTo("1,204 backups, newest 2026-09-28 14:02:11 UTC"));
    }

    [Test]
    public async Task Home_falls_back_to_the_newest_catalogued_backup_when_the_inventory_is_not_served()
    {
        Seed(FakeBackupControl.Manifest("b1", createdAt: new DateTimeOffset(2026, 9, 1, 0, 0, 0, TimeSpan.Zero)));

        Assert.That(await Area().GetHomeStatusAsync(CancellationToken.None), Is.EqualTo("Newest backup 2026-09-01 00:00:00 UTC"));
    }

    [Test]
    public async Task Home_says_so_when_there_are_no_backups()
    {
        Assert.That(await Area().GetHomeStatusAsync(CancellationToken.None), Is.EqualTo("No backups yet"));
        Backups.Inventory = () => Task.FromResult(new BackupInventoryReport(0, 0, 0, 0, null, null, 0, 0, 0));
        Assert.That(await Area().GetHomeStatusAsync(CancellationToken.None), Is.EqualTo("No backups yet"));
    }

    [Test]
    public async Task The_capture_command_targets_the_catalogue_and_opens_the_capture_form()
    {
        var command = Area().Commands.Single();

        await command.InvokeAsync!(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(command.Id, Is.EqualTo(BackupsArea.CaptureCommandId));
            Assert.That(command.Title, Is.EqualTo("Capture backup..."));
            Assert.That(command.Target, Is.EqualTo(BackupsAddresses.Root));
            Assert.That(CurrentPath, Is.EqualTo("/backups/new"));
        });
    }

    [Test]
    public void The_capture_command_has_its_visible_control_on_the_catalogue()
    {
        var cut = RenderAt<BackupsCataloguePage>("backups");

        ExplorerCommandControls.AssertVisibleControl(cut, Area().Commands.Single());
    }

    private BackupsArea Area() => Services.GetServices<IExplorerArea>().OfType<BackupsArea>().Single();
}
