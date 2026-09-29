using Bunit;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.UI;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// The Data area's contract with the chrome: its key and order, availability
/// (visible, hidden when there is no state API or the caller is refused,
/// unavailable with a fixed reason otherwise), badge, Home status, command and
/// registration.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataAreaTests : DataTestContext
{
    [Test]
    public void The_area_is_data_first_in_the_directory_and_tenant_scoped()
    {
        var area = Area;

        Assert.Multiple(() =>
        {
            Assert.That(area.Key, Is.EqualTo("data"));
            Assert.That(area.DisplayName, Is.EqualTo("Data"));
            Assert.That(area.DirectoryOrder, Is.EqualTo(10));
            Assert.That(((IExplorerArea)area).IsTenantScoped, Is.True);
            Assert.That(area.Completions, Is.InstanceOf<DataCompletionSource>());
        });
    }

    [Test]
    public async Task Availability_is_visible_when_the_catalogue_answers()
    {
        Client.WithTree("orders");

        var availability = await Area.GetAvailabilityAsync(CancellationToken.None);

        Assert.That(availability, Is.EqualTo(AreaAvailability.Visible));
    }

    [Test]
    public async Task Availability_is_hidden_for_a_restricted_identity()
    {
        Client.Fault = call => call == nameof(ILatticeStateClient.ListTreesAsync)
            ? new LatticeStateApiException("Access to the state API was denied.") { IsPermissionDenied = true }
            : null;

        var availability = await Area.GetAvailabilityAsync(CancellationToken.None);

        Assert.That(availability, Is.EqualTo(AreaAvailability.Hidden));
    }

    [Test]
    public async Task Availability_is_hidden_when_the_head_serves_no_state_api()
    {
        Services.RemoveAll<ILatticeStateClient>();
        Services.AddScoped<ILatticeStateClient>(provider => provider.GetRequiredService<ILatticeStateConnection>());

        var availability = await Area.GetAvailabilityAsync(CancellationToken.None);

        Assert.That(availability, Is.EqualTo(AreaAvailability.Hidden));
    }

    [Test]
    public async Task Availability_is_unavailable_with_a_fixed_reason_that_never_quotes_the_server()
    {
        Client.Fault = call => call == nameof(ILatticeStateClient.ListTreesAsync)
            ? new InvalidOperationException("tree t/acme/a/crm/orders is on fire")
            : null;

        var availability = await Area.GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(availability.Kind, Is.EqualTo(AreaAvailabilityKind.Unavailable));
            Assert.That(availability.Reason, Is.EqualTo("The Explorer could not read the tree catalogue. Try again."));
        });
    }

    [Test]
    public async Task Availability_retries_after_a_failure_and_memoises_success()
    {
        Client.WithTree("orders");
        Client.Fault = _ => new RpcException(new Status(StatusCode.Internal, "boom"));
        var first = await Area.GetAvailabilityAsync(CancellationToken.None);
        Client.Fault = null;

        var second = await Area.GetAvailabilityAsync(CancellationToken.None);
        var calls = Client.Calls.Count;
        var third = await Area.GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first.Kind, Is.EqualTo(AreaAvailabilityKind.Unavailable));
            Assert.That(second, Is.EqualTo(AreaAvailability.Visible));
            Assert.That(third, Is.EqualTo(AreaAvailability.Visible));
            Assert.That(Client.Calls, Has.Count.EqualTo(calls), "a visible verdict is memoised for the circuit");
        });
    }

    [Test]
    public async Task The_badge_counts_the_loaded_directory_and_is_absent_before_it_loads()
    {
        Client.WithTree("orders").WithTree("customers");
        var before = await Area.GetDirectoryBadgeAsync(CancellationToken.None);

        await Services.GetRequiredService<DataDirectory>().LoadAsync();
        var after = await Area.GetDirectoryBadgeAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(before, Is.Null);
            Assert.That(after, Is.EqualTo("2"));
        });
    }

    [Test]
    public async Task The_home_status_counts_trees_and_views()
    {
        Client.WithTree("orders").WithTree("customers");
        Client.Views.Add(new ViewStateSummary { ViewName = "by-status", SourceTreeId = "orders" });

        var status = await Area.GetHomeStatusAsync(CancellationToken.None);

        Assert.That(status, Is.EqualTo("2 trees and 1 view."));
    }

    [Test]
    public async Task The_refresh_command_reloads_the_directory_and_has_a_visible_control()
    {
        Client.WithTree("orders");
        var command = Area.Commands.Single();
        var cut = RenderAt("data");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr.lt-table__row"), Has.Count.EqualTo(1)));

        Client.WithTree("customers");
        await cut.InvokeAsync(async () => await command.InvokeAsync!(CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(command.Id, Is.EqualTo("data.refresh"));
            Assert.That(command.Target, Is.EqualTo(ExplorerAddress.ForArea("data")));
        });
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr.lt-table__row"), Has.Count.EqualTo(2)));
        ExplorerCommandControls.AssertVisibleControl(cut, command);
    }

    [Test]
    public void Registration_is_idempotent_and_registers_one_data_area()
    {
        var services = new ServiceCollection();
        services.AddLatticeExplorerShell();
        services.AddLatticeExplorerShell();

        Assert.That(services.Count(descriptor => descriptor.ServiceType == typeof(IExplorerArea) && descriptor.ImplementationType == typeof(DataArea)), Is.EqualTo(1));
        Assert.That(services.Any(descriptor => descriptor.ServiceType == typeof(DataDirectory)), Is.True);
    }
}
