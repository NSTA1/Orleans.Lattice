using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>
/// The area contract: fail-closed visibility from the facade's capability probe,
/// the Home status line and directory badge, the palette commands, the address
/// completions, and the registration seam.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class SchemaAreaTests : SchemaTestContext
{
    [Test]
    public async Task It_is_visible_when_the_capability_probe_grants_anything()
    {
        var area = Area;
        var availability = await area.GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(area.Key, Is.EqualTo("schema"));
            Assert.That(area.DisplayName, Is.EqualTo("Schema"));
            Assert.That(area.DirectoryOrder, Is.EqualTo(40));
            Assert.That(((IExplorerArea)area).IsTenantScoped, Is.True);
            Assert.That(availability, Is.EqualTo(AreaAvailability.Visible));
            Assert.That(Schema.Calls, Does.Contain("ProbeCapabilities:" + SchemaAccess.ProbeTreeId), "the probe asks about the reserved sentinel, never a real tree");
        });
    }

    [Test]
    public async Task A_signed_in_caller_the_probe_grants_nothing_sees_no_area()
    {
        Auth.SignIn("dana");
        Schema.DefaultCapabilities = FakeSchemaControl.None;

        Assert.That(await Area.GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Hidden));
    }

    [Test]
    public async Task An_anonymous_caller_the_probe_grants_nothing_is_asked_to_sign_in()
    {
        Schema.DefaultCapabilities = FakeSchemaControl.None;

        var availability = await Area.GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(availability.Kind, Is.EqualTo(AreaAvailabilityKind.Unavailable));
            Assert.That(availability.Reason, Is.EqualTo(SchemaAccess.SignInReason));
        });
    }

    [Test]
    public async Task A_denial_from_the_probe_fails_closed()
    {
        Auth.SignIn("dana");
        Schema.Faults["ProbeCapabilities"] = new LatticeAuthorizationDeniedException("denied");

        Assert.That(await Area.GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Hidden));
    }

    [Test]
    public async Task An_unserved_or_unreachable_facade_hides_the_area_and_is_asked_again_next_time()
    {
        Schema.Faults["ProbeCapabilities"] = new NotSupportedException("unimplemented");
        Assert.That(await Area.GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Hidden));

        Schema.Faults["ProbeCapabilities"] = new ShellTransportException("unavailable", isTransient: true, new InvalidOperationException("inner"));
        Assert.That(await Area.GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Hidden));

        Schema.Faults.Remove("ProbeCapabilities");
        Assert.Multiple(async () =>
        {
            Assert.That(await Area.GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Visible));
            Assert.That(Schema.CountOf("ProbeCapabilities"), Is.EqualTo(3), "a fault is never remembered");
        });
    }

    [Test]
    public async Task A_head_without_the_schema_facade_hides_the_area()
    {
        Services.RemoveAllKeyed<ILatticeSchemaControl>(ShellFacades.Key);

        Assert.That(await Area.GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Hidden));
    }

    [Test]
    public async Task A_definite_answer_is_remembered_until_the_sign_in_changes()
    {
        await Area.GetAvailabilityAsync(CancellationToken.None);
        await Area.GetAvailabilityAsync(CancellationToken.None);
        Assert.That(Schema.CountOf("ProbeCapabilities"), Is.EqualTo(1));

        Schema.DefaultCapabilities = FakeSchemaControl.None;
        Auth.SignIn("dana");

        Assert.Multiple(async () =>
        {
            Assert.That(await Area.GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Hidden));
            Assert.That(Schema.CountOf("ProbeCapabilities"), Is.EqualTo(2));
        });
    }

    [Test]
    public async Task A_new_connection_forgets_the_remembered_answer()
    {
        await Area.GetAvailabilityAsync(CancellationToken.None);

        await Explorer.ApplyAsync(Orleans.Lattice.Explorer.Tests.UI.Session.SessionTestContext.RemoteConfiguration());
        await Area.GetAvailabilityAsync(CancellationToken.None);

        Assert.That(Schema.CountOf("ProbeCapabilities"), Is.EqualTo(2));
    }

    [Test]
    public async Task The_home_status_counts_the_trees_under_schema()
    {
        UseEstate();

        Assert.That(await Area.GetHomeStatusAsync(CancellationToken.None), Is.EqualTo("3 trees under schema, 2 versioned."));
    }

    [Test]
    public async Task The_home_status_says_so_when_no_tree_is_under_schema()
    {
        UseTrees("scratch");

        Assert.That(await Area.GetHomeStatusAsync(CancellationToken.None), Is.EqualTo("No tree is under a schema policy yet."));
    }

    [Test]
    public async Task A_default_version_config_is_neither_under_schema_nor_versioned()
    {
        // The cluster reads an absent config as family 0 at version 0 (#3985).
        UseEstate();
        Schema.UnversionedReadsAsDefault = true;

        Assert.That(await Area.GetHomeStatusAsync(CancellationToken.None), Is.EqualTo("3 trees under schema, 2 versioned."));
    }

    [Test]
    public async Task Only_default_version_configs_leave_no_tree_under_schema()
    {
        UseTrees("scratch", "factory-floor");
        Schema.UnversionedReadsAsDefault = true;

        Assert.That(await Area.GetHomeStatusAsync(CancellationToken.None), Is.EqualTo("No tree is under a schema policy yet."));
    }

    [Test]
    public async Task The_badge_answers_only_from_a_listing_already_read()
    {
        UseEstate();

        Assert.That(await Area.GetDirectoryBadgeAsync(CancellationToken.None), Is.Null, "the badge never starts a listing");
        Assert.That(Schema.CountOf("GetPolicy"), Is.Zero);

        await Directory.GetAsync(refresh: false, CancellationToken.None);

        Assert.That(await Area.GetDirectoryBadgeAsync(CancellationToken.None), Is.EqualTo("3"));
    }

    [Test]
    public async Task The_badge_is_empty_when_nothing_is_under_schema()
    {
        UseTrees("scratch");
        await Directory.GetAsync(refresh: false, CancellationToken.None);

        Assert.That(await Area.GetDirectoryBadgeAsync(CancellationToken.None), Is.Null);
    }

    [Test]
    public async Task Scan_compliance_targets_the_directory_and_asks_it_to_open_the_picker()
    {
        var command = Area.Commands.Single(candidate => candidate.Id == SchemaArea.ScanCommandId);
        var signals = Services.GetRequiredService<SchemaCommandSignals>();

        await command.InvokeAsync!(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(command.Title, Is.EqualTo("Scan compliance..."));
            Assert.That(command.Target, Is.EqualTo(SchemaAddresses.Directory));
            Assert.That(signals.TryTake(SchemaArea.ScanCommandId), Is.True, "an unheard request is held for the page");
            Assert.That(signals.TryTake(SchemaArea.ScanCommandId), Is.False, "and taken once");
        });
    }

    [Test]
    public void Show_every_tree_is_a_navigation_command()
    {
        var command = Area.Commands.Single(candidate => candidate.Id == SchemaArea.AllTreesCommandId);

        Assert.Multiple(() =>
        {
            Assert.That(command.Target!.Format(), Is.EqualTo("/schema?show=all"));
            Assert.That(command.InvokeAsync, Is.Null);
            Assert.That(Area.Commands.Select(candidate => candidate.Id), Is.All.StartsWith("schema."));
        });
    }

    [Test]
    public void A_signal_heard_by_a_listening_page_is_not_held()
    {
        var signals = new SchemaCommandSignals();
        var heard = new List<string>();
        signals.Requested += heard.Add;

        _ = signals.RequestAsync(SchemaArea.ScanCommandId);

        Assert.Multiple(() =>
        {
            Assert.That(heard, Is.EqualTo(new[] { SchemaArea.ScanCommandId }));
            Assert.That(signals.TryTake(SchemaArea.ScanCommandId), Is.False);
            Assert.That(() => signals.RequestAsync(""), Throws.ArgumentException);
        });
    }

    [TestCase("Search", "ORD", new[] { "a/crm/orders", "orders" })]
    [TestCase("Search", "scratch", new string[0])]
    [TestCase("App", "cr", new[] { "a/crm/orders" })]
    [TestCase("Address", "/schema/au", new[] { "audit" })]
    [TestCase("Tenant", "a", new string[0])]
    public async Task It_completes_trees_in_schema_scope(string mode, string text, string[] expected)
    {
        UseEstate();

        var results = await Area.Completions!.CompleteAsync(new AddressQuery(text, Enum.Parse<AddressQueryMode>(mode), ExplorerAddress.Home), CancellationToken.None);

        Assert.That(results.Select(result => result.Label), Is.EqualTo(expected));
    }

    [Test]
    public async Task A_completion_targets_the_tree_in_the_current_tenant_and_says_why_it_is_in_scope()
    {
        UseEstate();
        var current = ExplorerAddress.Parse("/t/acme/schema");

        var results = await Area.Completions!.CompleteAsync(new AddressQuery("", AddressQueryMode.Search, current), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(results.Select(result => result.Target.Format()), Is.EqualTo(new[]
            {
                "/t/acme/schema/a/crm/orders",
                "/t/acme/schema/audit",
                "/t/acme/schema/orders",
            }));
            Assert.That(results.Select(result => result.Detail), Is.EqualTo(new[]
            {
                "Schema declared by an app",
                "Schema versioning",
                "Schema policy and versioning",
            }));
        });
    }

    [Test]
    public void The_shell_registers_the_area_once_with_its_services()
    {
        var areas = Services.GetServices<IExplorerArea>().OfType<SchemaArea>().ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(areas, Has.Length.EqualTo(1));
            Assert.That(Services.GetService<SchemaOperations>(), Is.Not.Null);
            Assert.That(Services.GetService<SchemaComplianceLedger>(), Is.Not.Null);
        });
    }
}
