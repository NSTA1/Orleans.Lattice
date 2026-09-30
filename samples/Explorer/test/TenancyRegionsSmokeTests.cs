using Bunit;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Tenancy;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Samples.Explorer.Tests;

/// <summary>
/// The operator sets acme's allowed regions and residency from the Explorer's
/// Regions section, rendered over a real console circuit of the two-region
/// sample, and the cluster holds what the UI said: acme starts Online in both
/// regions, and narrowing its residency to east keeps it served there with no
/// stop-serving confirmation (issue #4078). It runs its own sample, because it
/// drains west from acme's residency, which the shared estate's other checks
/// must not see.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class TenancyRegionsSmokeTests
{
    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(60);

    private ExplorerSample _sample = null!;

    [OneTimeSetUp]
    public async Task StartAsync() => _sample = await SampleTestHost.StartAsync(minimal: false);

    [OneTimeTearDown]
    public async Task StopAsync()
    {
        if (_sample is not null)
        {
            await _sample.DisposeAsync();
            File.Delete(_sample.Console.ConfigPath);
        }
    }

    [Test]
    public async Task The_operator_sets_acmes_allowed_regions_and_residency_from_the_regions_section()
    {
        const string East = SampleIdentities.EastRegion;
        const string West = SampleIdentities.WestRegion;
        const string Acme = SampleIdentities.AcmeTenant;
        await using var circuit = await ConsoleCircuit.OpenAsync(
            _sample, SampleIdentities.Administrator, TenantId.DefaultId);
        await using var ui = new BunitContext();
        ui.JSInterop.Mode = JSRuntimeMode.Loose;
        ui.Services.AddFallbackServiceProvider(circuit.Services);

        var toasts = ui.Render<LtToastRegion>();
        var cut = ui.Render<TenancyRegions>(parameters => parameters.Add(regions => regions.TenantId, Acme).Add(regions => regions.CanAuthorize, true));
        Wait(cut, () => cut.FindAll("tbody tr").Count > 0);
        Assert.That(cut.FindAll("section.lt-tenancy-part h3").Select(heading => heading.TextContent), Is.EqualTo(new[]
        {
            "Allowed regions (set by a platform operator)",
            "Residency (where the tenant's data is kept)",
        }));

        // Allow acme exactly east and west, whatever it was allowed before.
        foreach (var chip in Chips(cut))
        {
            cut.Find($"button[aria-label='Remove {chip}']").Click();
        }

        cut.Find("form.lt-tenancy-allowed input[role=combobox]").Input($"{East},{West},");
        Wait(cut, () => Chips(cut).Length == 2);
        cut.Find("form.lt-tenancy-allowed").Submit();

        // The save is done once its toast is posted and the section is no longer busy re-reading the regions.
        toasts.WaitForState(() => Toasts(toasts).Contains($"Tenant {Acme} is allowed {East}, {West}."), Budget);
        Wait(cut, () => cut.FindAll("button").Any(button => button.TextContent.Trim() == "Save allowed regions" && !button.HasAttribute("disabled")));
        Assert.That(cut.Find(".lt-dl__value").TextContent.Trim(), Is.EqualTo($"{East}, {West}"));

        // acme starts resident and Online in both regions, so both serve it.
        Wait(cut, () => cut.FindAll("tbody tr").Count == 2 && cut.FindAll("tbody tr").All(row => row.Children[2].TextContent.Trim() == "Served"));

        // Make acme resident in east alone: east stays Online, so acme stays served and the change
        // only drains west - no stop-serving dialog, and Apply is the primary button (issue #4078).
        var resident = cut.FindAll("label").Single(label => label.TextContent.Trim() == $"Resident in {West}").GetAttribute("for");
        cut.Find("#" + resident).Change(false);
        Wait(cut, () => !cut.Find("#" + resident).HasAttribute("checked"));
        Wait(cut, () => cut.FindAll(".lt-tenancy-preview__list li").Select(item => item.TextContent.Trim()).SequenceEqual(new[]
        {
            $"{East} stays in the residency, and is still served there.",
            $"{West} starts draining, and stops being served there.",
        }));
        Assert.That(cut.FindAll(".lt-tenancy-served-nowhere"), Is.Empty);
        Button(cut, "Apply residency").Click();
        Wait(cut, () => cut.FindAll("[role=alertdialog] .lt-dialog__title").Count == 1);
        Assert.That(cut.Find("[role=alertdialog] .lt-dialog__title").TextContent, Is.EqualTo("Remove regions from the residency?"));
        Button(cut, "Drain and apply").Click();

        toasts.WaitForState(() => Toasts(toasts).Contains($"Tenant {Acme} is draining {West}."), Budget);
        Wait(cut, () => cut.FindAll("tbody tr").Any(row => row.Children[0].TextContent.Trim() == West && row.Children[1].QuerySelector(".lt-pill__text")!.TextContent.Trim() == "Draining"));
        var eastRow = cut.FindAll("tbody tr").Single(row => row.Children[0].TextContent.Trim() == East);
        Assert.Multiple(() =>
        {
            Assert.That(eastRow.Children[1].QuerySelector(".lt-pill__text")!.TextContent.Trim(), Is.EqualTo("Online"));
            Assert.That(eastRow.Children[2].TextContent.Trim(), Is.EqualTo("Served"));
            Assert.That(cut.FindAll(".lt-tenancy-warning"), Is.Empty, "acme is not said to be served nowhere");
            Assert.That(Toasts(toasts), Has.None.Contains("not served anywhere"));
        });

        using (LatticeCredentialContext.Use(SampleSeeder.BasicToken(SampleIdentities.Administrator), scheme: DemoBasicAuthenticator.Scheme))
        {
            var report = await _sample.East.Services.GetRequiredService<ILatticeTenantRegionAdmin>().GetTenantRegionStatusAsync(Acme);
            Assert.Multiple(() =>
            {
                Assert.That(report.Regions.Where(region => region.IsAllowed).Select(region => region.RegionId), Is.EqualTo(new[] { East, West }));
                Assert.That(report.Regions.Single(region => region.RegionId == East).Status, Is.EqualTo(TenantRegionLifecycleStatus.Online));
                Assert.That(report.Regions.Single(region => region.RegionId == West).Status, Is.EqualTo(TenantRegionLifecycleStatus.Draining));
            });
        }

        // And east still serves acme: its admin reads acme's orders there.
        await using var acme = await ConsoleCircuit.OpenAsync(_sample, SampleIdentities.AcmeAdmin, Acme);
        var read = await acme.ReadAsync(SampleSeeder.OrdersTree(Acme), "order-1001");
        Assert.That(read.Status, Is.EqualTo(Orleans.Lattice.Api.State.StateQueryStatus.Found), "acme is still served in east");
    }

    private static string[] Chips(IRenderedComponent<TenancyRegions> cut) =>
        [.. cut.FindAll(".lt-combobox__chip-value").Select(chip => chip.TextContent)];

    private static string[] Toasts(IRenderedComponent<LtToastRegion> region) =>
        [.. region.FindAll(".lt-toast__message").Select(message => message.TextContent)];

    private static void Wait(IRenderedComponent<TenancyRegions> cut, Func<bool> state)
    {
        try
        {
            cut.WaitForState(state, Budget);
        }
        catch (Bunit.Extensions.WaitForHelpers.WaitForFailedException)
        {
            Assert.Fail("The regions section did not reach the expected state. It reads:" + Environment.NewLine + cut.Markup);
        }
    }

    private static AngleSharp.Dom.IElement Button(IRenderedComponent<TenancyRegions> cut, string text)
    {
        Wait(cut, () => cut.FindAll("button").Count(button => button.TextContent.Trim() == text) == 1);
        return cut.FindAll("button").Single(button => button.TextContent.Trim() == text);
    }
}
