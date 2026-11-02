using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A1;

/// <summary>Steps 1 to 3: signed out, signing in (wrong once), and the spine.</summary>
internal sealed partial class FirstContactRun
{
    /// <summary>The areas Contoso's spine lists for its bootstrap administrator, Home first. Tenancy is absent: Contoso is not tenanted.</summary>
    private static readonly string[] ContosoAreas = ["home", "data", "apps", "access", "schema", "replication", "backups", "telemetry", "cluster"];

    /// <summary>
    /// A1.1: the Explorer root, signed out. The connection indicator asks for a sign-in and names
    /// the west gateway, which the deployment configured (Contoso refuses browser configuration).
    /// The sign-in dialog offers the Basic form the State API advertised.
    /// </summary>
    public Task OpenSignedOutAsync() => StepAsync("1", async () =>
    {
        await Shell.ExpectSignedOutAsync();
        // Signed out, the Explorer still holds a live anonymous connection to the gateway: the
        // indicator reads Connected and names the endpoint the deployment configured.
        await Indicator.ExpectAsync("Connected", WestGateway);
        Coverage.Hit("use-case:shell/connection-indicator");
        await CheckpointAsync("1", "signed-out", "Olivia opens the Explorer signed out: the indicator is connected to the west gateway, and the header offers Sign in");

        // The deployment configured the endpoint, so the indicator's endpoint opens the read-only
        // Cluster connection dialog, which names it and offers no way to change it (#5479).
        var connection = await Indicator.OpenConnectionDialogAsync();
        await CheckpointAsync("1", "connection-dialog", "The Cluster connection dialog names the configured west gateway, read-only");
        await Indicator.ExpectReadOnlyAndCloseAsync(connection, WestGateway);
        Coverage.Hit("dialog:shell/connection-read-only");

        var dialog = await Shell.OpenSignInAsync();
        await Expect(dialog.GetByLabel("Username")).ToBeVisibleAsync();
        await Expect(dialog.GetByLabel("Password")).ToBeVisibleAsync();
        await Expect(dialog.Locator("form[method=post]")).ToHaveCountAsync(1);
        Coverage.Hit("dialog:shell/sign-in");
        Coverage.Hit("use-case:shell/discovery");
        await CheckpointAsync("1", "sign-in-dialog", "The sign-in dialog offers the username and password form the cluster advertised");
    });

    /// <summary>
    /// A1.2: a mistyped password is refused, with no partial shell; the right one signs Olivia in
    /// and she lands on Home.
    /// </summary>
    public Task SignInAsync() => StepAsync("2", async () =>
    {
        await Shell.SubmitSignInAsync(Olivia.Persona.Id, Olivia.Persona.Password + "-mistyped");
        await Shell.ExpectSignInRefusedAsync();
        await Expect(Shell.SessionButton(Olivia.Persona.Id)).ToHaveCountAsync(0);
        Coverage.Hit("query:shell.signin");
        await CheckpointAsync("2", "password-refused", "A mistyped password: the cluster refuses it, and the dialog says so");

        await Shell.CloseSignInAsync();
        await Shell.ExpectSignedOutAsync();
        var refused = await Shell.ReadSpineAsync();
        Assert.That(refused.CanOpen("access") || refused.CanOpen("cluster"), Is.False, $"a refused password opens nothing of the estate: {refused}");
        await CheckpointAsync("2", "no-partial-shell", "No session was established: no area of the estate is open and the header still offers Sign in");

        await Olivia.SignInAsync();
        await Shell.ExpectConnectionAsync("Connected");
        await ExpectHomeAsync();
        Coverage.Hit("use-case:shell/sign-in-basic");
        Coverage.Hit("route:/");
        await CheckpointAsync("2", "home", "With the right password Olivia is signed in and lands on Home, the estate overview");
    });

    /// <summary>A1.3: the spine lists every Contoso area, and no Tenancy.</summary>
    public Task ReadSpineAsync() => StepAsync("3", async () =>
    {
        var spine = await Shell.ReadSpineAsync();
        Assert.Multiple(() =>
        {
            Assert.That(spine.Stops, Is.EquivalentTo(ContosoAreas), $"the spine lists Contoso's areas: {spine}");
            Assert.That(spine.Stops, Does.Not.Contain("tenancy"), "Contoso is not tenanted");
            Assert.That(spine.Open, Is.EquivalentTo(ContosoAreas), $"the bootstrap administrator can open every area: {spine}");
        });
        await Expect(Shell.Stop("tenancy")).ToHaveCountAsync(0);
        Coverage.Hit("use-case:shell/spine");
        await CheckpointAsync("3", "spine", "The spine: Data, Apps, Access, Schema, Replication, Backups, Telemetry and Cluster, and no Tenancy");
    });
}
