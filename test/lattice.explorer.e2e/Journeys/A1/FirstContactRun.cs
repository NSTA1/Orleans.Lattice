using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A1;

/// <summary>
/// One run of A1 (#5371) in Olivia's browser: the steps of the issue, in order, each in its own
/// coverage scope, each distinct state a checkpoint filed under journey <see cref="JourneyId"/>.
/// </summary>
internal sealed partial class FirstContactRun
{
    /// <summary>The journey id A1's screenshots are filed under.</summary>
    public const string JourneyId = "a1";

    /// <summary>The heading of Home, the estate overview.</summary>
    private const string HomeHeading = "Estate";

    private readonly ChapterContext _context;
    private readonly JourneyRecorder _recorder = JourneyRecorder.For(JourneyId);

    /// <summary>Creates the run.</summary>
    /// <param name="context">The chapter's context.</param>
    /// <param name="olivia">Olivia's session, opened signed out at the Explorer root.</param>
    public FirstContactRun(ChapterContext context, PersonaSession olivia)
    {
        _context = context;
        Olivia = olivia;
        Indicator = new ConnectionIndicatorPage(olivia.Shell);
        Appearance = new AppearancePage(olivia.Shell);
    }

    private PersonaSession Olivia { get; }

    private ShellPage Shell => Olivia.Shell;

    private IPage Page => Olivia.Page;

    private ConnectionIndicatorPage Indicator { get; }

    private AppearancePage Appearance { get; }

    /// <summary>The <c>host:port</c> of the west gateway, the endpoint Contoso's west Explorer is configured with.</summary>
    private string WestGateway => _context.Estate.WestRegion.GatewayAddress.Authority;

    private static async Task StepAsync(string step, Func<Task> body)
    {
        using var scope = Coverage.BeginScope("A1." + step);
        await body();
        scope.Complete();
    }

    private Task CheckpointAsync(string step, string substep, string title) =>
        Olivia.SubstepAsync(_recorder, step, substep, title);

    /// <summary>Waits until the address is <paramref name="path"/>, under any tenant root, with or without a query.</summary>
    private Task ExpectAddressAsync(string path) =>
        Expect(Page).ToHaveURLAsync(new Regex($"(/t/[^/]+)?{Regex.Escape(path)}(\\?.*)?$"));

    private async Task ExpectHomeAsync()
    {
        await Shell.ExpectHeadingAsync(HomeHeading);
        await Expect(Shell.Spine).ToBeVisibleAsync();
    }
}
