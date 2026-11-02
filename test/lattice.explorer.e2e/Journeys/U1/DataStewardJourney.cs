using System.Text.RegularExpressions;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U1;

/// <summary>
/// U1 (#5383), a data steward's day: priya, through the rights A2 gave
/// <c>data-stewards</c>, browses the factory's trees, fixes orders under A4's policy, loads
/// the October archive, cancels an oversized import, applies finance's corrections, exports
/// evidence and checks the order views and tag index, while the line feed keeps writing
/// around her. Every step is driven through the Explorer; the only off-UI actors are the
/// line feed (its own writes) and, in step 4, the harness writing <c>order/000050</c> as the
/// line feed while priya's editor is open.
/// </summary>
[Journey(JourneyId)]
[JourneyChapter("U1", After = new[] { "A2", "A3", "A4" })]
public sealed partial class DataStewardJourney : JourneyChapter
{
    /// <summary>The journey id its screenshots are filed under.</summary>
    public const string JourneyId = "u1";

    private ChapterContext _context = default!;
    private PersonaSession _priya = default!;
    private DataDirectoryPage _directory = default!;
    private DataTreePage _tree = default!;
    private string _files = default!;

    private static JourneyRecorder Recorder => JourneyRecorder.For(JourneyId);

    /// <inheritdoc />
    public override async Task RunAsync(ChapterContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        _context = context;
        _files = Path.Combine(context.ArtifactsDirectory, "files");
        Directory.CreateDirectory(_files);

        await using var priya = await context.Estate.SignInAsAsync(Personas.Priya);
        _priya = priya;
        _directory = new DataDirectoryPage(priya);
        _tree = new DataTreePage(priya);

        await StepAsync("1", SignInAndBrowseTheDirectoryAsync);
        await StepAsync("2", CompleteAnAddressAndPageTheOrdersAsync);
        await StepAsync("3", FollowLiveUpdatesAndRecoverAnExpiredCursorAsync);
        await StepAsync("4", FixAnOrderAndResolveAConcurrentChangeAsync);
        await StepAsync("5", EditBinaryAndRawValuesAsync);
        await StepAsync("6", HaveAnInvalidEditRefusedAsync);
        await StepAsync("7", CreateAndDeleteAnEntryAndTailHistoryAsync);
        await StepAsync("8", SeedTheArchiveFromACsvAsync);
        await StepAsync("9", CancelAnOversizedImportAndDeleteItsTreeAsync);
        await StepAsync("10", ApplyTheCorrectionsAsync);
        await StepAsync("11", ExportEvidenceAsync);
        await StepAsync("12", CheckViewsTagIndexesAndMetricsAsync);
        await StepAsync("13", SeeWhatIsNotOfferedAsync);
    }

    /// <summary>Runs one step inside its own coverage scope, so a later failure keeps this step's evidence.</summary>
    private static async Task StepAsync(string step, Func<Task> body)
    {
        using var scope = Coverage.BeginScope("U1." + step);
        await body();
        scope.Complete();
    }

    /// <summary>A checkpoint of the running step: screenshot, axe, console and host faults.</summary>
    private Task CheckpointAsync(string step, string substep, string title, SubstepOptions? options = null) =>
        _priya.SubstepAsync(Recorder, step, substep, title, options);

    /// <summary>A regex matching <paramref name="text"/> exactly, ignoring surrounding whitespace.</summary>
    private static Regex Exactly(string text) => new("^\\s*" + Regex.Escape(text) + "\\s*$");
}
