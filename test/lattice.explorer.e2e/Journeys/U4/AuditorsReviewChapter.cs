using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U4;

/// <summary>
/// U4, the auditor's review (#5388): dana gathers evidence across every area she can read
/// and confirms every mutating action is out of her reach; nina, still ungranted, finds a
/// coherent, empty console. A read-only chapter: it produces nothing, and its contract
/// verifies the estate it read is unchanged.
/// </summary>
/// <remarks>
/// dana's rights are the ones Act I granted (<see cref="Contracts.ContosoAccess"/>): Read and
/// RangeRead on the orders, the lines and the archive through <c>auditors</c>; the telemetry
/// only through the nested <c>factory-staff</c> (A9); the cluster-wide Telemetry capability;
/// the task board's viewer role (A5). No Write, Admin, SchemaAdmin, Backup, Replication or
/// access administration anywhere.
/// </remarks>
[JourneyChapter("U4", After = new[] { "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10", "A11", "A12", "U1", "U2", "U3", "U5", "U6" })]
public sealed partial class AuditorsReviewChapter : JourneyChapter
{
    /// <summary>The journey id the screenshots are filed under.</summary>
    public const string JourneyId = "u4";

    private static JourneyRecorder Recorder => JourneyRecorder.For(JourneyId);

    // Checks that failed for a reason outside this chapter (an upstream chapter not yet
    // merged), recorded so the remaining steps still run; the chapter fails at its end.
    private readonly List<string> _deferredFailures = [];

    /// <inheritdoc />
    public override async Task RunAsync(ChapterContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        await using (var dana = await context.Estate.SignInAsAsync(Personas.Dana))
        {
            await StepAsync("1", () => DanaSignsInAsync(dana));
            await StepAsync("2", () => HistoryOfTheOrdersAsync(dana));
            await StepAsync("3", () => TelemetryThroughTheNestedGroupAsync(dana));
            await StepAsync("4", () => ExportsAsync(dana));
            await StepAsync("5", () => NoWriteAffordanceInDataAsync(dana));
            await StepAsync("6", () => SchemaIsReadOnlyAsync(dana));
            await StepAsync("7", () => BackupsAreUnavailableAsync(dana));
            await StepAsync("8", () => ReplicationShowsHerNothingAsync(dana));
            await StepAsync("9", () => TelemetryBoardsAsync(dana));
            await StepAsync("10", () => ClusterAndAccessAreWithheldAsync(dana));
        }

        await using var nina = await context.Estate.SignInAsAsync(Personas.Nina);
        await StepAsync("11", () => NinaFindsAnEmptyConsoleAsync(nina));

        Assert.That(_deferredFailures, Is.Empty, "U4 ran every step, but these checks failed:" + Environment.NewLine + string.Join(Environment.NewLine, _deferredFailures));
    }

    /// <summary>Runs one numbered step in its own coverage scope, so a later step's failure keeps this step's evidence.</summary>
    private static async Task StepAsync(string step, Func<Task> body)
    {
        using var scope = Coverage.BeginScope("U4." + step);
        await body();
        scope.Complete();
    }

    /// <summary>A checkpoint of step <paramref name="step"/>: a screenshot, then axe, the console and the hosts' logs.</summary>
    private static Task SnapAsync(PersonaSession session, string step, string substep, string title, SubstepOptions? options = null) =>
        session.SubstepAsync(Recorder, step, substep, title, options);
}
