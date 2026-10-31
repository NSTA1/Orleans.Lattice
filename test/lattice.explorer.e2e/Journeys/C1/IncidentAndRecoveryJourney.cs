using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C1;

/// <summary>
/// Journey C1 (#5389), incident and recovery, which opens Act III: priya deletes the October
/// archive by mistake and olivia recovers it; a faulty upstream deploy writes 500 wrong prices
/// the policy cannot catch; olivia restores <c>orders-baseline</c> over <c>factory-orders</c>,
/// reverts it when priya's corrections turn out to be gone, and restores again; priya
/// re-imports her corrections, sam re-runs the migration to version 2, and the east ledger's
/// replay of three captured orders lands in the dead letters.
/// </summary>
[Journey(C1Steps.JourneyId)]
[JourneyChapter("C1", After = new[] { "U1", "U2", "A6", "A7" })]
public sealed class IncidentAndRecoveryJourney : JourneyChapter
{
    /// <summary>
    /// How long an assertion waits by default while this journey runs: every page it drives
    /// reads a cluster through a gateway, which takes longer than Playwright's five seconds.
    /// </summary>
    private const float ExpectTimeout = 30_000;

    /// <summary>Playwright's own default assertion timeout, restored afterwards.</summary>
    private const float PlaywrightExpectTimeout = 5_000;

    /// <inheritdoc />
    public override async Task RunAsync(ChapterContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        Assertions.SetDefaultExpectTimeout(ExpectTimeout);
        try
        {
            await new IncidentAndRecoveryStory(context).RunAsync();
        }
        finally
        {
            Assertions.SetDefaultExpectTimeout(PlaywrightExpectTimeout);
        }
    }
}
