using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A1;

/// <summary>
/// A1, first contact (#5371): Olivia, the platform operator and bootstrap administrator, is
/// handed the Contoso console. She signs in (getting her password wrong once), learns the
/// shell - the spine, the address line, the command palette, the keyboard, the not-found page -
/// sets it up the way she likes it, follows a deep link signed out, and resets her view.
/// </summary>
/// <remarks>
/// <para>
/// Consumes only the booted Contoso estate. Produces olivia's appearance, Board and compact,
/// recorded at <see cref="StoryKeys.OliviaAppearance"/> as the controls read it at the end.
/// </para>
/// <para>
/// Each numbered step of the issue is a step id (<c>1</c> to <c>9</c>) and a coverage scope
/// <c>A1.n</c>; each distinct state in it is a substep checkpoint. Screenshots are filed under
/// journey <c>a1</c> in both run modes.
/// </para>
/// </remarks>
[JourneyChapter("A1")]
[Journey(FirstContactRun.JourneyId)]
public sealed class FirstContactJourney : JourneyChapter
{
    /// <inheritdoc />
    public override async Task RunAsync(ChapterContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        await using var olivia = await context.Estate.OpenSessionAsync(Personas.Olivia);
        var run = new FirstContactRun(context, olivia);

        await run.OpenSignedOutAsync();
        await run.SignInAsync();
        await run.ReadSpineAsync();
        await run.NavigateAsync();
        await run.NavigateByKeyboardAsync();
        await run.MeetNotFoundAsync();
        await run.ChooseAppearanceAsync();
        await run.FollowDeepLinkSignedOutAsync();
        await run.ResetViewAsync();
    }
}
