namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>
/// A chapter's contract, implemented for every chapter by F6 (#5364) through the facades and
/// never through the UI: <see cref="EstablishAsync"/> produces the chapter's produces and
/// <see cref="VerifyAsync"/> asserts them. A contract is found by reflection: any concrete
/// type in this assembly implementing this interface, with a parameterless constructor.
/// </summary>
/// <remarks>
/// <para>
/// The <see cref="StorySession"/> establishes every chapter a contract's chapter consumes, in
/// dependency order, before calling its <see cref="EstablishAsync"/>, and records each
/// establishment so it runs at most once per session. An <see cref="EstablishAsync"/> therefore
/// produces only its own chapter's state; calling
/// <see cref="ChapterContext.EstablishAsync(string)"/> for a consume is harmless.
/// </para>
/// <para>
/// Isolated mode calls, in order: <see cref="EstablishAsync"/> for the chapter's consumes,
/// <see cref="VerifyAsync"/> for each consume, the chapter's UI steps, then the chapter's own
/// <see cref="VerifyAsync"/>. Story mode calls only the chapter's own <see cref="VerifyAsync"/>
/// after its UI steps, and stands a not-yet-implemented chapter in with
/// <see cref="EstablishAsync"/> then <see cref="VerifyAsync"/>.
/// </para>
/// </remarks>
public interface IChapterContract
{
    /// <summary>The chapter id this contract is for, such as <c>A3</c>.</summary>
    string Chapter { get; }

    /// <summary>Produces the chapter's produces through the facades. Must be idempotent.</summary>
    /// <param name="context">The chapter's context: <see cref="ChapterContext.Chapter"/> is <see cref="Chapter"/>.</param>
    Task EstablishAsync(ChapterContext context);

    /// <summary>Asserts the chapter's produces through the facades.</summary>
    /// <param name="context">The chapter's context: <see cref="ChapterContext.Chapter"/> is <see cref="Chapter"/>.</param>
    Task VerifyAsync(ChapterContext context);
}
