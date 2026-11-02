using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;

/// <summary>
/// A1, first contact (#5371). Produces olivia's appearance, Board theme and compact density.
/// An appearance lives in the browser's own storage, not in the cluster, so the ledger carries
/// it (<see cref="StoryKeys.OliviaAppearance"/>), in the <c>theme/contrast/density</c> form of
/// <see cref="ContosoAppearance"/>.
/// </summary>
public sealed class A1Contract() : ChapterContract("A1")
{
    /// <inheritdoc />
    public override Task EstablishAsync(ChapterContext context)
    {
        context.Record(StoryKeys.OliviaAppearance, ContosoAppearance.OliviaAfterA1);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public override Task VerifyAsync(ChapterContext context)
    {
        RequireEqual(context.Recall(StoryKeys.OliviaAppearance), ContosoAppearance.OliviaAfterA1, "olivia's appearance is Board, compact");
        return Task.CompletedTask;
    }
}
