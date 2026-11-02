using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;

/// <summary>
/// Personalising the console (U5, #5387). Produces sam's appearance preference, which lives in
/// his browser's storage, so the ledger carries it (<see cref="StoryKeys.SamAppearance"/>):
/// <see cref="ContosoAppearance.SamAfterU5"/>, because the chapter's last step resets the view.
/// </summary>
public sealed class U5Contract() : ChapterContract("U5")
{
    /// <inheritdoc />
    public override Task EstablishAsync(ChapterContext context)
    {
        context.Record(StoryKeys.SamAppearance, ContosoAppearance.SamAfterU5);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public override Task VerifyAsync(ChapterContext context)
    {
        RequireEqual(context.Recall(StoryKeys.SamAppearance), ContosoAppearance.SamAfterU5, "sam's appearance is the reset default");
        return Task.CompletedTask;
    }
}
