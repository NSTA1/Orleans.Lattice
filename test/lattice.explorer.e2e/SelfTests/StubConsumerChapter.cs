using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.SelfTests;

/// <summary>The self-test story's second stub chapter: it consumes the producer's fact through the ledger.</summary>
[JourneyChapter(StubStory.Consumer, After = new[] { StubStory.Producer }, Storyline = StubStory.Storyline)]
public sealed class StubConsumerChapter : IStoryChapter
{
    /// <inheritdoc />
    public Task RunAsync(ChapterContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        StubStory.Walk(context, context.Chapter);
        context.Record(StubStory.Received, context.Recall(StubStory.HandOff));
        return Task.CompletedTask;
    }
}
