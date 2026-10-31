using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.SelfTests;

/// <summary>The self-test story's first stub chapter: it records a fact for the consumer, or fails on request.</summary>
[JourneyChapter(StubStory.Producer, Storyline = StubStory.Storyline)]
public sealed class StubProducerChapter : IStoryChapter
{
    /// <summary>The failure the producer raises when asked to.</summary>
    public const string FailureMessage = "The stub producer failed on purpose.";

    /// <inheritdoc />
    public Task RunAsync(ChapterContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        StubStory.Walk(context, context.Chapter);
        if (context.Ledger.TryGet(StubStory.FailProducer, out var fail) && fail)
        {
            throw new InvalidOperationException(FailureMessage);
        }

        context.Record(StubStory.HandOff, "made by " + context.Chapter);
        return Task.CompletedTask;
    }
}
