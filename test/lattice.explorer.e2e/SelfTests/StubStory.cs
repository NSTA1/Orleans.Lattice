using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.SelfTests;

/// <summary>The self-test storyline and the ledger keys its two stub chapters hand each other.</summary>
public static class StubStory
{
    /// <summary>The self-test storyline: not a storyline of the epic, so no real run ever picks its chapters.</summary>
    public const string Storyline = "selftest";

    /// <summary>The producer stub's chapter id.</summary>
    public const string Producer = "S1";

    /// <summary>The consumer stub's chapter id.</summary>
    public const string Consumer = "S2";

    /// <summary>Set to make the producer fail.</summary>
    public static readonly StoryKey<bool> FailProducer = new("selftest.fail-producer");

    /// <summary>The fact the producer hands the consumer.</summary>
    public static readonly StoryKey<string> HandOff = new("selftest.hand-off");

    /// <summary>What the consumer received.</summary>
    public static readonly StoryKey<string> Received = new("selftest.received");

    /// <summary>The chapters in the order they ran.</summary>
    public static readonly StoryKey<IReadOnlyList<string>> Trail = new("selftest.trail");

    /// <summary>Appends <paramref name="chapter"/> to the trail.</summary>
    /// <param name="context">The chapter's context.</param>
    /// <param name="chapter">The chapter id.</param>
    public static void Walk(ChapterContext context, string chapter)
    {
        ArgumentNullException.ThrowIfNull(context);
        var trail = context.Ledger.TryGet(Trail, out var sofar) ? sofar : [];
        context.Record(Trail, [.. trail, chapter]);
    }
}
