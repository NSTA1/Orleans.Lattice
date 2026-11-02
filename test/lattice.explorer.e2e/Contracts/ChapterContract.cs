using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;

/// <summary>
/// A base for a chapter contract: the chapter id it is for, and the two halves every contract
/// implements. Concrete contracts are public, sealed and parameterless, so
/// <see cref="ChapterContracts.Discovered"/> finds them.
/// </summary>
/// <param name="chapter">The chapter id.</param>
public abstract class ChapterContract(string chapter) : IChapterContract
{
    /// <inheritdoc />
    public string Chapter { get; } = chapter;

    /// <inheritdoc />
    public abstract Task EstablishAsync(ChapterContext context);

    /// <inheritdoc />
    public abstract Task VerifyAsync(ChapterContext context);

    /// <summary>Asserts <paramref name="condition"/>, naming the chapter and the fact.</summary>
    /// <param name="condition">The fact.</param>
    /// <param name="fact">What should hold.</param>
    protected void Require(bool condition, string fact) =>
        Assert.That(condition, Is.True, $"{Chapter} produces: {fact}");

    /// <summary>Asserts <paramref name="actual"/> equals <paramref name="expected"/>, naming the chapter and the fact.</summary>
    /// <typeparam name="T">The value's type.</typeparam>
    /// <param name="actual">The observed value.</param>
    /// <param name="expected">The produced value.</param>
    /// <param name="fact">What should hold.</param>
    protected void RequireEqual<T>(T actual, T expected, string fact) =>
        Assert.That(actual, Is.EqualTo(expected), $"{Chapter} produces: {fact}");

    /// <summary>Verifies every chapter <paramref name="context"/>'s chapter consumes: the "estate unchanged" check of a read-only chapter.</summary>
    /// <param name="context">The chapter's context.</param>
    protected static async Task VerifyConsumesAsync(ChapterContext context)
    {
        foreach (var consume in context.Consumes)
        {
            var contract = ChapterContracts.Discovered.Find(consume) ?? throw new MissingChapterContractException(consume);
            await contract.VerifyAsync(context.For(consume));
        }
    }
}
