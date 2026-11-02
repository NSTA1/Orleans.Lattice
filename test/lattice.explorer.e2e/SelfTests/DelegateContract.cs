using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.SelfTests;

/// <summary>
/// A contract built from delegates, for self-tests. Its constructor takes arguments, so contract
/// discovery never picks it up.
/// </summary>
/// <param name="chapter">The chapter.</param>
/// <param name="establish">Establishes the chapter's produces.</param>
/// <param name="verify">Verifies them.</param>
public sealed class DelegateContract(string chapter, Func<ChapterContext, Task> establish, Func<ChapterContext, Task> verify) : IChapterContract
{
    /// <inheritdoc />
    public string Chapter { get; } = chapter;

    /// <inheritdoc />
    public Task EstablishAsync(ChapterContext context) => establish(context);

    /// <inheritdoc />
    public Task VerifyAsync(ChapterContext context) => verify(context);

    /// <summary>A contract for <paramref name="chapter"/> that appends <c>establish {chapter}</c> and <c>verify {chapter}</c> to <paramref name="log"/>.</summary>
    /// <param name="chapter">The chapter.</param>
    /// <param name="log">The call log.</param>
    public static DelegateContract Logging(string chapter, List<string> log)
    {
        ArgumentNullException.ThrowIfNull(log);
        return new DelegateContract(
            chapter,
            _ =>
            {
                lock (log)
                {
                    log.Add("establish " + chapter);
                }

                return Task.CompletedTask;
            },
            _ =>
            {
                lock (log)
                {
                    log.Add("verify " + chapter);
                }

                return Task.CompletedTask;
            });
    }
}
