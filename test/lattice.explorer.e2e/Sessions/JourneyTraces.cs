using System.Collections.Concurrent;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;

/// <summary>
/// The traces sessions wrote during a test, kept until the test's outcome is known: a session
/// is disposed inside its test (an <c>await using</c>), before NUnit has decided whether the
/// test failed, so it always writes its trace and the journey's
/// <see cref="Checkpoints.JourneyAttribute"/> discards the traces of a test that passed.
/// </summary>
internal static class JourneyTraces
{
    private static readonly ConcurrentDictionary<string, ConcurrentBag<string>> Pending = new(StringComparer.Ordinal);

    /// <summary>Holds <paramref name="path"/> until test <paramref name="testId"/> ends.</summary>
    /// <param name="testId">The NUnit test id.</param>
    /// <param name="path">The trace file.</param>
    public static void Register(string testId, string path) => Pending.GetOrAdd(testId, _ => []).Add(path);

    /// <summary>Deletes the traces test <paramref name="testId"/>'s sessions wrote: it passed, so nothing needs them.</summary>
    /// <param name="testId">The NUnit test id.</param>
    public static void Discard(string testId)
    {
        if (!Pending.TryRemove(testId, out var paths))
        {
            return;
        }

        foreach (var path in paths)
        {
            try
            {
                File.Delete(path);
            }
            catch (IOException)
            {
            }
        }
    }

    /// <summary>Forgets test <paramref name="testId"/>'s traces, keeping the files: it failed.</summary>
    /// <param name="testId">The NUnit test id.</param>
    public static void Keep(string testId) => Pending.TryRemove(testId, out _);
}
