using System.Collections.Concurrent;

namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// Implementation of <see cref="IQueueDepthProbeGrain"/> that holds its
/// activation until released, and records how many calls were ever executing
/// its body <em>simultaneously</em>.
/// <para>
/// That second tally is the point of the grain, not incidental bookkeeping. It
/// is the measurement an <c>IIncomingGrainCallFilter</c> - or any in-grain
/// counter - is able to take, because both run only after the scheduler has
/// dequeued the request. On a non-reentrant grain it is therefore pinned at one
/// however many callers are queued, which is exactly the censoring the outgoing
/// observation filter exists to avoid. A test that asserts this stays at one
/// while the observation channel reports a non-empty depth fails if the channel
/// is ever re-sited at the post-dequeue seam.
/// </para>
/// <para>
/// State is static and keyed by the grain's primary key, so concurrently
/// running tests using distinct keys cannot read each other's tallies.
/// </para>
/// </summary>
[GrainType(GrainTypeName)]
public sealed class QueueDepthProbeGrain(IGrainContext context)
    : IQueueDepthProbeGrain, IGrainBase
{
    /// <summary>
    /// The explicit Orleans grain type name, so the value a test expects on the
    /// <c>grain_type</c> metric tag is fixed by this file rather than by the
    /// runtime's class-name convention.
    /// </summary>
    public const string GrainTypeName = "queuedepthprobe";

    private static readonly ConcurrentDictionary<string, TaskCompletionSource> Gates = new(StringComparer.Ordinal);
    private static readonly ConcurrentDictionary<string, Tally> Tallies = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public IGrainContext GrainContext => context;

    /// <inheritdoc />
    public async Task<int> BlockUntilReleasedAsync()
    {
        var key = context.GrainId.Key.ToString()!;
        var tally = Tallies.GetOrAdd(key, static _ => new Tally());

        int entered;
        lock (tally)
        {
            tally.Entered++;
            tally.Concurrent++;
            entered = tally.Entered;
            if (tally.Concurrent > tally.MaxConcurrent)
            {
                tally.MaxConcurrent = tally.Concurrent;
            }
        }

        try
        {
            await Gate(key).Task;
        }
        finally
        {
            lock (tally)
            {
                tally.Concurrent--;
            }
        }

        return entered;
    }

    /// <summary>Releases every call currently blocked on <paramref name="key"/>.</summary>
    /// <param name="key">The probe grain's primary key.</param>
    public static void Release(string key) => Gate(key).TrySetResult();

    /// <summary>Reads how many calls have entered the grain body for <paramref name="key"/>.</summary>
    /// <param name="key">The probe grain's primary key.</param>
    /// <returns>The cumulative entry count.</returns>
    public static int EnteredCount(string key) =>
        Tallies.TryGetValue(key, out var tally) ? Read(tally, static t => t.Entered) : 0;

    /// <summary>
    /// Reads the greatest number of calls ever executing the grain body
    /// simultaneously for <paramref name="key"/> - the post-dequeue view of
    /// concurrency, which a non-reentrant grain pins at one.
    /// </summary>
    /// <param name="key">The probe grain's primary key.</param>
    /// <returns>The peak simultaneous body executions.</returns>
    public static int MaxConcurrentExecutions(string key) =>
        Tallies.TryGetValue(key, out var tally) ? Read(tally, static t => t.MaxConcurrent) : 0;

    private static TaskCompletionSource Gate(string key) =>
        Gates.GetOrAdd(key, static _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));

    private static int Read(Tally tally, Func<Tally, int> select)
    {
        lock (tally)
        {
            return select(tally);
        }
    }

    private sealed class Tally
    {
        public int Entered;
        public int Concurrent;
        public int MaxConcurrent;
    }
}
