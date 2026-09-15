namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// Implementation of <see cref="IQueueDepthFanOutGrain"/>. Every call is
/// dispatched before any is awaited, so all of them are outstanding against the
/// probe activation at once and the queue is genuinely deep rather than merely
/// busy.
/// </summary>
public sealed class QueueDepthFanOutGrain(IGrainContext context, IGrainFactory grainFactory)
    : IQueueDepthFanOutGrain, IGrainBase
{
    /// <inheritdoc />
    public IGrainContext GrainContext => context;

    /// <inheritdoc />
    public Task FanOutAsync(string probeKey, int count)
    {
        var probe = grainFactory.GetGrain<IQueueDepthProbeGrain>(probeKey);
        var calls = new Task<int>[count];
        for (var i = 0; i < count; i++)
        {
            calls[i] = probe.BlockUntilReleasedAsync();
        }

        return Task.WhenAll(calls);
    }
}
