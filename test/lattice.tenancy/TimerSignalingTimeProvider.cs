using System.Threading.Channels;
using Microsoft.Extensions.Time.Testing;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// A <see cref="FakeTimeProvider"/> that signals every timer it creates, so a test
/// can wait for a background loop to arm its delay before moving the clock -
/// otherwise a timer armed after the move would never fire. Deterministic: no
/// sleeps, no polling.
/// </summary>
internal sealed class TimerSignalingTimeProvider : FakeTimeProvider
{
    private readonly Channel<bool> _created = Channel.CreateUnbounded<bool>();

    /// <inheritdoc />
    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        var timer = base.CreateTimer(callback, state, dueTime, period);
        _created.Writer.TryWrite(true);
        return timer;
    }

    /// <summary>Completes when the next (not yet awaited) timer has been created.</summary>
    public async Task NextTimerAsync()
    {
        await _created.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(30)).ConfigureAwait(false);
    }
}
