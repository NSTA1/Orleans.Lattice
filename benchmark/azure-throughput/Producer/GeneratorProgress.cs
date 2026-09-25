using System.Diagnostics;

namespace VehicleFleetSimulator.AzureThroughput.Producer;

internal sealed class GeneratorProgress
{
    private readonly object gate = new();
    private long sent;
    private long blocked;
    private long slip;
    private long last = Stopwatch.GetTimestamp();
    private long due = long.MaxValue;
    private bool waiting;

    private void Advance()
    {
        var now = Stopwatch.GetTimestamp();
        // Union of channel-wait time and time beyond this tick's budget.
        // Snapshot advances live waits too, even if a write never unblocks.
        blocked += waiting ? now - last : Math.Max(0, now - Math.Max(last, due));
        if (due != long.MaxValue) slip = Math.Max(slip, now - due);
        last = now;
    }

    public void BeginTick(long scheduled, long interval)
    {
        lock (gate)
        {
            Advance();
            slip = Math.Max(slip, last - scheduled);
            due = scheduled + interval;
        }
    }

    public void BeginWait() { lock (gate) { Advance(); waiting = true; } }
    public void EndWait() { lock (gate) { Advance(); waiting = false; } }
    public void Sent(int count) { lock (gate) { Advance(); sent += count; } }
    public void Complete() { lock (gate) { Advance(); due = long.MaxValue; } }
    public (long Sent, long Blocked, long Slip) Snapshot()
    {
        lock (gate) { Advance(); return (sent, blocked, slip); }
    }
}
