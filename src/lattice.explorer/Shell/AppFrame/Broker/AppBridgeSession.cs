namespace Orleans.Lattice.Explorer.Shell.Framing.Broker;

/// <summary>
/// One frame's bridge session: the launch that fixes its authority and the frame's own
/// rate and concurrency limits. Created by <see cref="AppBridgeBroker.Open"/>; once closed
/// it drops every further request.
/// </summary>
internal sealed class AppBridgeSession
{
    private int _closed;

    internal AppBridgeSession(AppBridgeBroker owner, AppFrameLaunch launch, AppBridgeRateLimiter limiter)
    {
        Owner = owner;
        Launch = launch;
        Limiter = limiter;
    }

    /// <summary>The broker, and therefore the circuit, that opened the session.</summary>
    internal AppBridgeBroker Owner { get; }

    /// <summary>The launch whose <c>(slug, install revision)</c> is the port's fixed authority.</summary>
    public AppFrameLaunch Launch { get; }

    /// <summary>The frame's rate and concurrency limits.</summary>
    public AppBridgeRateLimiter Limiter { get; }

    /// <summary>Whether the session was closed.</summary>
    public bool IsClosed => Volatile.Read(ref _closed) != 0;

    /// <summary>Closes the session; idempotent.</summary>
    public void Close() => Interlocked.Exchange(ref _closed, 1);
}
