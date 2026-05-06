using Microsoft.Extensions.Logging;
using MultiSiteManufacturing.Host.Domain;
using Orleans.Streams;

namespace MultiSiteManufacturing.Host.Dashboard;

/// <summary>
/// Cluster-wide Orleans stream subscribe + publish path for
/// <see cref="DashboardBroadcaster"/>. Every silo publishes each
/// routed / replicated fact onto the broadcast stream and subscribes
/// to it, so per-circuit fan-out happens locally on whichever silo
/// the Blazor session is attached to. See the class-level remarks on
/// <see cref="DashboardBroadcaster"/> for the architecture overview.
/// </summary>
public sealed partial class DashboardBroadcaster
{
    /// <summary>
    /// Tuneable publish-retry policy. A publish hitting a transient
    /// Azure Storage Queue hiccup (throttling, 500, brief network
    /// blip) should not silently drop a dashboard update — we retry
    /// with exponential backoff before logging and giving up. Values
    /// kept small: a dashboard update is time-sensitive, not worth
    /// holding onto for minutes.
    /// </summary>
    private static readonly TimeSpan[] PublishBackoff =
    {
        TimeSpan.FromMilliseconds(100),
        TimeSpan.FromMilliseconds(400),
        TimeSpan.FromSeconds(2),
    };

    /// <summary>
    /// Subscribe-retry policy. Queue provisioning, PubSubStore
    /// readiness, and silo-wide startup ordering can all delay the
    /// broadcast stream from being ready when the hosted service
    /// starts; we retry subscribe with bounded backoff rather than
    /// crashing the host.
    /// </summary>
    private static readonly TimeSpan[] SubscribeBackoff =
    {
        TimeSpan.FromMilliseconds(500),
        TimeSpan.FromSeconds(2),
        TimeSpan.FromSeconds(5),
        TimeSpan.FromSeconds(10),
        TimeSpan.FromSeconds(15),
    };

    /// <summary>
    /// Subscribes to the broadcast stream with bounded exponential
    /// backoff. Also wires an <c>onError</c> handler so a mid-flight
    /// stream failure triggers a fresh subscribe attempt on a
    /// background task — Azure Storage Queue streams can surface
    /// errors when the agent loses its lease or the queue is
    /// transiently unavailable.
    /// </summary>
    private async Task SubscribeWithRetryAsync(CancellationToken cancellationToken)
    {
        if (_broadcastStream is null)
        {
            return;
        }

        for (var attempt = 0; attempt <= SubscribeBackoff.Length; attempt++)
        {
            if (cancellationToken.IsCancellationRequested || _shutdownCts.IsCancellationRequested)
            {
                return;
            }
            try
            {
                _broadcastSubscription = await _broadcastStream.SubscribeAsync(
                    OnBroadcastReceived,
                    OnSubscriptionError);
                if (attempt > 0)
                {
                    _logger.LogInformation(
                        "Subscribed to dashboard broadcast stream after {Attempts} attempt(s)",
                        attempt + 1);
                }
                return;
            }
            catch (Exception ex) when (attempt < SubscribeBackoff.Length)
            {
                _logger.LogWarning(
                    ex,
                    "Dashboard broadcast subscribe failed (attempt {Attempt}); retrying in {Delay}",
                    attempt + 1,
                    SubscribeBackoff[attempt]);
                try
                {
                    await Task.Delay(SubscribeBackoff[attempt], cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Dashboard broadcast subscribe permanently failed after {Attempts} attempts; live dashboard updates disabled on this silo",
                    SubscribeBackoff.Length + 1);
                return;
            }
        }
    }

    /// <summary>
    /// Stream <c>onError</c> callback. Logs the failure and kicks off
    /// a background resubscribe so a transient queue-agent fault
    /// doesn't permanently silence the feed on this silo. The
    /// existing handle is dropped — <see cref="SubscribeWithRetryAsync"/>
    /// will allocate a fresh one.
    /// </summary>
    private Task OnSubscriptionError(Exception ex)
    {
        _logger.LogWarning(ex, "Dashboard broadcast stream reported an error; attempting resubscribe");
        _broadcastSubscription = null;
        _ = Task.Run(() => SubscribeWithRetryAsync(_shutdownCts.Token));
        return Task.CompletedTask;
    }

    private void OnFactForBroadcast(object? sender, Fact fact) => _ = PublishToBroadcastStreamAsync(fact);

    /// <summary>
    /// Publishes the fact to the cluster-wide broadcast stream so
    /// every silo's broadcaster — including this one — can fan it out
    /// to its locally-attached Blazor circuits. Fire-and-forget from
    /// the router's perspective: a publish failure must not propagate
    /// back into the router's synchronous fan-out. Retries transient
    /// storage-queue failures with bounded exponential backoff before
    /// giving up — a dropped publish means one Blazor update is lost,
    /// not a persistent feed outage.
    /// </summary>
    private async Task PublishToBroadcastStreamAsync(Fact fact)
    {
        var stream = _broadcastStream;
        if (stream is null)
        {
            return;
        }
        for (var attempt = 0; attempt <= PublishBackoff.Length; attempt++)
        {
            if (_shutdownCts.IsCancellationRequested)
            {
                return;
            }
            try
            {
                await stream.OnNextAsync(fact);
                if (attempt > 0)
                {
                    _logger.LogInformation(
                        "Published fact {FactId} after {Attempts} attempt(s)",
                        fact.FactId,
                        attempt + 1);
                }
                return;
            }
            catch (Exception ex) when (attempt < PublishBackoff.Length)
            {
                _logger.LogDebug(
                    ex,
                    "Transient publish failure for fact {FactId} (attempt {Attempt}); retrying in {Delay}",
                    fact.FactId,
                    attempt + 1,
                    PublishBackoff[attempt]);
                try
                {
                    await Task.Delay(PublishBackoff[attempt], _shutdownCts.Token);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to publish fact {FactId} to dashboard broadcast stream after {Attempts} attempts",
                    fact.FactId,
                    PublishBackoff.Length + 1);
                return;
            }
        }
    }

    /// <summary>
    /// Handles a fact delivered via the cluster-wide broadcast stream.
    /// Runs on every subscribed silo, so per-circuit fan-out happens
    /// locally wherever the Blazor session is attached, regardless of
    /// which silo originated the fact. Combines the part-summary /
    /// divergence fan-out (<see cref="PublishPartAsync"/>) with the
    /// site-activity fan-out in a single entry point so the two paths
    /// can't drift (e.g. one wired to the stream and the other still
    /// local-only).
    /// </summary>
    /// <remarks>
    /// Swallows exceptions so a single bad fact cannot poison the
    /// stream agent: if the handler throws, Orleans would retry
    /// delivery indefinitely, blocking every subsequent message on
    /// the same queue. Per-step fan-out already has its own
    /// try/catch; the top-level catch here is the last line of
    /// defence.
    /// </remarks>
    private async Task OnBroadcastReceived(Fact fact, StreamSequenceToken? token)
    {
        try
        {
            await PublishPartAsync(fact.Serial);
            FanOutSiteActivity(fact);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Dashboard fan-out threw for fact {FactId}; dropping to protect stream agent",
                fact.FactId);
        }
    }
}
