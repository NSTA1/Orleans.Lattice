using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace MultiSiteManufacturing.Host.Replication;

/// <summary>
/// HTTP client for shipping a <see cref="ReplicationBatch"/> to a peer
/// cluster. Thin wrapper over <see cref="HttpClient"/> — the DI
/// registration supplies a named typed client so connection pooling
/// is per-peer.
/// </summary>
/// <remarks>
/// <para>
/// <b>Failover.</b> Each peer cluster carries a list of base URLs
/// (<see cref="ReplicationPeer.BaseUrls"/>). <see cref="SendAsync"/>
/// tries them in order and moves on to the next on any transport
/// failure or non-success status, so failed URLs never stall
/// shipping. Only when <i>every</i> URL fails does the call raise an
/// exception and bump the <see cref="ReplicationActivityTracker"/>
/// error counter.
/// </para>
/// <para>
/// <b>Deployment shapes.</b> The BaseUrls list is flexible on purpose:
/// <list type="bullet">
/// <item>
/// <description>
/// <b>Single LB endpoint (default in Docker Compose).</b> One URL
/// per peer pointing at the peer cluster's Traefik (or any L7 LB).
/// Traefik's round-robin router for <c>/replicate/*</c> plus its
/// active health check handles per-silo failover inside the peer
/// cluster within ~2 s, so the client just needs one URL.
/// </description>
/// </item>
/// <item>
/// <description>
/// <b>Explicit per-silo fan-out (localhost dev, no LB).</b> One URL
/// per silo. The client walks the list until one silo accepts the
/// batch. Matches the shape of <c>appsettings.cluster.*.json</c>
/// when running outside Docker.
/// </description>
/// </item>
/// <item>
/// <description>
/// <b>Multi-zone failover (advanced).</b> One URL per availability
/// zone, each pointing at that zone's regional LB (for example
/// <c>https://eu-az1.example.com</c>,
/// <c>https://eu-az2.example.com</c>). The client stays on
/// the first zone while it's healthy and only falls over to the next
/// zone when every silo behind the current zone's LB has failed.
/// Because the list is walked in order and short-circuits on first
/// success, the primary zone absorbs all traffic under nominal
/// conditions — ideal when zones are charged for cross-zone
/// bandwidth.
/// </description>
/// </item>
/// </list>
/// </para>
/// <para>
/// <b>Authentication</b> is a shared-secret bearer token in the
/// <see cref="ReplicationConstants.AuthHeader"/> header — adequate
/// for the sample and the localhost-to-localhost demo. Production
/// deployments replace this with mTLS or Entra ID.
/// </para>
/// <para>
/// <b>FUTURE seam.</b> When the library ships cross-tree continuous
/// merge, this class is replaced by a direct merge invocation (no
/// HTTP hop, no batch envelope). The wire envelope stays as a test
/// fixture.
/// </para>
/// </remarks>
internal sealed class ReplicationHttpClient(
    HttpClient http,
    ReplicationTopology topology,
    ReplicationActivityTracker activity,
    ILogger<ReplicationHttpClient> logger)
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = false,
    };

    /// <summary>
    /// Ships <paramref name="batch"/> to <paramref name="peer"/> and
    /// returns the peer's ack. Iterates the peer's
    /// <see cref="ReplicationPeer.BaseUrls"/> in order and returns on
    /// the first 2xx; throws only when every URL has failed.
    /// </summary>
    public async Task<ReplicationAck> SendAsync(
        ReplicationPeer peer,
        ReplicationBatch batch,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(peer);
        ArgumentNullException.ThrowIfNull(batch);

        if (peer.BaseUrls.Count == 0)
        {
            throw new InvalidOperationException($"Peer '{peer.Name}' has no BaseUrls configured.");
        }

        Exception? lastError = null;
        for (var i = 0; i < peer.BaseUrls.Count; i++)
        {
            var baseUrl = peer.BaseUrls[i];
            var url = new Uri(baseUrl, $"/replicate/{Uri.EscapeDataString(batch.Tree)}");
            using var req = new HttpRequestMessage(HttpMethod.Post, url)
            {
                Content = JsonContent.Create(batch, options: JsonOptions),
            };
            req.Headers.Add(ReplicationConstants.AuthHeader, topology.SharedSecret);

            try
            {
                using var resp = await http.SendAsync(req, cancellationToken).ConfigureAwait(false);
                if (!resp.IsSuccessStatusCode)
                {
                    var body = await resp.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
                    logger.LogWarning(
                        "Replication POST to {Peer} [{Url}] for tree {Tree} failed: {Status} {Body}",
                        peer.Name, baseUrl, batch.Tree, (int)resp.StatusCode, body);
                    lastError = new HttpRequestException(
                        $"Peer {peer.Name} [{baseUrl}] replied {(int)resp.StatusCode}.");
                    continue;
                }

                var ack = await resp.Content.ReadFromJsonAsync<ReplicationAck>(JsonOptions, cancellationToken).ConfigureAwait(false)
                    ?? throw new InvalidOperationException($"Peer {peer.Name} [{baseUrl}] returned an empty ack.");

                activity.RecordSent(peer.Name, ack.Applied);
                return ack;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogDebug(ex,
                    "Replication POST to {Peer} [{Url}] failed; trying next URL.",
                    peer.Name, baseUrl);
                lastError = ex;
            }
        }

        activity.RecordSendError();
        throw new HttpRequestException(
            $"All {peer.BaseUrls.Count} URL(s) for peer '{peer.Name}' failed.", lastError);
    }
}
