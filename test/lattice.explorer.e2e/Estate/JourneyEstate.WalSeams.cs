using System.Net.Http.Json;
using System.Text.Json.Serialization;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

/// <summary>A kind of WAL materialiser pin the harness can plant on a tree (A12, issue #5382).</summary>
public enum JourneyWalPin
{
    /// <summary>A leaf published a pin at a WAL offset and crashed before it ever persisted a checkpoint: reclamation is blocked and does not clear on its own.</summary>
    Stranded,

    /// <summary>A leaf has not checkpointed yet, so its pin reports no offset: reclamation waits for the checkpoint.</summary>
    Unchecked,

    /// <summary>A pin whose consumer names no leaf the cluster can read: whether reclamation is blocked cannot be established.</summary>
    Unidentified,
}

/// <summary>
/// The WAL fault seams (A12, issue #5382): a target storage that stops answering a WAL move,
/// and the materialiser pins that hold a tree's WAL floor. Like every fault seam they are the
/// harness acting as the outside world, never a journey through a back door.
/// </summary>
public sealed partial class JourneyEstate
{
    /// <summary>
    /// Holds, on every running silo of <paramref name="region"/>, the writes and tail reads a WAL
    /// move makes to the secondary WAL provider for <paramref name="tree"/>, as a target storage
    /// account that has stopped answering would: a move to it waits in its copy phase, before
    /// its flip, until it is stopped or <see cref="ReleaseWalTargetAsync"/> is called. A hold
    /// lapses by itself after five minutes.
    /// </summary>
    /// <param name="tree">The logical tree.</param>
    /// <param name="region">The region.</param>
    public Task HoldWalTargetAsync(string tree, string region = West) => SetWalTargetAsync(tree, hold: true, region);

    /// <summary>Releases a hold <see cref="HoldWalTargetAsync"/> placed.</summary>
    /// <param name="tree">The logical tree.</param>
    /// <param name="region">The region.</param>
    public Task ReleaseWalTargetAsync(string tree, string region = West) => SetWalTargetAsync(tree, hold: false, region);

    /// <summary>
    /// Plants a WAL materialiser pin of <paramref name="kind"/> on <paramref name="tree"/>: the
    /// durable state a leaf leaves behind, read by the Explorer's WAL reclamation section as it
    /// would be in production. Remove it with <see cref="ReleaseWalPinAsync"/>.
    /// </summary>
    /// <param name="tree">The logical tree.</param>
    /// <param name="kind">The kind of pin.</param>
    /// <param name="region">The region.</param>
    /// <returns>The pin's consumer id.</returns>
    public async Task<string> PlantWalPinAsync(string tree, JourneyWalPin kind, string region = West)
    {
        RequireFaults();
        ArgumentException.ThrowIfNullOrEmpty(tree);
        var silo = RegionOf(region).Silos.First(candidate => candidate.IsRunning);
        using var response = await _probe.PostAsync(new Uri(
            $"http://127.0.0.1:{silo.HttpPort}/e2e/wal-pin?tree={Uri.EscapeDataString(tree)}&kind={kind.ToString().ToLowerInvariant()}"), null);
        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException($"Planting a {kind} WAL pin on '{tree}' failed: {(int)response.StatusCode} {await response.Content.ReadAsStringAsync()}");
        }

        return (await response.Content.ReadFromJsonAsync<PlantedPin>())?.ConsumerId
            ?? throw new InvalidOperationException("The silo did not name the pin it planted.");
    }

    /// <summary>Removes a pin <see cref="PlantWalPinAsync"/> planted, and the leaf record behind it.</summary>
    /// <param name="tree">The logical tree.</param>
    /// <param name="consumerId">The pin's consumer id.</param>
    /// <param name="region">The region.</param>
    public async Task ReleaseWalPinAsync(string tree, string consumerId, string region = West)
    {
        RequireFaults();
        var silo = RegionOf(region).Silos.First(candidate => candidate.IsRunning);
        using var response = await _probe.DeleteAsync(new Uri(
            $"http://127.0.0.1:{silo.HttpPort}/e2e/wal-pin?tree={Uri.EscapeDataString(tree)}&consumerId={Uri.EscapeDataString(consumerId)}"));
        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException($"Releasing the WAL pin {consumerId} on '{tree}' failed: {(int)response.StatusCode} {await response.Content.ReadAsStringAsync()}");
        }
    }

    private async Task SetWalTargetAsync(string tree, bool hold, string region)
    {
        RequireFaults();
        ArgumentException.ThrowIfNullOrEmpty(tree);
        foreach (var silo in RegionOf(region).Silos.Where(candidate => candidate.IsRunning))
        {
            using var response = await _probe.PostAsync(new Uri(
                $"http://127.0.0.1:{silo.HttpPort}/e2e/wal-target?tree={Uri.EscapeDataString(tree)}&hold={(hold ? "true" : "false")}"), null);
            if (!response.IsSuccessStatusCode)
            {
                throw new InvalidOperationException(
                    $"{(hold ? "Holding" : "Releasing")} the WAL target of '{tree}' on silo {silo.Index} failed: {(int)response.StatusCode} {await response.Content.ReadAsStringAsync()}");
            }
        }
    }

    private sealed record PlantedPin([property: JsonPropertyName("consumerId")] string ConsumerId);
}
