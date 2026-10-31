namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C2;

/// <summary>One replication link as the Replication area draws it.</summary>
/// <param name="Health">The health cell's text: <c>Healthy</c>, <c>Lagging</c>, <c>Stalled</c> or <c>Unknown</c>.</param>
/// <param name="EntriesBehind">The entries behind, or <see langword="null"/> when the cell holds no number.</param>
/// <param name="StallReason">The stall-reason cell's text, empty when there is none.</param>
/// <param name="Errors">The consecutive-errors cell's text.</param>
/// <param name="Text">The whole row, for messages.</param>
internal sealed record ReplicationLinkReading(string Health, long? EntriesBehind, string StallReason, string Errors, string Text)
{
    /// <summary>Whether the link reads Healthy.</summary>
    public bool IsHealthy => Health.Contains("Healthy", StringComparison.Ordinal);

    /// <summary>Whether the link reads Lagging or Stalled: degraded, with a verdict.</summary>
    public bool IsDegraded => Health.Contains("Lagging", StringComparison.Ordinal) || Health.Contains("Stalled", StringComparison.Ordinal);
}
