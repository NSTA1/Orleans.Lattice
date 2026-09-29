using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.UI.Areas.Replication;

/// <summary>
/// How a <see cref="ReplicationLinkHealth"/> is drawn and ordered: its state role
/// (which carries the glyph and the colour), its text label, its severity for
/// sorting and roll-ups, and its lower-case query value for <c>?health=</c>.
/// </summary>
internal static class ReplicationHealth
{
    /// <summary>Every health, worst first.</summary>
    public static IReadOnlyList<ReplicationLinkHealth> WorstFirst { get; } =
    [
        ReplicationLinkHealth.Stalled,
        ReplicationLinkHealth.Lagging,
        ReplicationLinkHealth.Unknown,
        ReplicationLinkHealth.Healthy,
    ];

    /// <summary>The state role that draws <paramref name="health"/>'s glyph and colour.</summary>
    /// <param name="health">The link health.</param>
    public static LtStateRole Role(ReplicationLinkHealth health) => health switch
    {
        ReplicationLinkHealth.Healthy => LtStateRole.Healthy,
        ReplicationLinkHealth.Lagging => LtStateRole.Lagging,
        ReplicationLinkHealth.Stalled => LtStateRole.Stalled,
        _ => LtStateRole.Unknown,
    };

    /// <summary>The text label shown beside the glyph.</summary>
    /// <param name="health">The link health.</param>
    public static string Label(ReplicationLinkHealth health) => health switch
    {
        ReplicationLinkHealth.Healthy => "Healthy",
        ReplicationLinkHealth.Lagging => "Lagging",
        ReplicationLinkHealth.Stalled => "Stalled",
        _ => "Unknown",
    };

    /// <summary>
    /// The severity used to sort and to roll links up: stalled is the worst, then
    /// lagging, then unknown (no verdict yet), then healthy.
    /// </summary>
    /// <param name="health">The link health.</param>
    public static int Severity(ReplicationLinkHealth health) => health switch
    {
        ReplicationLinkHealth.Stalled => 3,
        ReplicationLinkHealth.Lagging => 2,
        ReplicationLinkHealth.Healthy => 0,
        _ => 1,
    };

    /// <summary>The worse of two healths.</summary>
    /// <param name="left">One health.</param>
    /// <param name="right">The other.</param>
    public static ReplicationLinkHealth Worse(ReplicationLinkHealth left, ReplicationLinkHealth right) =>
        Severity(right) > Severity(left) ? right : left;

    /// <summary>The lower-case value <paramref name="health"/> takes in <c>?health=</c>.</summary>
    /// <param name="health">The link health.</param>
    public static string QueryValue(ReplicationLinkHealth health) => Label(health).ToLowerInvariant();

    /// <summary>Reads a <c>?health=</c> value, case-insensitively.</summary>
    /// <param name="value">The query value.</param>
    /// <param name="health">The health it names.</param>
    /// <returns><see langword="true"/> when <paramref name="value"/> names a health.</returns>
    public static bool TryParse(string? value, out ReplicationLinkHealth health)
    {
        foreach (var candidate in WorstFirst)
        {
            if (string.Equals(QueryValue(candidate), value, StringComparison.OrdinalIgnoreCase))
            {
                health = candidate;
                return true;
            }
        }

        health = ReplicationLinkHealth.Unknown;
        return false;
    }
}
