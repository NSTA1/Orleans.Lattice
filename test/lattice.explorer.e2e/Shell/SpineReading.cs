namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;

/// <summary>What the directory spine offers the signed-in caller, read once it has loaded.</summary>
/// <param name="Stops">Every stop drawn, by area key, in directory order (<c>home</c> first).</param>
/// <param name="Open">The stops the caller can open; the rest are drawn with the reason they cannot.</param>
public sealed record SpineReading(IReadOnlyList<string> Stops, IReadOnlyList<string> Open)
{
    /// <summary>The stops drawn but not open to the caller.</summary>
    public IReadOnlyList<string> Unavailable => [.. Stops.Except(Open, StringComparer.Ordinal)];

    /// <summary>Whether the caller can open <paramref name="area"/> from the spine.</summary>
    /// <param name="area">The area key.</param>
    public bool CanOpen(string area) => Open.Contains(area, StringComparer.Ordinal);

    /// <inheritdoc />
    public override string ToString() => $"open [{string.Join(", ", Open)}], unavailable [{string.Join(", ", Unavailable)}]";
}
