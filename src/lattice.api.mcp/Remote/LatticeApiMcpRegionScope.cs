namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The ambient, per-invocation selected target region under the
/// <c>Orleans.Lattice.Api.Mcp</c> remote-host topology. Set once at the single
/// narrowest tool seam (<see cref="CredentialStampingTool"/>) when a caller
/// supplies an explicit <c>region</c>, and read by the region-routing gRPC call
/// invoker so every facade group's outbound call for that invocation is dispatched
/// to the selected region's channel.
/// </summary>
/// <remarks>
/// <para>
/// The current value is <see langword="null"/> for the default-region path (no
/// <c>region</c> supplied), which the routing invoker treats as its cached default
/// channel - so an unadorned call adds zero region-resolution work and behaves
/// byte-for-byte as before. A non-<see langword="null"/> value is a validated
/// region id the router already confirmed serves the invoked group.
/// </para>
/// <para>
/// Backed by an <see cref="AsyncLocal{T}"/> so the selection flows across the
/// awaited facade call without leaking to a sibling request: the scope is entered
/// for exactly one tool invocation and restored on dispose, preserving per-circuit
/// isolation.
/// </para>
/// </remarks>
internal static class LatticeApiMcpRegionScope
{
    private static readonly AsyncLocal<string?> Selected = new();

    /// <summary>
    /// The region id selected for the current invocation, or
    /// <see langword="null"/> when the default (current) region applies.
    /// </summary>
    public static string? Current => Selected.Value;

    /// <summary>
    /// Enters a region scope for one tool invocation, selecting
    /// <paramref name="regionId"/> for the duration. Dispose the returned handle
    /// when the invocation completes to restore the prior selection.
    /// </summary>
    /// <param name="regionId">The validated target region id to select.</param>
    /// <returns>A disposable that restores the prior selection on dispose.</returns>
    public static IDisposable Enter(string regionId)
    {
        ArgumentNullException.ThrowIfNull(regionId);
        var previous = Selected.Value;
        Selected.Value = regionId;
        return new Restore(previous);
    }

    private sealed class Restore : IDisposable
    {
        private readonly string? _previous;
        private bool _disposed;

        public Restore(string? previous) => _previous = previous;

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            Selected.Value = _previous;
        }
    }
}
