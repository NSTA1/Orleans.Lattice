namespace Orleans.Lattice.Membership.Entra.Graph;

/// <summary>
/// One page of <see cref="EntraDirectoryRecord"/> results returned by the Graph
/// query seam <see cref="IEntraGraphDirectoryClient"/>, carrying the matched
/// records plus the raw Microsoft Graph <c>@odata.nextLink</c> (or <c>null</c> on
/// the final page). <see cref="EntraGraphIdentityDirectory"/> maps these to
/// <see cref="DirectoryPrincipal"/> values and reshapes the continuation token for
/// the caller.
/// </summary>
/// <param name="Records">
/// The matched records in Graph-defined order. Empty when nothing matched; never
/// <c>null</c>.
/// </param>
/// <param name="ContinuationToken">
/// The raw Microsoft Graph next-link to fetch the following page, or <c>null</c>
/// when this is the final page.
/// </param>
internal sealed record EntraDirectoryPage(
    IReadOnlyList<EntraDirectoryRecord> Records,
    string? ContinuationToken)
{
    /// <summary>
    /// A shared empty page (no records, no continuation token), so the
    /// nothing-matched path allocates nothing per call.
    /// </summary>
    public static EntraDirectoryPage Empty { get; } = new(Array.Empty<EntraDirectoryRecord>(), null);
}
