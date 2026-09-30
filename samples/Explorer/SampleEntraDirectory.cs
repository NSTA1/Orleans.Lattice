namespace Orleans.Lattice.Samples.Explorer;

/// <summary>
/// The Microsoft Entra app registration that backs the Access area's identity
/// directory in the opt-in Entra mode.
/// </summary>
/// <param name="TenantId">The Entra tenant (directory) id.</param>
/// <param name="ClientId">The app registration's client id.</param>
/// <param name="ClientSecret">The app registration's client secret.</param>
internal sealed record SampleEntraDirectory(string TenantId, string ClientId, string ClientSecret)
{
    /// <inheritdoc />
    public override string ToString() => $"SampleEntraDirectory {{ TenantId = {TenantId}, ClientId = {ClientId} }}";
}
