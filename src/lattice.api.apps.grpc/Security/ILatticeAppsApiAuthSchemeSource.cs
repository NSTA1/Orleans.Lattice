namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>Supplies public endpoint sign-in metadata, with no credentials or caller-specific data.</summary>
public interface ILatticeAppsApiAuthSchemeSource
{
    /// <summary>Returns the public advertisement for unauthenticated discovery.</summary>
    AuthSchemeAdvertisement GetAdvertisement();
}
