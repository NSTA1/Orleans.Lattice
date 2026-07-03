namespace Orleans.Lattice.Membership.Entra;

/// <summary>
/// A marker service registered by
/// <see cref="LatticeEntraServiceCollectionExtensions.AddEntraCredentialAuthenticator"/>.
/// Its presence lets the <c>Orleans.Lattice.Membership.Entra.Graph</c> add-on
/// verify the Entra authenticator was registered before its group resolver.
/// </summary>
internal sealed class EntraAuthenticatorRegistrationMarker
{
}
