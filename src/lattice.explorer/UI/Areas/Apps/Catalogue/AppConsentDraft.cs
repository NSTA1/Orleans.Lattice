using System.Collections.Immutable;
using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>
/// A consent an operator is drafting or has recorded: the capability ceiling's
/// operations, the approved exception scopes outside <c>a/{slug}/</c>, and the
/// consented UI bridge grants.
/// </summary>
/// <param name="Operations">The ceiling's operation mask.</param>
/// <param name="Scopes">The approved exception scopes.</param>
/// <param name="BridgeGrants">The consented bridge grants.</param>
internal sealed record AppConsentDraft(
    LatticeOperation Operations,
    ImmutableArray<AppExceptionScope> Scopes,
    ImmutableArray<AppUiBridgeGrantDescriptor> BridgeGrants)
{
    /// <summary>The draft that covers exactly what <paramref name="app"/> asks for.</summary>
    /// <param name="app">The described app.</param>
    public static AppConsentDraft Requested(AppDescriptor app) => new(
        AppConsentAnalysis.RequiredOperations(app),
        AppConsentAnalysis.RequiredScopes(app),
        AppConsentAnalysis.RequestedBridge(app));

    /// <summary>The draft a recorded consent amounts to.</summary>
    /// <param name="consent">The recorded consent.</param>
    public static AppConsentDraft FromReport(AppConsentReport consent)
    {
        ArgumentNullException.ThrowIfNull(consent);
        return new(consent.Ceiling.AllowedOperations, consent.Ceiling.ApprovedExceptionScopes, consent.BridgeGrants ?? []);
    }

    /// <summary>The ceiling to send.</summary>
    public AppCapabilityCeilingDescriptor ToCeiling() => new()
    {
        AllowedOperations = Operations,
        ApprovedExceptionScopes = Scopes,
    };
}
