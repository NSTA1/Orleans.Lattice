using System.Collections.Immutable;
using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>Why an install or activation under a draft consent would fail.</summary>
internal enum AppActivationIssueKind
{
    /// <summary>A role requests operations the ceiling excludes.</summary>
    CeilingExceeded,

    /// <summary>A scope outside <c>a/{slug}/</c> is not approved.</summary>
    ScopeNotApproved,

    /// <summary>The UI requests a bridge grant that is not consented.</summary>
    BridgeConsentRequired,

    /// <summary>A declared tree is owned by something else, so install is refused.</summary>
    TreeOwnershipConflict,
}
