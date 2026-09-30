namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>
/// The stages of the staged install flow (epic decision E15). The flow is
/// resolve, review manifest and consent, bind roles, confirm the ceiling, install,
/// then optionally enable. <see cref="Acquiring"/> and <see cref="Verifying"/>
/// exist in the machine but are only entered for a source that advertises
/// <c>RequiresAcquisition</c>, so a dynamic source needs no redesign.
/// </summary>
internal enum AppInstallStage
{
    /// <summary>The description is being read from a source that holds it already.</summary>
    Resolving,

    /// <summary>A dynamic source is acquiring the app version.</summary>
    Acquiring,

    /// <summary>The acquired version is being checked against its manifest digests.</summary>
    Verifying,

    /// <summary>The operator reviews identity, provenance and what the app asks for.</summary>
    Review,

    /// <summary>Each declared role is bound to a membership group.</summary>
    BindRoles,

    /// <summary>The operator edits the ceiling, exception scopes and bridge grants and sees what would fail activation.</summary>
    ConfirmCeiling,

    /// <summary>The install, upgrade or consent update is running.</summary>
    Installing,

    /// <summary>The app is installed (not enabled).</summary>
    Installed,

    /// <summary>The app is being enabled.</summary>
    Enabling,

    /// <summary>The app is enabled.</summary>
    Enabled,

    /// <summary>The source does not offer the app or version.</summary>
    NotFound,

    /// <summary>A step failed; the flow can return to the stage it failed from.</summary>
    Failed,

    /// <summary>
    /// Changing the installed version's role bindings: the operator compares each role's
    /// recorded and proposed group and confirms before anything is applied.
    /// </summary>
    ConfirmBindings,

    /// <summary>The installed version's role bindings were changed.</summary>
    Rebound,
}
