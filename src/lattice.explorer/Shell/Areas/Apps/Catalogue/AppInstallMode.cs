namespace Orleans.Lattice.Explorer.Shell.Areas.Apps.Catalogue;

/// <summary>What the staged flow does once the review is approved.</summary>
internal enum AppInstallMode
{
    /// <summary>The app is not installed in this tenant: install it.</summary>
    Install,

    /// <summary>Another version is installed: upgrade to the reviewed one.</summary>
    Upgrade,

    /// <summary>The reviewed version is the installed one: record a new consent for it.</summary>
    Reconsent,
}
