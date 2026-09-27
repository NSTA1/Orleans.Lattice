namespace Orleans.Lattice.Apps;

/// <summary>
/// The lifecycle operation an <see cref="IAppActivationPipeline"/> run performed.
/// </summary>
[GenerateSerializer, Alias(AppActivationTypeAliases.AppActivationOperation)]
public enum AppActivationOperation
{
    /// <summary>Activate an installed app: provision its trees, emit its rules, and mark it enabled.</summary>
    Enable = 0,

    /// <summary>Withdraw an enabled app's rules and mark it disabled; its trees and data are kept.</summary>
    Disable = 1,

    /// <summary>Withdraw an app's rules, soft-delete its structural trees, and mark it uninstalled.</summary>
    Uninstall = 2,

    /// <summary>
    /// Re-apply the app's current registry state without changing it: an enabled app is
    /// re-activated (rules and trees re-converged), any other state has its owned rules withdrawn.
    /// </summary>
    Reconcile = 3,
}
