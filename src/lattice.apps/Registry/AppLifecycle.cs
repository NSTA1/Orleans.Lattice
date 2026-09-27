namespace Orleans.Lattice.Apps;

/// <summary>
/// The pure lifecycle transition table of the app registry. Given the current record
/// (or its absence) and a requested action, it decides whether to apply the action and
/// to which state, treat it as an idempotent no-op, or reject it. It touches no storage
/// and allocates nothing: every diagnostic is a constant.
/// </summary>
internal static class AppLifecycle
{
    private const string NotInstalledMessage = "The app is not installed for this tenant.";
    private const string AlreadyInstalledMessage =
        "The app is already installed for this tenant; upgrade it to re-consent, or uninstall it first.";
    private const string DisableRequiresEnabledMessage = "Only an enabled app can be disabled.";
    private const string CeilingNotPinnedMessage =
        "The stored capability ceiling was not consented for the stored version; upgrade the app to re-consent before enabling it.";

    /// <summary>Decides the outcome of <paramref name="action"/> against <paramref name="current"/>.</summary>
    /// <param name="current">The stored record, or <c>null</c> when absent.</param>
    /// <param name="action">The requested transition.</param>
    /// <returns>The decision.</returns>
    internal static AppLifecycleDecision Evaluate(AppRegistryRecord? current, AppLifecycleAction action)
    {
        var live = current is not null && current.State != AppRegistryLifecycleState.Uninstalled;

        switch (action)
        {
            case AppLifecycleAction.Install:
                return live
                    ? AppLifecycleDecision.Reject(AppRegistryTransitionError.AlreadyInstalled, AlreadyInstalledMessage)
                    : AppLifecycleDecision.Apply(AppRegistryLifecycleState.Installed);

            case AppLifecycleAction.Upgrade:
                return live
                    ? AppLifecycleDecision.Apply(current!.State)
                    : AppLifecycleDecision.Reject(AppRegistryTransitionError.NotInstalled, NotInstalledMessage);

            case AppLifecycleAction.Enable:
                if (!live)
                {
                    return AppLifecycleDecision.Reject(AppRegistryTransitionError.NotInstalled, NotInstalledMessage);
                }

                if (!current!.IsCeilingPinnedToVersion)
                {
                    return AppLifecycleDecision.Reject(AppRegistryTransitionError.CeilingNotPinned, CeilingNotPinnedMessage);
                }

                return current.State == AppRegistryLifecycleState.Enabled
                    ? AppLifecycleDecision.NoOp
                    : AppLifecycleDecision.Apply(AppRegistryLifecycleState.Enabled);

            case AppLifecycleAction.Disable:
                if (!live)
                {
                    return AppLifecycleDecision.Reject(AppRegistryTransitionError.NotInstalled, NotInstalledMessage);
                }

                return current!.State switch
                {
                    AppRegistryLifecycleState.Enabled => AppLifecycleDecision.Apply(AppRegistryLifecycleState.Disabled),
                    AppRegistryLifecycleState.Disabled => AppLifecycleDecision.NoOp,
                    _ => AppLifecycleDecision.Reject(AppRegistryTransitionError.InvalidTransition, DisableRequiresEnabledMessage),
                };

            case AppLifecycleAction.Uninstall:
                if (current is null)
                {
                    return AppLifecycleDecision.Reject(AppRegistryTransitionError.NotInstalled, NotInstalledMessage);
                }

                return live
                    ? AppLifecycleDecision.Apply(AppRegistryLifecycleState.Uninstalled)
                    : AppLifecycleDecision.NoOp;

            default:
                throw new ArgumentOutOfRangeException(nameof(action), action, "Unknown app lifecycle action.");
        }
    }
}
