namespace Orleans.Lattice.Apps;

/// <summary>Why an app-registry lifecycle transition was rejected.</summary>
[GenerateSerializer, Alias(AppRegistryTypeAliases.AppRegistryTransitionError)]
public enum AppRegistryTransitionError
{
    /// <summary>The transition was applied, or was an idempotent no-op.</summary>
    None = 0,

    /// <summary>The app has no install record, or its record is <see cref="AppRegistryLifecycleState.Uninstalled"/>.</summary>
    NotInstalled = 1,

    /// <summary>An install was requested for an app that is already installed.</summary>
    AlreadyInstalled = 2,

    /// <summary>The transition is not legal from the app's current lifecycle state.</summary>
    InvalidTransition = 3,

    /// <summary>
    /// The stored ceiling is not pinned to the stored version, so the app cannot be
    /// enabled until it is re-consented through an upgrade.
    /// </summary>
    CeilingNotPinned = 4,

    /// <summary>
    /// Competing writers kept changing the record, and the bounded optimistic-concurrency
    /// retry budget was exhausted. Retrying later is safe.
    /// </summary>
    ConcurrencyConflict = 5,
}
