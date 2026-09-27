namespace Orleans.Lattice.Apps;

/// <summary>A decision of the <see cref="AppLifecycle"/> transition table.</summary>
internal readonly record struct AppLifecycleDecision
{
    private AppLifecycleDecision(
        AppLifecycleDecisionKind kind,
        AppRegistryLifecycleState nextState,
        AppRegistryTransitionError error,
        string? message)
    {
        Kind = kind;
        NextState = nextState;
        Error = error;
        Message = message;
    }

    /// <summary>An idempotent no-op: the target state already holds, so nothing is written.</summary>
    internal static AppLifecycleDecision NoOp { get; } =
        new(AppLifecycleDecisionKind.NoOp, default, AppRegistryTransitionError.None, null);

    /// <summary>Whether to apply, skip, or reject the transition.</summary>
    internal AppLifecycleDecisionKind Kind { get; }

    /// <summary>The state to write when <see cref="Kind"/> is <see cref="AppLifecycleDecisionKind.Apply"/>.</summary>
    internal AppRegistryLifecycleState NextState { get; }

    /// <summary>The rejection reason when <see cref="Kind"/> is <see cref="AppLifecycleDecisionKind.Reject"/>.</summary>
    internal AppRegistryTransitionError Error { get; }

    /// <summary>The constant rejection diagnostic, or <c>null</c>.</summary>
    internal string? Message { get; }

    /// <summary>Creates an apply decision.</summary>
    /// <param name="nextState">The state to write.</param>
    /// <returns>The decision.</returns>
    internal static AppLifecycleDecision Apply(AppRegistryLifecycleState nextState) =>
        new(AppLifecycleDecisionKind.Apply, nextState, AppRegistryTransitionError.None, null);

    /// <summary>Creates a rejection decision.</summary>
    /// <param name="error">The rejection reason.</param>
    /// <param name="message">The constant diagnostic.</param>
    /// <returns>The decision.</returns>
    internal static AppLifecycleDecision Reject(AppRegistryTransitionError error, string message) =>
        new(AppLifecycleDecisionKind.Reject, default, error, message);
}
