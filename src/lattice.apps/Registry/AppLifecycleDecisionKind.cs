namespace Orleans.Lattice.Apps;

/// <summary>The kind of an <see cref="AppLifecycleDecision"/>.</summary>
internal enum AppLifecycleDecisionKind
{
    /// <summary>Write a new record revision in the decided state.</summary>
    Apply,

    /// <summary>The target state already holds; write nothing and succeed.</summary>
    NoOp,

    /// <summary>The transition is illegal; write nothing and report the reason.</summary>
    Reject,
}
