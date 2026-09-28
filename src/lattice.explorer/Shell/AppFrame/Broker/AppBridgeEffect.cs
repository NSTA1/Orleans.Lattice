namespace Orleans.Lattice.Explorer.Shell.Framing.Broker;

/// <summary>What the frame host must do after the broker handled a message, beyond posting its reply.</summary>
internal enum AppBridgeEffect
{
    /// <summary>Nothing more.</summary>
    None = 0,

    /// <summary>The frame reported its in-app path; <see cref="AppBridgeOutcome.Argument"/> carries it.</summary>
    NavSync = 1,

    /// <summary>
    /// The launch no longer holds; <see cref="AppBridgeOutcome.Argument"/> carries the
    /// <c>lattice.revoked</c> reason. The session is already closed; the host posts the event,
    /// closes the port and replaces the frame.
    /// </summary>
    Revoked = 2,
}
