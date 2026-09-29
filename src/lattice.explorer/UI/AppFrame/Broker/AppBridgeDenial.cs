namespace Orleans.Lattice.Explorer.UI.Framing.Broker;

/// <summary>Why the broker refused or dropped a frame message; logged structurally, never with a key or value.</summary>
internal enum AppBridgeDenial
{
    /// <summary>The message was not a well-formed request envelope.</summary>
    Malformed = 0,

    /// <summary>The message exceeded the request size limit.</summary>
    Oversize = 1,

    /// <summary>The operation is not in the bridge vocabulary.</summary>
    UnknownOperation = 2,

    /// <summary>The install's consented bridge set does not grant the operation (over that tree).</summary>
    Unconsented = 3,

    /// <summary>The arguments carried a member the operation does not define.</summary>
    UnknownArgument = 4,

    /// <summary>The arguments were malformed or out of range.</summary>
    InvalidArguments = 5,

    /// <summary>The tree was not a logical tree name, for example a physical tree id.</summary>
    PhysicalTree = 6,

    /// <summary>The tree is not one the app declares.</summary>
    UndeclaredTree = 7,

    /// <summary>The frame exceeded its token-bucket rate.</summary>
    RateLimited = 8,

    /// <summary>The frame already had the maximum requests in flight.</summary>
    ConcurrencyLimited = 9,

    /// <summary>The session was closed or does not belong to this broker.</summary>
    Closed = 10,

    /// <summary>A collaborator the operation needs is not registered.</summary>
    CollaboratorMissing = 11,

    /// <summary>The cluster bridge refused the request.</summary>
    BridgeRefused = 12,

    /// <summary>The launch was revoked.</summary>
    Revoked = 13,
}
