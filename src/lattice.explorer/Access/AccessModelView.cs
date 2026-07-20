using Orleans.Lattice.Api.Auth;

namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// An Access-area view of the cluster's best-effort access model: the active
/// authentication mode, whether authorization rules are enforced, and whether an
/// identity directory is available (with its provider id and operator-facing
/// explanation). Carries the read <see cref="Status"/> and an optional
/// <see cref="Message"/> so the service can fold a server denial or a transport
/// failure into a safe, UI-facing snapshot (<see cref="Unavailable"/>) rather than
/// throwing.
/// </summary>
public sealed record AccessModelView
{
    /// <summary>The outcome category of the read.</summary>
    public required AccessOperationStatus Status { get; init; }

    /// <summary>A human-readable message, populated on a denial or failure.</summary>
    public string Message { get; init; } = string.Empty;

    /// <summary>The best-effort active authentication mode.</summary>
    public AccessAuthenticationMode AuthenticationMode { get; init; } = AccessAuthenticationMode.Unknown;

    /// <summary><see langword="true"/> when membership and authorization rules are enforced on the data path.</summary>
    public bool RulesEnforced { get; init; }

    /// <summary><see langword="true"/> when a searchable identity directory is configured.</summary>
    public bool DirectoryAvailable { get; init; }

    /// <summary>The stable id of the configured identity-directory provider.</summary>
    public string DirectoryProviderId { get; init; } = string.Empty;

    /// <summary>The operator-facing explanation of what a valid principal id is for this deployment.</summary>
    public string DirectoryExplanation { get; init; } = string.Empty;

    /// <summary>
    /// <see langword="true"/> when locally-defined group membership contributes to a
    /// subject's effective groups at authorization time; <see langword="false"/> when
    /// the cluster resolves group membership solely from the identity-provider token
    /// (a <c>TokenOnly</c> merge mode), which makes the local group / member editing
    /// surface inert.
    /// </summary>
    public bool LocalMembershipEffective { get; init; }

    /// <summary><see langword="true"/> when the read succeeded.</summary>
    public bool IsSuccess => Status == AccessOperationStatus.Succeeded;

    /// <summary>Builds a successful view from a resolved <paramref name="descriptor"/>.</summary>
    /// <param name="descriptor">The resolved access-model descriptor. Must not be <see langword="null"/>.</param>
    public static AccessModelView FromDescriptor(AccessModelDescriptor descriptor)
    {
        ArgumentNullException.ThrowIfNull(descriptor);
        return new AccessModelView
        {
            Status = AccessOperationStatus.Succeeded,
            AuthenticationMode = descriptor.AuthenticationMode,
            RulesEnforced = descriptor.RulesEnforced,
            DirectoryAvailable = descriptor.DirectoryAvailable,
            DirectoryProviderId = descriptor.DirectoryProviderId,
            DirectoryExplanation = descriptor.DirectoryExplanation,
            LocalMembershipEffective = descriptor.LocalMembershipEffective,
        };
    }

    /// <summary>
    /// The safe snapshot shown when the access model cannot be read (a denial or a
    /// transport failure): an unknown authentication mode, no rule enforcement, and
    /// no directory available.
    /// </summary>
    public static AccessModelView Unavailable { get; } = new() { Status = AccessOperationStatus.Failed };
}
