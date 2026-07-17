using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// The Access-area create-form state machine, extracted from the Razor view so it
/// is unit-testable without a component-render harness. Holds the cluster's
/// best-effort <see cref="AccessModelView"/> and turns it into the create form's
/// decisions: whether a new principal id must be validated against a directory and
/// fail closed when it does not exist (<see cref="ValidateAsync"/>), what a valid
/// id is for this deployment (<see cref="DirectoryExplanation"/>), and whether the
/// active authorizer actually enforces the recorded rules and membership
/// (<see cref="ShowEnforcementNotice"/>). The view binds to this model and keeps
/// the existing capability-aware disabling around it.
/// </summary>
public sealed class AccessCreateModel
{
    /// <summary>The inline reason shown when a create is blocked because the id is not in the directory.</summary>
    public const string NoSuchPrincipalReason = "No such principal in the directory.";

    /// <summary>The banner shown when the active authorizer records but does not enforce rules and membership.</summary>
    public const string EnforcementNoticeText =
        "Rules and membership are recorded but not enforced by the active authorizer.";

    private readonly IMembershipAdminService _membership;

    /// <summary>Creates a model over the membership admin service the resolve runs over.</summary>
    /// <param name="membership">The membership admin service. Must not be <see langword="null"/>.</param>
    public AccessCreateModel(IMembershipAdminService membership)
    {
        ArgumentNullException.ThrowIfNull(membership);
        _membership = membership;
    }

    /// <summary>The last applied access-model snapshot; the safe unavailable snapshot until one is applied.</summary>
    public AccessModelView Model { get; private set; } = AccessModelView.Unavailable;

    /// <summary><see langword="true"/> when a searchable identity directory is available to validate against.</summary>
    public bool DirectoryAvailable => Model.DirectoryAvailable;

    /// <summary>The operator-facing explanation of what a valid principal id is for this deployment.</summary>
    public string DirectoryExplanation => Model.DirectoryExplanation;

    /// <summary>The best-effort active authentication mode.</summary>
    public AccessAuthenticationMode AuthenticationMode => Model.AuthenticationMode;

    /// <summary>A human-readable label for the active authentication mode.</summary>
    public string AuthenticationModeLabel => DescribeAuthenticationMode(Model.AuthenticationMode);

    /// <summary>
    /// <see langword="true"/> when the access model was read successfully and the
    /// active authorizer does <b>not</b> enforce rules and membership on the data
    /// path (a flat / Basic authorizer), so the UI must not overstate enforcement.
    /// A failed / denied read never shows the notice - an unknown model must not be
    /// presented as an unenforced one.
    /// </summary>
    public bool ShowEnforcementNotice => Model.IsSuccess && !Model.RulesEnforced;

    /// <summary>Applies a freshly read access-model snapshot.</summary>
    /// <param name="model">The snapshot to apply. Must not be <see langword="null"/>.</param>
    public void Apply(AccessModelView model)
    {
        ArgumentNullException.ThrowIfNull(model);
        Model = model;
    }

    /// <summary>
    /// Validates a <b>new</b> principal <paramref name="principalId"/> before it is
    /// created. When a directory is available the id is resolved against it and the
    /// create is blocked unless it resolves to a real principal of the expected
    /// <paramref name="kind"/>; when no directory is available the create is allowed
    /// as an explicitly unvalidated free-text entry.
    /// </summary>
    /// <param name="principalId">The entered / chosen id. Must not be <see langword="null"/>.</param>
    /// <param name="kind">The kind of principal the form creates (user or group).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task<CreatePrincipalDecision> ValidateAsync(
        string principalId,
        DirectoryPrincipalKind kind,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(principalId);
        var id = principalId.Trim();
        if (id.Length == 0)
        {
            return CreatePrincipalDecision.Block("Enter a principal id.");
        }

        if (!DirectoryAvailable)
        {
            // No directory can be queried, so existence cannot be enforced: allow
            // the free-text id through as an explicitly unvalidated entry.
            return CreatePrincipalDecision.AllowUnvalidated();
        }

        var descriptor = await _membership
            .ResolveDirectoryPrincipalAsync(id, cancellationToken)
            .ConfigureAwait(false);
        if (descriptor is null)
        {
            return CreatePrincipalDecision.Block(NoSuchPrincipalReason);
        }

        if (descriptor.Kind != kind)
        {
            return CreatePrincipalDecision.Block(
                $"'{id}' is a {DescribePrincipalKind(descriptor.Kind)} in the directory, not a {DescribePrincipalKind(kind)}.");
        }

        return CreatePrincipalDecision.Allow();
    }

    /// <summary>Maps an <see cref="AccessAuthenticationMode"/> to an operator-facing label.</summary>
    /// <param name="mode">The authentication mode to describe.</param>
    public static string DescribeAuthenticationMode(AccessAuthenticationMode mode) => mode switch
    {
        AccessAuthenticationMode.Anonymous => "Anonymous",
        AccessAuthenticationMode.Claims => "Claims",
        AccessAuthenticationMode.Basic => "Basic",
        _ => "Unknown",
    };

    private static string DescribePrincipalKind(DirectoryPrincipalKind kind) =>
        kind == DirectoryPrincipalKind.Group ? "group" : "user";
}
