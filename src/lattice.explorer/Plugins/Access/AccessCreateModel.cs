using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
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

    /// <summary>
    /// The banner shown when the cluster resolves group membership solely from the
    /// identity-provider token, so locally-defined groups and members have no effect
    /// on access. Group and member editing is disabled but stays read-only viewable.
    /// </summary>
    public const string MembershipInertNoticeText =
        "This cluster resolves group membership from the identity-provider token; " +
        "locally-defined membership has no effect on access. Manage groups in your identity provider.";

    private readonly IMembershipAdminService _membership;

    /// <summary>
    /// The reserved policy tree an access-administration delegation rule targets
    /// (<c>"sys-auth-policy"</c>, from <see cref="LatticeAuthReservedTrees.PolicyTreeId"/>).
    /// The delegation affordance supplies this automatically, so the operator never
    /// has to pick the reserved tree from the (hidden) tree catalog.
    /// </summary>
    public static string AccessAdministrationTreeId => LatticeAuthReservedTrees.PolicyTreeId;

    /// <summary>
    /// The operator-facing helper text shown beside the access-administration
    /// delegation affordance. States that it grants full access-administration
    /// authority and that the cluster must have the delegation option enabled for
    /// the server to accept the rule.
    /// </summary>
    public const string AccessAdministrationHelpText =
        "Grants full access administration on this cluster: the chosen subject may manage groups, " +
        "membership, and policy rules. Requires the cluster's access-administration delegation option " +
        "to be enabled; if it is off, the server rejects this rule.";

    /// <summary>
    /// Builds the single access-administration delegation rule the affordance
    /// authors: a <b>whole-tree</b> <see cref="LatticeOperation.Admin"/>
    /// <see cref="LatticeEffect.Allow"/> rule on the reserved policy tree
    /// (<see cref="AccessAdministrationTreeId"/>) for <paramref name="subject"/>.
    /// This is exactly the shape the policy store permits when
    /// access-administration delegation is enabled, so authoring it delegates
    /// access administration to the chosen user or group.
    /// </summary>
    /// <param name="ruleId">A stable id for the rule. Must not be <see langword="null"/> or empty.</param>
    /// <param name="subject">The user or group to delegate to. Must not be <see langword="null"/>.</param>
    /// <returns>The whole-tree Admin Allow rule on the reserved policy tree.</returns>
    /// <exception cref="ArgumentException"><paramref name="ruleId"/> is <see langword="null"/> or empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="subject"/> is <see langword="null"/>.</exception>
    public static LatticeAuthorizationRule BuildAccessAdministrationRule(string ruleId, LatticeSubjectSelector subject)
    {
        ArgumentException.ThrowIfNullOrEmpty(ruleId);
        ArgumentNullException.ThrowIfNull(subject);
        return new LatticeAuthorizationRule(
            ruleId,
            subject,
            LatticeScope.Tree(AccessAdministrationTreeId),
            LatticeOperation.Admin,
            LatticeEffect.Allow);
    }

    /// <summary>
    /// The operator-facing helper text shown beside the all-trees (cluster-wide)
    /// grant affordance. States that the grant governs every application tree and
    /// that the cluster must have all-trees grants enabled for the server to
    /// enforce the data-plane operations, since the UI cannot read the server-side
    /// flag, and that Telemetry is honoured regardless because it is a scopeless
    /// cluster-wide capability rather than a data-plane grant.
    /// </summary>
    public const string AllTreesHelpText =
        "Grants the chosen operations on every application tree in the cluster (the reserved " +
        "authorization and system trees are always excluded). The data-plane operations take " +
        "effect only when the cluster's all-trees grants option is enabled; if it is off, they " +
        "are recorded but stay inert. Telemetry is the exception: it is a cluster-wide capability " +
        "attached to no tree, so a Telemetry grant here is honoured whether or not that option is on.";

    /// <summary>
    /// Builds an all-trees (cluster-wide) grant: a <b>whole-tree</b> rule over the
    /// all-trees sentinel (<see cref="LatticeScope.ClusterWide()"/>,
    /// <see cref="LatticeScope.ClusterWideTreeId"/> <c>"*"</c>) for
    /// <paramref name="subject"/>, carrying <paramref name="operations"/> with
    /// <paramref name="effect"/>. When the cluster has all-trees grants enabled the
    /// decision engine consults this rule for every non-system tree, per the
    /// four-tier precedence; when it is off the rule is inert.
    /// </summary>
    /// <param name="ruleId">A stable id for the rule. Must not be <see langword="null"/> or empty.</param>
    /// <param name="subject">The user or group to grant to. Must not be <see langword="null"/>.</param>
    /// <param name="operations">The operations the grant covers.</param>
    /// <param name="effect">Whether the rule allows or denies.</param>
    /// <returns>The whole-tree rule over the all-trees sentinel.</returns>
    /// <exception cref="ArgumentException"><paramref name="ruleId"/> is <see langword="null"/> or empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="subject"/> is <see langword="null"/>.</exception>
    public static LatticeAuthorizationRule BuildAllTreesRule(
        string ruleId,
        LatticeSubjectSelector subject,
        LatticeOperation operations,
        LatticeEffect effect)
    {
        ArgumentException.ThrowIfNullOrEmpty(ruleId);
        ArgumentNullException.ThrowIfNull(subject);
        return new LatticeAuthorizationRule(
            ruleId,
            subject,
            LatticeScope.ClusterWide(),
            operations,
            effect);
    }

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

    /// <summary>
    /// <see langword="true"/> when the access model was read successfully and the
    /// cluster resolves group membership solely from the identity-provider token, so
    /// locally-defined groups and members are inert for authorization. A failed /
    /// denied read never reports inert membership - an unknown model must not gate the
    /// editing surface.
    /// </summary>
    public bool ShowMembershipInertNotice => Model.IsSuccess && !Model.LocalMembershipEffective;

    /// <summary>
    /// <see langword="true"/> when locally-defined group and member editing is
    /// meaningful and should stay enabled; <see langword="false"/> only when the
    /// access model was read successfully and reports token-only membership, in which
    /// case editing is disabled (but groups and members remain read-only viewable).
    /// An unread / failed model leaves editing enabled, matching the capability-aware
    /// gate the view already applies.
    /// </summary>
    public bool MembershipEditingEnabled => !ShowMembershipInertNotice;

    /// <summary>
    /// <see langword="true"/> when the access model was read successfully and the
    /// cluster-wide all-trees grant tier is enabled, so a <c>Tree:*</c> data-plane
    /// rule is enforced. When the read failed the state is unknown and this is
    /// <see langword="false"/>. Drives the live posture badge.
    /// </summary>
    public bool AllTreesGrantsEnabled => Model.IsSuccess && Model.AllTreesGrantsEnabled;

    /// <summary>
    /// <see langword="true"/> when the access model was read successfully and
    /// access-administration delegation is enabled, so a whole-tree <c>Admin</c>
    /// rule on the policy tree may be authored. When the read failed the state is
    /// unknown and this is <see langword="false"/>. Drives the live posture badge.
    /// </summary>
    public bool AccessAdministrationDelegationEnabled =>
        Model.IsSuccess && Model.AccessAdministrationDelegationEnabled;

    /// <summary>
    /// <see langword="true"/> when the posture badges should render: the access
    /// model was read successfully, so the two tier flags reflect the live
    /// server-side state rather than an unknown default.
    /// </summary>
    public bool ShowPosture => Model.IsSuccess;

    /// <summary>The posture badge label for the all-trees grant tier.</summary>
    public string AllTreesGrantsLabel =>
        AllTreesGrantsEnabled ? "All-trees grants: on" : "All-trees grants: off";

    /// <summary>
    /// What the all-trees posture badge means, rendered as a help disclosure
    /// beside it rather than as a title attribute a keyboard or touch caller can
    /// never reach.
    /// </summary>
    public const string AllTreesGrantsExplanation =
        "When off, an all-trees ('*') data grant is recorded but inert - the decision engine never consults "
        + "it for an ordinary tree. Enable LatticeAuthOptions.AllTreesGrantsEnabled on the silo.";

    /// <summary>
    /// What the delegation posture badge means, rendered as a help disclosure
    /// beside it rather than as a title attribute.
    /// </summary>
    public const string AccessAdministrationDelegationExplanation =
        "When off, a whole-tree Admin delegation rule on the policy tree is unauthorable - the server "
        + "rejects it. Enable LatticeAuthOptions.AccessAdministrationDelegationEnabled on the silo.";

    /// <summary>The posture badge label for the access-administration delegation tier.</summary>
    public string AccessAdministrationDelegationLabel =>
        AccessAdministrationDelegationEnabled
            ? "Access-admin delegation: on"
            : "Access-admin delegation: off";

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
