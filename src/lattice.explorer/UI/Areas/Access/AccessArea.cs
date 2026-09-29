using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.UI.Areas.Access;

/// <summary>
/// The Access area: rules, groups, and the explanation of an access decision,
/// bound to <see cref="ILatticeAuthAdmin"/>. It is cluster-wide (the policy store
/// is not per tenant), so its addresses are never tenant-rooted.
/// </summary>
/// <remarks>
/// <para>
/// Visibility comes from a fail-closed probe: the smallest page of the group
/// catalogue, which the facade admits only for an administrator. It succeeds, and
/// the area is visible. It is refused while the Explorer is not signed in, and the
/// area stays in the directory as unavailable with a sign-in sentence, so an
/// anonymous circuit reads as "sign in", never as an empty cluster. Any other
/// outcome - a signed-in identity without the grant, a cluster that does not serve
/// the facade, no connection - hides the area.
/// </para>
/// <para>
/// The verdict is memoised per circuit for the identity it was reached under, so
/// the directory's per-navigation ask is free until the identity changes.
/// </para>
/// </remarks>
internal sealed class AccessArea : IExplorerArea
{
    /// <summary>The reason shown while the Explorer is not signed in.</summary>
    public const string SignInReason = "Sign in to administer access on this cluster.";

    private static readonly AuthPageRequest ProbePage = new() { PageSize = 1 };

    private readonly ILatticeAuthAdmin? _admin;
    private readonly IExplorerAuthSession? _session;
    private readonly AccessCatalog? _catalog;
    private (bool Authenticated, string? User, AreaAvailability Availability)? _verdict;

    /// <summary>Creates the area over the circuit's services, any of which may be absent.</summary>
    /// <param name="services">The circuit's services.</param>
    public AccessArea(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _admin = services.GetService<ILatticeAuthAdmin>();
        _session = services.GetService<IExplorerAuthSession>();
        _catalog = _admin is null ? null : services.GetService<AccessCatalog>() ?? new AccessCatalog(_admin);
        Completions = _catalog is null ? null : new AccessCompletionSource(_catalog);
        Commands =
        [
            new ExplorerCommand(ExplainCommandId, "Explain access...")
            {
                Target = AccessRoutes.Explain,
                Detail = "Why a subject is allowed or denied an operation, and its effective permissions",
            },
            new ExplorerCommand(CreateRuleCommandId, "Create an access rule")
            {
                Target = AccessRoutes.Rules.WithQuery(AccessRoutes.NewQuery, "true"),
            },
            new ExplorerCommand(CreateGroupCommandId, "Create a group")
            {
                Target = AccessRoutes.Groups.WithQuery(AccessRoutes.NewQuery, "true"),
            },
        ];
    }

    /// <summary>The id of the "Explain access..." command.</summary>
    public const string ExplainCommandId = "access.explain";

    /// <summary>The id of the "Create an access rule" command.</summary>
    public const string CreateRuleCommandId = "access.create-rule";

    /// <summary>The id of the "Create a group" command.</summary>
    public const string CreateGroupCommandId = "access.create-group";

    /// <inheritdoc />
    public string Key => AccessRoutes.AreaKey;

    /// <inheritdoc />
    public string DisplayName => "Access";

    /// <inheritdoc />
    public int DirectoryOrder => 30;

    /// <inheritdoc />
    public bool IsTenantScoped => false;

    /// <inheritdoc />
    public IAddressCompletionSource? Completions { get; }

    /// <inheritdoc />
    public IReadOnlyList<ExplorerCommand> Commands { get; }

    /// <inheritdoc />
    public async ValueTask<AreaAvailability> GetAvailabilityAsync(CancellationToken cancellationToken)
    {
        if (_admin is null)
        {
            return AreaAvailability.Hidden;
        }

        var authenticated = _session?.IsAuthenticated == true;
        var user = _session?.Username;
        if (_verdict is { } memo && memo.Authenticated == authenticated && string.Equals(memo.User, user, StringComparison.Ordinal))
        {
            return memo.Availability;
        }

        var availability = await ProbeAsync(authenticated, cancellationToken).ConfigureAwait(true);
        _verdict = (authenticated, user, availability);
        return availability;
    }

    /// <inheritdoc />
    public async ValueTask<string?> GetHomeStatusAsync(CancellationToken cancellationToken)
    {
        if (_catalog is null)
        {
            return null;
        }

        var model = await _catalog.GetAccessModelAsync(cancellationToken).ConfigureAwait(true);
        return model is null ? null
            : model.RulesEnforced ? "Rules are enforced."
            : "Rules are recorded but not enforced.";
    }

    private async Task<AreaAvailability> ProbeAsync(bool authenticated, CancellationToken cancellationToken)
    {
        try
        {
            var page = await _admin!.ListGroupsAsync(ProbePage, cancellationToken).ConfigureAwait(true);

            // An absent answer proved nothing, so it admits nothing.
            return page is null ? AreaAvailability.Hidden : AreaAvailability.Visible;
        }
        catch (LatticeAuthorizationDeniedException)
        {
            return authenticated ? AreaAvailability.Hidden : AreaAvailability.Unavailable(SignInReason);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return AreaAvailability.Hidden;
        }
    }
}
