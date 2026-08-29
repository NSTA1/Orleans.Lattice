using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Access.Views;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Access.Workspace;

/// <summary>
/// The Access plugin's view state and operations, lifted out of the panel's
/// code-behind so the surface can be split into small views by concern without
/// each one re-deriving the state the others depend on.
/// <para>
/// Everything the plugin does runs against its single controlled domain
/// contract (<see cref="IAccessDomain"/>) plus the keyed plugin access store;
/// it holds no connection, no channel, and no container (epic decision D3).
/// Every action is gated on the plugin's own advisory access decision, read
/// from the store under <see cref="AccessPluginKeys.PluginId"/> - rendering
/// disabled, not hidden, when denied - and folds a server denial into a clean
/// status banner rather than surfacing an unhandled error.
/// </para>
/// </summary>
/// <remarks>
/// Gating here is advisory: the server remains the sole enforcement point, so
/// every operation still handles a runtime denial.
/// </remarks>
public sealed partial class AccessWorkspace : IDisposable
{
    private readonly IAccessDomain _domain;
    private readonly IExplorerPluginAccessStore _store;

    /// <summary>
    /// Creates the workspace over the plugin's domain contract and the keyed
    /// access store its gate publishes into. Reads the current gate decision
    /// immediately, so a view rendered before the first probe completes is
    /// fail-closed rather than optimistic.
    /// </summary>
    /// <param name="domain">The plugin's controlled domain contract. Must not be <see langword="null"/>.</param>
    /// <param name="store">The keyed plugin access store. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">Either argument is <see langword="null"/>.</exception>
    public AccessWorkspace(IAccessDomain domain, IExplorerPluginAccessStore store)
    {
        ArgumentNullException.ThrowIfNull(domain);
        ArgumentNullException.ThrowIfNull(store);

        _domain = domain;
        _store = store;
        Labels = domain.CreateLabelResolver();
        AccessModel = new AccessCreateModel(domain.Membership);

        ReadAccess(out var allowed, out var authenticationRequired);
        Allowed = allowed;
        AuthenticationRequired = authenticationRequired;

        _store.Changed += OnAccessChanged;
    }

    /// <summary>Raised whenever the observable state changes, so the views re-render.</summary>
    public event Action? Changed;

    /// <summary>The plugin's controlled domain contract, handed to the views that need a picker.</summary>
    public IAccessDomain Domain => _domain;

    /// <summary>The panel-lifetime cache of directory display names for principal ids.</summary>
    public PrincipalLabelResolver Labels { get; }

    /// <summary>
    /// The create-form / access-state model: directory availability, the
    /// provider explanation, the auth-mode and enforcement banners, and the
    /// resolve-and-block decision for a new principal.
    /// </summary>
    public AccessCreateModel AccessModel { get; }

    /// <summary>Whether a request is in flight, so every action renders disabled.</summary>
    public bool Busy { get; private set; }

    /// <summary>Whether the plugin's own gate currently admits the caller.</summary>
    public bool Allowed { get; private set; }

    /// <summary>
    /// Whether the gate refused because the connection carries no accepted
    /// credential, rather than because an authenticated caller was denied. The
    /// panel prompts a sign-in for this state instead of greying out.
    /// </summary>
    public bool AuthenticationRequired { get; private set; }

    /// <summary>The last operation's outcome, rendered as a status banner.</summary>
    public AccessOperationResult? LastResult { get; private set; }

    /// <summary>The active internal sub-surface, one of <see cref="AccessSurfaces"/>'s ids.</summary>
    public string ActiveSurfaceId { get; private set; } = AccessSurfaces.Groups;

    /// <summary>
    /// Whether locally-defined group and member editing is meaningful for this
    /// cluster. False only when the access model was read successfully and
    /// reports token-only membership, in which case the editing surface is
    /// disabled but stays read-only viewable. The server remains the
    /// enforcement point.
    /// </summary>
    public bool MembershipEditable => AccessModel.MembershipEditingEnabled;

    /// <summary>
    /// Loads the surface: the access model, the tree catalog, and the active
    /// sub-surface's data. A no-op beyond the gate read when the gate denies.
    /// </summary>
    public async Task InitializeAsync()
    {
        if (!Allowed)
        {
            return;
        }

        await LoadAccessModelAsync();
        await LoadTreesAsync();
        await ReloadAsync();
    }

    /// <summary>
    /// Activates <paramref name="surfaceId"/>, closing any open create / edit
    /// form so the caller always returns to the list-first view, and loading the
    /// newly activated surface's data if it has not been loaded yet.
    /// </summary>
    /// <param name="surfaceId">The sub-surface id to activate.</param>
    public async Task SelectSurfaceAsync(string surfaceId)
    {
        if (string.Equals(ActiveSurfaceId, surfaceId, StringComparison.Ordinal))
        {
            return;
        }

        ActiveSurfaceId = surfaceId;
        LastResult = null;

        // Leaving a surface closes any open create/edit form so the user always
        // returns to the list-first view with an explicit call to action.
        GroupFormOpen = false;
        RuleFormOpen = false;

        // Load the newly activated surface's data if it has not been loaded yet,
        // so the list (and, for the tree-scoped surfaces, the subject drop-down)
        // is populated without requiring a manual Refresh.
        await LoadForSurfaceAsync(force: false);
        RaiseChanged();
    }

    /// <summary>Re-reads the access model and reloads the active sub-surface.</summary>
    public async Task ReloadAsync()
    {
        LastResult = null;
        await LoadAccessModelAsync();
        await LoadForSurfaceAsync(force: true);
        RaiseChanged();
    }

    /// <inheritdoc />
    public void Dispose() => _store.Changed -= OnAccessChanged;

    /// <summary>
    /// Reads the cluster's best-effort access model, so the create forms know
    /// whether to fail closed against a directory, what a valid id is for this
    /// deployment, and whether the active authorizer actually enforces the
    /// recorded rules and membership. A denial or transport failure folds into
    /// the safe unavailable snapshot rather than throwing.
    /// </summary>
    private async Task LoadAccessModelAsync()
    {
        if (!Allowed)
        {
            return;
        }

        AccessModel.Apply(await _domain.Membership.GetAccessModelAsync());
    }

    /// <summary>
    /// Loads the data the active sub-surface needs. The Policies and Explain
    /// surfaces also load the groups so the shared subject drop-down is
    /// populated. When <paramref name="force"/> is false the membership lists
    /// are only loaded if still empty, so switching surfaces does not clobber
    /// data already in view.
    /// </summary>
    private async Task LoadForSurfaceAsync(bool force)
    {
        switch (ActiveSurfaceId)
        {
            case AccessSurfaces.Groups:
                if (force || Groups.Count == 0)
                {
                    await LoadGroupsAsync(reset: true);
                }

                break;
            case AccessSurfaces.Policies:
                if (force || Rules.Count == 0)
                {
                    await LoadRulesAsync(reset: true);
                }

                await LoadSubjectListsAsync(force);
                break;
            case AccessSurfaces.Explain:
                await LoadSubjectListsAsync(force);
                break;
            default:
                break;
        }
    }

    /// <summary>
    /// Loads the groups that back the shared subject drop-down on the Policies
    /// and Explain surfaces. Uses the guarded loaders, so it is safe to call
    /// when not already busy.
    /// </summary>
    private async Task LoadSubjectListsAsync(bool force)
    {
        if (force || Groups.Count == 0)
        {
            await LoadGroupsAsync(reset: true);
        }
    }

    private void ReadAccess(out bool allowed, out bool authenticationRequired)
    {
        var access = _store.Get(AccessPluginKeys.PluginId);
        allowed = access.IsAllowed;
        authenticationRequired = access.State == ExplorerPluginAccessState.AuthenticationRequired;
    }

    private void OnAccessChanged(ExplorerPluginAccessChange change)
    {
        // Only this plugin's own plugin-level decision gates the panel; a sibling
        // plugin's probe completing, or this plugin's scoped sub-capability
        // changing, must not re-render it.
        if (change.Key.Scope is not null
            || !string.Equals(change.Key.PluginId, AccessPluginKeys.PluginId, StringComparison.Ordinal))
        {
            return;
        }

        ReadAccess(out var allowed, out var authenticationRequired);
        if (allowed == Allowed && authenticationRequired == AuthenticationRequired)
        {
            return;
        }

        Allowed = allowed;
        AuthenticationRequired = authenticationRequired;
        _ = OnGateChangedAsync();
    }

    private async Task OnGateChangedAsync()
    {
        // When the gate freshly opens (for example after the connection reaches
        // the cluster or an admin signs in) populate the tree list so the
        // tree-scoped surfaces are usable without a manual refresh, and read the
        // access model so the create forms and enforcement banner are accurate.
        if (Allowed && Trees.Count == 0)
        {
            await LoadAccessModelAsync();
            await LoadTreesAsync();
        }

        RaiseChanged();
    }

    private void RaiseChanged() => Changed?.Invoke();

    private static AccessOperationResult ToResult(AccessOperationStatus status, string message) => status switch
    {
        AccessOperationStatus.Denied => AccessOperationResult.Denied(message),
        _ => AccessOperationResult.Failure(message),
    };

    /// <summary>The status-banner modifier class for an operation outcome.</summary>
    /// <param name="status">The operation status to classify.</param>
    public static string ResultClass(AccessOperationStatus status) => status switch
    {
        AccessOperationStatus.Succeeded => "is-success",
        AccessOperationStatus.Denied => "is-denied",
        _ => "is-failed",
    };

    // Warms the label cache for the subject id of every ranked rule about to be
    // rendered, so each subject cell upgrades from its raw id to a friendly name.
    // Bounded by the loaded rule page and only run on data load, never per render.
    private async Task ResolveRuleSubjectsAsync(IReadOnlyList<RankedRule> ranked)
    {
        if (ranked.Count == 0)
        {
            return;
        }

        var ids = new List<string>(ranked.Count);
        foreach (var rule in ranked)
        {
            ids.Add(rule.Rule.Subject.Id);
        }

        await Labels.ResolveManyAsync(ids);
    }

    private static LatticeScope BuildScope(LatticeScopeKind kind, string treeId, string keyOrPrefix) => kind switch
    {
        LatticeScopeKind.Key => LatticeScope.Key(treeId, keyOrPrefix.Trim()),
        LatticeScopeKind.Prefix => LatticeScope.Prefix(treeId, keyOrPrefix.Trim()),
        _ => LatticeScope.Tree(treeId),
    };

    private static LatticeOperation CombineOperations(IEnumerable<LatticeOperation> flags)
    {
        var combined = LatticeOperation.None;
        foreach (var flag in flags)
        {
            combined |= flag;
        }

        return combined;
    }
}
