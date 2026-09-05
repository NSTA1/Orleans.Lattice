using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Default <see cref="ILatticeApiMcpPermissionResolver"/> that reuses the
/// <c>Api.Auth</c> effective-permissions surface
/// (<see cref="ILatticeAuthAdmin.EffectivePermissionsAsync"/>) to decide which
/// facade groups a caller may use. It does <b>not</b> re-implement any
/// authorization logic: it reads the authored rules in effect for the caller's
/// subject and maps their granted operations onto the four facade groups through
/// <see cref="LatticeApiMcpGroupCapabilityMap"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Trusted in-silo introspection.</b> The effective-permissions surface is
/// administrator-gated: it authorizes the <em>ambient</em> caller as an
/// administrator before reporting a subject's rules. The MCP server is trusted
/// infrastructure co-hosted on the silo resolving policy on the caller's behalf,
/// so the read runs inside a system-origin scope - the same primitive the auth
/// facade uses internally for its own directory reads - which lets the server
/// introspect any authenticated caller's permissions without itself holding an
/// administrator grant. The credential is never elevated for the caller's own
/// tool calls; only this read-only introspection is trusted.
/// </para>
/// <para>
/// <b>Fail-closed.</b> When the auth facade is not registered, or the
/// introspection returns an authoritative "no grants" answer, the resolver
/// returns <see cref="LatticeApiMcpAccessSet.None"/> so the caller is offered no
/// tools rather than an unscoped set.
/// </para>
/// <para>
/// <b>Transient faults are not denials.</b> When the introspection call itself
/// never lands - a cancelled, deadline-exceeded, unavailable, or internal
/// transport fault, an Orleans response timeout, or silo churn - the resolver
/// raises <see cref="LatticeApiMcpDiscoveryUnavailableException"/> instead of
/// reporting an empty grant set. Reporting one would answer <c>tools/list</c>
/// <em>successfully</em> with a single meta-tool, which a client cannot
/// distinguish from having lost its permissions. Nothing extra is advertised on
/// either branch, so this is no wider than failing closed.
/// </para>
/// </remarks>
internal sealed class AuthAdminMcpPermissionResolver : ILatticeApiMcpPermissionResolver
{
    private readonly IServiceProvider _services;
    private readonly ILogger<AuthAdminMcpPermissionResolver> _logger;

    /// <summary>
    /// Initialises the resolver. The auth facade is resolved lazily and
    /// optionally so the MCP server can be registered in a host that has not
    /// wired the auth control plane (in which case no group is granted).
    /// </summary>
    public AuthAdminMcpPermissionResolver(
        IServiceProvider services,
        ILogger<AuthAdminMcpPermissionResolver> logger)
    {
        _services = services ?? throw new ArgumentNullException(nameof(services));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async ValueTask<LatticeApiMcpAccessSet> ResolveAsync(
        LatticeCredential credential,
        CancellationToken cancellationToken)
    {
        var admin = _services.GetService<ILatticeAuthAdmin>();
        if (admin is null)
        {
            _logger.LogDebug(
                "No ILatticeAuthAdmin registered; MCP discovery fails closed with no facade groups granted.");
            return LatticeApiMcpAccessSet.None;
        }

        var subjectId = ResolveSubjectId(credential);
        if (string.IsNullOrEmpty(subjectId))
        {
            return LatticeApiMcpAccessSet.None;
        }

        AuthEffectivePermissions permissions;
        try
        {
            // Trusted, read-only introspection on the caller's behalf. The
            // system-origin scope makes the facade's administrator gate
            // short-circuit so the co-hosted server can resolve any caller's
            // effective rules without itself being an administrator.
            using (LatticeSystemOrigin.Enter())
            {
                permissions = await admin.EffectivePermissionsAsync(subjectId, cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException
            && LatticeApiMcpDiscoveryFaultClassifier.IsTransientBackendFault(ex))
        {
            // The backend never answered, so there is no permission set to report.
            // Failing closed here would advertise an empty - but SUCCESSFUL - tool
            // list, which a client cannot tell apart from a genuine revocation.
            // Surface a retryable fault instead; nothing extra is advertised either
            // way, so the fail-closed guarantee is preserved.
            _logger.LogWarning(
                ex,
                "Resolving MCP facade-group access for subject '{SubjectId}' hit a transient backend fault; "
                + "surfacing a retryable discovery error rather than a falsely narrow tool set.",
                subjectId);
            throw new LatticeApiMcpDiscoveryUnavailableException(
                "MCP tool discovery could not resolve the caller's effective permissions because the "
                + "authorization backend was transiently unavailable. Retry the session.",
                ex);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(
                ex,
                "Resolving MCP facade-group access for subject '{SubjectId}' failed; failing closed.",
                subjectId);
            return LatticeApiMcpAccessSet.None;
        }

        return MapGroups(permissions);
    }

    private static string? ResolveSubjectId(LatticeCredential credential)
        => !string.IsNullOrEmpty(credential.PrincipalId) ? credential.PrincipalId
            : !string.IsNullOrEmpty(credential.Token) ? credential.Token
            : null;

    private static LatticeApiMcpAccessSet MapGroups(AuthEffectivePermissions permissions)
    {
        var access = LatticeApiMcpAccessSet.None;
        var rules = permissions.Rules;
        if (rules.Count == 0)
        {
            return access;
        }

        foreach (var group in LatticeApiMcpGroupCapabilityMap.AllGroups)
        {
            if (GroupIsGranted(rules, LatticeApiMcpGroupCapabilityMap.RequiredOperations(group)))
            {
                access = access.With(group);
            }
        }

        // Carry the caller's own Allow-granted operations so the discovery core can
        // apply a per-tool minimum inside a group it already admitted. Denies stay
        // the call-time gate's job, exactly as GroupIsGranted documents.
        for (var i = 0; i < rules.Count; i++)
        {
            if (rules[i].Effect == LatticeEffect.Allow)
            {
                access = access.WithOperations(rules[i].Operations);
            }
        }

        return access;
    }

    private static bool GroupIsGranted(
        IReadOnlyList<LatticeAuthorizationRule> rules,
        LatticeOperation mask)
    {
        // A group is discoverable when the caller holds at least one Allow grant
        // covering any operation the group exercises. Denies are honoured at
        // call time by the access gate; discovery advertises on grant presence.
        for (var i = 0; i < rules.Count; i++)
        {
            var rule = rules[i];
            if (rule.Effect == LatticeEffect.Allow && (rule.Operations & mask) != LatticeOperation.None)
            {
                return true;
            }
        }

        return false;
    }
}
