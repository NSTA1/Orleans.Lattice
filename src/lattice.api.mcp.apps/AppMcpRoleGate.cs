using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Mcp.Apps;

/// <summary>
/// One manifest role compiled for one tenant install: the role's operations and its
/// scopes resolved to effective (tenant-composed) tree ids exactly as the role
/// compiler resolves them. It answers whether a caller holds the role, through the
/// shared access gate, which is the per-tool gating rule of the app tool surface.
/// </summary>
/// <remarks>
/// <para>
/// <b>The rule.</b> A caller holds the role when, for at least one of its resolved
/// scopes, the gate allows <em>every</em> operation bit of the role on that scope. Each
/// bit is presented as its own <see cref="LatticeAccessRequest"/> over the scope's
/// effective tree id: with the key for a key scope, and with no key for a tree or
/// prefix scope. An unfiltered allow holds the bit. A key-filtered allow holds the bit
/// only on a prefix scope and only when the filter keeps the prefix itself; on a tree
/// scope a filtered allow means the caller holds only part of the tree, so it does not.
/// A role with no operations or no scopes is never held.
/// </para>
/// <para>
/// The same evaluation runs when the tool is advertised and again when it is invoked,
/// so the two decisions cannot drift apart.
/// </para>
/// </remarks>
internal sealed class AppMcpRoleGate
{
    /// <summary>Initializes a new <see cref="AppMcpRoleGate"/>.</summary>
    /// <param name="operations">The role's operations.</param>
    /// <param name="scopes">The role's scopes, resolved to effective tree ids.</param>
    public AppMcpRoleGate(LatticeOperation operations, LatticeScope[] scopes)
    {
        ArgumentNullException.ThrowIfNull(scopes);
        Operations = operations;
        Scopes = scopes;
    }

    /// <summary>The role's operations.</summary>
    public LatticeOperation Operations { get; }

    /// <summary>The role's scopes, resolved to effective (tenant-composed) tree ids.</summary>
    public LatticeScope[] Scopes { get; }

    /// <summary>Evaluates whether <paramref name="subject"/> holds the role.</summary>
    /// <param name="gate">The shared access gate.</param>
    /// <param name="subject">The resolved caller.</param>
    /// <param name="cancellationToken">Cancels the evaluation.</param>
    /// <returns><c>true</c> when the caller holds the role on at least one scope.</returns>
    public async ValueTask<bool> IsHeldAsync(
        ILatticeAccessGate gate,
        LatticeSubject subject,
        CancellationToken cancellationToken)
    {
        if (Operations == LatticeOperation.None)
            return false;

        foreach (var scope in Scopes)
        {
            if (await HoldsAllAsync(gate, subject, scope, cancellationToken).ConfigureAwait(false))
                return true;
        }

        return false;
    }

    private async ValueTask<bool> HoldsAllAsync(
        ILatticeAccessGate gate,
        LatticeSubject subject,
        LatticeScope scope,
        CancellationToken cancellationToken)
    {
        var key = scope.Kind == LatticeScopeKind.Key ? scope.KeyOrPrefix : null;
        var remaining = (int)Operations;
        while (remaining != 0)
        {
            var bit = remaining & -remaining;
            remaining &= remaining - 1;

            var request = new LatticeAccessRequest(scope.TreeId, (LatticeOperation)bit, subject, key);
            var decision = await gate.AuthorizeAsync(in request, cancellationToken).ConfigureAwait(false);
            if (!decision.Allowed)
                return false;

            if (decision.KeyFilter is { } filter
                && (scope.Kind != LatticeScopeKind.Prefix || scope.KeyOrPrefix is null || !filter(scope.KeyOrPrefix)))
            {
                return false;
            }
        }

        return true;
    }
}
