using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps;

/// <summary>
/// One manifest role compiled for one tenant install: the role's operations and its scopes resolved to
/// effective (tenant-composed) tree ids exactly as the role compiler resolves them. It answers whether a
/// caller holds the role through the shared access gate. It is the per-role unit of
/// <see cref="AppRoleGrantEvaluator"/>, shared by the app MCP tool surface and the app workspace so the two
/// gate on the same evaluation.
/// </summary>
/// <remarks>
/// <para>
/// <b>The rule.</b> A caller holds the role when, for at least one of its resolved scopes, the gate allows
/// <em>every</em> operation bit of the role on that scope. Each bit is presented as its own
/// <see cref="LatticeAccessRequest"/> over the scope's effective tree id: with the key for a key scope, and
/// with no key for a tree or prefix scope. Only an unfiltered allow holds the bit. A key-filtered allow
/// admits some keys rather than the whole scope, so it never holds the bit - on a prefix scope no less than
/// on a tree scope, because the gate exposes no whole-prefix query and testing the filter at the prefix
/// string would resolve the exact-key tier, letting a grant on the single key that spells the prefix carry
/// the entire prefix. A role with no operations or no scopes is never held.
/// </para>
/// </remarks>
internal sealed class AppRoleGate
{
    /// <summary>Initializes a new <see cref="AppRoleGate"/>.</summary>
    /// <param name="operations">The role's operations.</param>
    /// <param name="scopes">The role's scopes, resolved to effective tree ids.</param>
    /// <exception cref="ArgumentNullException"><paramref name="scopes"/> is null.</exception>
    public AppRoleGate(LatticeOperation operations, LatticeScope[] scopes)
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

            // A key-filtered allow admits some keys, not the scope. No scope kind
            // the role gate evaluates is attached to a key (a key scope passes its
            // key on the request itself), so a filter always means the caller holds
            // less than the role asks for. Testing the filter at the prefix string
            // would resolve the exact-key tier and let a grant on the single key
            // that spells the prefix carry the whole prefix (#3863).
            if (decision.KeyFilter is not null)
            {
                return false;
            }
        }

        return true;
    }
}
