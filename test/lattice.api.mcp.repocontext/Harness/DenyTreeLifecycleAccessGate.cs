using System.Collections.Concurrent;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// A real, deny-by-default <see cref="ILatticeAccessGate"/> that reproduces the
/// production authorization posture issue #2737 was reported from: a resolved
/// subject that holds ordinary data-plane grants but has <b>no</b> matching rule
/// for <see cref="LatticeOperation.TreeLifecycle"/>, so a tree delete or purge is
/// refused with <see cref="LatticeAuthorizationDeniedException"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>This deliberately is not <see cref="NullLatticeAccessGate"/>.</b> The core
/// enforcement seam short-circuits entirely when the registered gate is the null
/// one, so a fixture that leaves the default in place exercises a code path that
/// cannot fail and proves nothing about whether a privileged maintenance
/// operation is authorized. Every assertion about the re-deriver's authorization
/// behaviour has to run against a gate that genuinely refuses, which is what this
/// type is for.
/// </para>
/// <para>
/// The evaluator is deny-by-default: an operation is allowed only when it is in
/// <see cref="GrantedOperations"/>, which deliberately omits
/// <see cref="LatticeOperation.TreeLifecycle"/>. Every refusal is recorded in
/// <see cref="DeniedOperations"/> so a negative arm cannot pass vacuously - a
/// test can assert the gate was actually consulted and actually refused, rather
/// than inferring it from an exception that some other seam might have thrown.
/// </para>
/// </remarks>
public sealed class DenyTreeLifecycleAccessGate : ILatticeAccessGate
{
    /// <summary>
    /// The operations this gate grants. Mirrors a subject with ordinary
    /// data-plane authority and no tree-lifecycle rule:
    /// <see cref="LatticeOperation.TreeLifecycle"/> is absent, so it falls to the
    /// deny default.
    /// </summary>
    public const LatticeOperation GrantedOperations =
        LatticeOperation.Read
        | LatticeOperation.Write
        | LatticeOperation.Delete
        | LatticeOperation.RangeRead
        | LatticeOperation.RangeDelete
        | LatticeOperation.CrdtApply
        | LatticeOperation.AtomicWrite
        | LatticeOperation.BulkLoad
        | LatticeOperation.Admin;

    private readonly ConcurrentQueue<(string TreeId, LatticeOperation Operation)> _denied = new();
    private readonly ConcurrentQueue<(string TreeId, LatticeOperation Operation)> _allowed = new();

    /// <summary>Every request this gate refused, in observation order.</summary>
    public IReadOnlyCollection<(string TreeId, LatticeOperation Operation)> DeniedOperations => _denied;

    /// <summary>Every request this gate allowed, in observation order.</summary>
    public IReadOnlyCollection<(string TreeId, LatticeOperation Operation)> AllowedOperations => _allowed;

    /// <summary>
    /// How many times the gate was asked to authorize
    /// <see cref="LatticeOperation.TreeLifecycle"/> against the supplied tree.
    /// Zero after a re-derivation proves the operation never reached the gate -
    /// which is the observable signature of a system-origin bypass, as distinct
    /// from the gate having been consulted and having allowed it.
    /// </summary>
    /// <param name="treeId">The tree to count lifecycle evaluations for.</param>
    /// <returns>The number of tree-lifecycle authorization requests seen for the tree.</returns>
    public int TreeLifecycleEvaluations(string treeId)
        => _denied.Concat(_allowed)
            .Count(r => r.Operation == LatticeOperation.TreeLifecycle
                && string.Equals(r.TreeId, treeId, StringComparison.Ordinal));

    /// <inheritdoc />
    public ValueTask<LatticeAccessDecision> AuthorizeAsync(
        in LatticeAccessRequest request,
        CancellationToken cancellationToken = default)
    {
        var treeId = request.TreeId;
        var operation = request.Operation;

        // Deny by default: only an operation explicitly present in the grant set
        // is allowed. TreeLifecycle is not, so a tree delete or purge is refused.
        if ((GrantedOperations & operation) == operation && operation != LatticeOperation.None)
        {
            _allowed.Enqueue((treeId, operation));
            return new ValueTask<LatticeAccessDecision>(LatticeAccessDecision.Allow());
        }

        _denied.Enqueue((treeId, operation));
        return new ValueTask<LatticeAccessDecision>(LatticeAccessDecision.Deny(
            $"no rule grants operation '{operation}' on tree '{treeId}' to this subject"));
    }
}
