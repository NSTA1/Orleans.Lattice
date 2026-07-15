using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Schema;

/// <summary>
/// The Schema area's policy operations for a single governed tree: reading the
/// current enforcement policy, replacing it, and clearing it. Every member folds a
/// server denial or a transport failure into a non-success envelope rather than
/// throwing, so the panel degrades cleanly and always has a message to show.
/// </summary>
public interface ISchemaPolicyService
{
    /// <summary>Reads the enforcement policy for <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<SchemaReadView<LatticeSchemaPolicy>> GetPolicyAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>Sets or replaces the enforcement policy for <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="policy">The policy to apply. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<SchemaOperationResult> SetPolicyAsync(string treeId, LatticeSchemaPolicy policy, CancellationToken cancellationToken = default);

    /// <summary>Clears the enforcement policy for <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<SchemaOperationResult> ClearPolicyAsync(string treeId, CancellationToken cancellationToken = default);
}
