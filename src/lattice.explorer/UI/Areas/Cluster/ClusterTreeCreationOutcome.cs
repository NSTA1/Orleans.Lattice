using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster;

/// <summary>
/// What a create-tree call came to: the tree the cluster registered, or one
/// sentence saying why it registered none.
/// </summary>
/// <param name="Result">The cluster's answer when it registered a new tree; <see langword="null"/> when it did not.</param>
/// <param name="Refusal">Why no tree was created; <see langword="null"/> when one was.</param>
internal sealed record ClusterTreeCreationOutcome(TreeCreationResult? Result, string? Refusal)
{
    /// <summary>Whether the cluster registered a new tree.</summary>
    public bool IsCreated => Result is { Created: true };

    /// <summary>The outcome of a registered tree.</summary>
    /// <param name="result">The cluster's answer.</param>
    /// <returns>The outcome.</returns>
    public static ClusterTreeCreationOutcome Created(TreeCreationResult result) => new(result, null);

    /// <summary>The outcome of a refused create.</summary>
    /// <param name="refusal">Why no tree was created.</param>
    /// <returns>The outcome.</returns>
    public static ClusterTreeCreationOutcome Refused(string refusal) => new(null, refusal);
}
