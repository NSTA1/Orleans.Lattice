using System.Collections.Immutable;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;

/// <summary>
/// A tree's WAL partitions: the storage provider key backing each one, and
/// whether that key resolves on the silo that answered - in words and a state
/// pill, never by colour alone.
/// </summary>
public partial class ClusterWalPartitions
{
    private IReadOnlyList<TreeWalPartitionPlacement> _rows = [];

    /// <summary>The partitions, from a placement read or an audit.</summary>
    [Parameter, EditorRequired]
    public ImmutableArray<TreeWalPartitionPlacement> Partitions { get; set; }

    /// <inheritdoc />
    protected override void OnParametersSet() =>
        _rows = Partitions.IsDefault ? [] : Partitions;

    private static string KeyText(string? key) => string.IsNullOrEmpty(key) ? "default" : key;
}
