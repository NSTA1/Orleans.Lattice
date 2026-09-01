namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// Thrown at silo start when a grain index's backing tree is configured to
/// replicate across clusters while the index's declaration has not opted in
/// with <see cref="GrainIndexOptions.AllowReplication"/>.
/// <para>
/// A grain index points at grain activations, and an activation is local to one
/// cluster: replicating the index tree ships entries naming grains the receiving
/// cluster does not host, so queries there resolve references that cannot be
/// addressed. The startup guard therefore audits the resolved merge mode and
/// fails the silo rather than letting the mismatch surface as a wrong answer.
/// </para>
/// </summary>
/// <remarks>
/// <para>
/// The guard <b>audits only</b>: it never rewrites the tree's merge mode, so
/// replication stays a deliberate and reversible operator choice. Setting
/// <see cref="GrainIndexOptions.AllowReplication"/> to <c>true</c> keeps full
/// replication working - the guard then merely records the opt-in - which
/// discourages the cluster-local footgun without disabling the capability.
/// </para>
/// <para>
/// The type derives directly from <see cref="Exception"/> so Orleans can
/// deep-copy it across a co-located grain-call boundary without a hand-written
/// copier, and is Orleans-serializable so the failure propagates intact across a
/// silo boundary.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.GrainIndexReplicationNotAllowedException)]
public sealed class GrainIndexReplicationNotAllowedException : Exception
{
    /// <summary>
    /// The logical name of the index whose tree is replicated. Empty on the
    /// message-only constructors.
    /// </summary>
    [Id(0)]
    public string IndexName { get; }

    /// <summary>
    /// The backing tree that resolved to a replicated merge mode. Empty on the
    /// message-only constructors.
    /// </summary>
    [Id(1)]
    public string TreeName { get; }

    /// <summary>
    /// The merge mode the tree resolved to. <see cref="LatticeMergeMode.LwwRegister"/>
    /// on the message-only constructors, which carry no context.
    /// </summary>
    [Id(2)]
    public LatticeMergeMode MergeMode { get; }

    /// <summary>
    /// Initialises a new instance with no diagnostic message and empty context.
    /// Provided to satisfy the framework's exception-construction contract;
    /// production throw sites use the context-carrying overload.
    /// </summary>
    public GrainIndexReplicationNotAllowedException()
    {
        IndexName = string.Empty;
        TreeName = string.Empty;
    }

    /// <summary>Initialises a new instance with the specified diagnostic message and empty context.</summary>
    /// <param name="message">Diagnostic context describing the mismatch.</param>
    public GrainIndexReplicationNotAllowedException(string message) : base(message)
    {
        IndexName = string.Empty;
        TreeName = string.Empty;
    }

    /// <summary>Initialises a new instance with the specified diagnostic message and wrapped inner exception.</summary>
    /// <param name="message">Diagnostic context describing the mismatch.</param>
    /// <param name="innerException">The underlying cause.</param>
    public GrainIndexReplicationNotAllowedException(string message, Exception innerException)
        : base(message, innerException)
    {
        IndexName = string.Empty;
        TreeName = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance naming the index, the replicated tree, and the
    /// merge mode it resolved to. The primary production throw shape.
    /// </summary>
    /// <param name="indexName">The index's logical name. Must not be <c>null</c>.</param>
    /// <param name="treeName">The replicated backing tree. Must not be <c>null</c>.</param>
    /// <param name="mergeMode">The merge mode the tree resolved to.</param>
    /// <exception cref="ArgumentNullException">Any reference argument is <c>null</c>.</exception>
    public GrainIndexReplicationNotAllowedException(
        string indexName,
        string treeName,
        LatticeMergeMode mergeMode)
        : base(BuildMessage(indexName, treeName, mergeMode))
    {
        IndexName = indexName;
        TreeName = treeName;
        MergeMode = mergeMode;
    }

    private static string BuildMessage(string indexName, string treeName, LatticeMergeMode mergeMode)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        ArgumentNullException.ThrowIfNull(treeName);

        return $"Grain index '{indexName}' is backed by tree '{treeName}', which is configured to "
            + $"replicate across clusters with merge mode '{mergeMode}', but the index has not "
            + "opted in to replication. A grain index names grain activations, which are local to "
            + "one cluster, so a replicated index tree points a receiving cluster at grains it "
            + "does not host. Either remove the tree from the replicated-tree configuration, or "
            + "opt in deliberately by setting this index's AllowReplication to true.";
    }
}
