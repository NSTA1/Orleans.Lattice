namespace Orleans.Lattice.Backup;

/// <summary>How a <see cref="BackupPublishRequest"/> publishes its manifest.</summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupPublishMode)]
internal enum BackupPublishMode
{
    /// <summary>
    /// A full backup whose id is still its bare content address: the publisher
    /// resolves the final id (scope-qualified when another scope owns the bare
    /// one), then writes the sink manifest and, unless deferred to a set stamp,
    /// registers the catalog row.
    /// </summary>
    Full = 0,

    /// <summary>
    /// An increment: through the fence when its id has no sink manifest yet, in
    /// place when it has one; abandoned when its base's row is gone.
    /// </summary>
    Increment = 1,

    /// <summary>
    /// A manifest whose id is final - a set's membership stamp, or a member
    /// registered unstamped: the sink manifest is rewritten and the catalog row
    /// registered.
    /// </summary>
    Register = 2,

    /// <summary>
    /// A catalog rebuild: the row is registered only while the manifest is still
    /// in the sink, and the sink is not written.
    /// </summary>
    Rebuild = 3,
}
