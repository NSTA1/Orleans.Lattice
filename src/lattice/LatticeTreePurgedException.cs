using Orleans.Serialization.Cloning;

namespace Orleans.Lattice;

/// <summary>
/// Thrown by <c>ShardRootGrain</c> when a call that was not routed through a
/// logical tree's alias - a maintenance verb, a write addressed to the physical
/// copy, or an atomic-write saga's direct terminal - reaches a shard whose
/// physical copy has been purged and that the registry does not name as live
/// (issue #4503). The shard holds no data, and serving the call would re-seed an
/// empty root on a copy nothing routes to, so it is refused instead.
/// <para>
/// A routed call on the same shard is refused with
/// <see cref="StaleTreeRoutingException"/> instead, so its router refreshes and
/// retries on the live copy. An atomic-write saga bound to a purged copy
/// recognises this type to tell a purged copy from a live refusal.
/// </para>
/// <para>
/// Derives from <see cref="InvalidOperationException"/> because that is how a
/// purged tree already refuses an operation (<c>PurgedTreeRegistrationGuard</c>),
/// so existing catch sites keep working.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeTreePurged)]
internal sealed class LatticeTreePurgedException : InvalidOperationException
{
    /// <summary>The purged physical tree id the call addressed.</summary>
    [Id(0)] public string PhysicalTreeId { get; set; } = "";

    /// <summary>Creates a new <see cref="LatticeTreePurgedException"/>.</summary>
    /// <param name="physicalTreeId">The purged physical tree id the call addressed.</param>
    public LatticeTreePurgedException(string physicalTreeId)
        : base($"Tree '{physicalTreeId}' has been purged and no longer exists. A read does not recreate it; "
            + "create the tree or write to it to reuse the id.")
    {
        PhysicalTreeId = physicalTreeId;
    }

    /// <summary>Parameterless constructor for Orleans serialization.</summary>
    public LatticeTreePurgedException() { }
}

/// <summary>
/// Same-silo deep-copier for <see cref="LatticeTreePurgedException"/>. The generated
/// copier for a <c>[GenerateSerializer]</c> exception deriving from a BCL exception
/// subclass requests a copier for that base type, which Orleans does not provide, so
/// a co-located throw would otherwise fail with an opaque <c>KeyNotFoundException</c>.
/// An exception is immutable once constructed, so returning the same instance is a
/// correct deep copy.
/// </summary>
[RegisterCopier]
internal sealed class LatticeTreePurgedExceptionCopier : IDeepCopier<LatticeTreePurgedException>
{
    /// <inheritdoc />
    public LatticeTreePurgedException DeepCopy(LatticeTreePurgedException input, CopyContext context) => input;
}
