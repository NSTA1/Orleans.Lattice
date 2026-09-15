using System.Reflection;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Tests for <see cref="ILatticeLeafUnavailable"/>, the public marker that lets a
/// caller outside this assembly recognise "this leaf cannot be activated" without
/// naming the concrete exception types.
/// <para>
/// The marker exists because the two exceptions carrying that condition sit on
/// different bases and one of them is <see langword="internal"/>. Before it, a
/// package above the core could catch only the public one, so the
/// memory-exhaustion shape fell straight through every handler written for the
/// condition - which is how a recoverable fault came to abort the very verb that
/// exists to recover from it.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeLeafUnavailableTests
{
    /// <summary>
    /// The public implementor. A caller that catches the marker must catch this.
    /// </summary>
    [Test]
    public void LeafProjectionStaleException_carries_the_leaf_unavailable_marker()
    {
        Assert.That(
            new LeafProjectionStaleException("stale"),
            Is.InstanceOf<ILatticeLeafUnavailable>());
    }

    /// <summary>
    /// The internal implementor, reached by reflection because that is exactly the
    /// caller's position: it cannot name the type either. This is the half the
    /// marker was added for - a handler written against the marker must catch the
    /// unaffordable-snapshot shape too, and nothing outside this assembly could
    /// assert that before.
    /// </summary>
    [Test]
    public void LeafSnapshotUnaffordableException_carries_the_leaf_unavailable_marker()
    {
        var type = typeof(ILattice).Assembly
            .GetType("Orleans.Lattice.LeafSnapshotUnaffordableException", throwOnError: true)!;

        Assert.That(
            typeof(ILatticeLeafUnavailable).IsAssignableFrom(type),
            Is.True,
            "The internal unaffordable-snapshot exception must be recognisable through the public marker, "
            + "because a caller outside this assembly has no other way to name the condition.");
    }

    /// <summary>
    /// The property that forced a marker interface rather than a shared base class,
    /// pinned so a later tidy-up cannot quietly take it away. The unaffordable
    /// exception derives <b>directly</b> from <see cref="Exception"/> because the
    /// generated same-silo deep copier resolves a base-type copier, which Orleans
    /// registers for <see cref="Exception"/> but not for its BCL subclasses -
    /// re-parenting it under a shared base would break a co-located grain result
    /// with an opaque <c>KeyNotFoundException</c> that names none of this.
    /// </summary>
    [Test]
    public void LeafSnapshotUnaffordableException_still_derives_directly_from_Exception()
    {
        var type = typeof(ILattice).Assembly
            .GetType("Orleans.Lattice.LeafSnapshotUnaffordableException", throwOnError: true)!;

        Assert.That(
            type.BaseType,
            Is.EqualTo(typeof(Exception)),
            "Deriving directly from Exception is what lets the generated deep copier resolve a base-type "
            + "copier; a shared base class would have removed that, which is why the marker is an interface.");
    }

    /// <summary>
    /// The marker must stay a pure predicate. Members on it would become a contract
    /// every implementor has to satisfy, and the two implementors deliberately
    /// carry different diagnostic payloads - a tree id and byte figures on one, a
    /// projection identity on the other. The interface says only that the leaf is
    /// unavailable; the concrete type says why.
    /// </summary>
    [Test]
    public void The_marker_declares_no_members()
    {
        Assert.That(
            typeof(ILatticeLeafUnavailable).GetMembers(
                BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static
                | BindingFlags.DeclaredOnly),
            Is.Empty);
    }

    /// <summary>
    /// The base type of the public implementor is unchanged, so the marker is
    /// additive: every existing <c>catch (InvalidOperationException)</c> written
    /// against <see cref="LeafProjectionStaleException"/> still catches it.
    /// </summary>
    [Test]
    public void LeafProjectionStaleException_still_derives_from_InvalidOperationException()
    {
        Assert.That(
            new LeafProjectionStaleException("stale"),
            Is.InstanceOf<InvalidOperationException>());
    }
}
