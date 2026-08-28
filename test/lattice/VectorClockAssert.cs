using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Assertion helper for the ambient vector-clock frontier.
/// <para>
/// A <see cref="VersionVector"/> is a mutable CRDT, so the platform takes a
/// defensive copy wherever a caller-supplied frontier becomes durable state
/// (<c>LatticeVectorClockContext</c>'s setter) or is handed back out
/// (<c>LwwEntry</c>). Reference identity between the vector a test supplied and
/// the one it reads back is therefore no longer a property of the system - it is
/// precisely the aliasing that was removed - so these assertions compare the
/// frontier by <b>content</b> instead.
/// </para>
/// <para>
/// <see cref="VersionVector"/> is a class with no value equality, so the
/// comparison is over its per-replica entries. <c>HybridLogicalClock</c> is a
/// <c>readonly record struct</c> and compares by value, and the comparison is
/// order-insensitive because a dictionary's enumeration order is not part of the
/// frontier's meaning.
/// </para>
/// </summary>
internal static class VectorClockAssert
{
    /// <summary>
    /// Asserts that <paramref name="actual"/> carries the same per-replica
    /// frontier as <paramref name="expected"/>, without requiring the two to be
    /// the same instance.
    /// </summary>
    public static void SameFrontier(VersionVector? actual, VersionVector? expected, string? message = null)
    {
        var because = message ?? "the frontier must be preserved by value across the defensive copy";

        if (expected is null)
        {
            Assert.That(actual, Is.Null, because);
            return;
        }

        Assert.That(actual, Is.Not.Null, because);
        Assert.That(actual!.Entries, Is.EquivalentTo(expected.Entries), because);
    }
}
