using System.Reflection;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeOperationTag"/>: the single-flag tag cache
/// returns the expected tag for every single-bit operation (including the newest
/// bit, <see cref="LatticeOperation.AppInstall"/>), maps the empty request to
/// <c>none</c>, and falls back to the flags string for a composite mask. Also
/// pins the cached-table size so bit 15 (AppInstall) is covered by the
/// allocation-free cached path rather than the fallback.
/// </summary>
[TestFixture]
public sealed class LatticeOperationTagTests
{
    [Test]
    public void For_returns_the_member_name_for_every_single_bit_operation()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeOperationTag.For(LatticeOperation.Read), Is.EqualTo("Read"));
            Assert.That(LatticeOperationTag.For(LatticeOperation.Write), Is.EqualTo("Write"));
            Assert.That(LatticeOperationTag.For(LatticeOperation.Delete), Is.EqualTo("Delete"));
            Assert.That(LatticeOperationTag.For(LatticeOperation.RangeRead), Is.EqualTo("RangeRead"));
            Assert.That(LatticeOperationTag.For(LatticeOperation.RangeDelete), Is.EqualTo("RangeDelete"));
            Assert.That(LatticeOperationTag.For(LatticeOperation.CrdtApply), Is.EqualTo("CrdtApply"));
            Assert.That(LatticeOperationTag.For(LatticeOperation.AtomicWrite), Is.EqualTo("AtomicWrite"));
            Assert.That(LatticeOperationTag.For(LatticeOperation.BulkLoad), Is.EqualTo("BulkLoad"));
            Assert.That(LatticeOperationTag.For(LatticeOperation.Admin), Is.EqualTo("Admin"));
            Assert.That(LatticeOperationTag.For(LatticeOperation.Backup), Is.EqualTo("Backup"));
            Assert.That(LatticeOperationTag.For(LatticeOperation.Restore), Is.EqualTo("Restore"));
            Assert.That(LatticeOperationTag.For(LatticeOperation.SchemaAdmin), Is.EqualTo("SchemaAdmin"));
            Assert.That(LatticeOperationTag.For(LatticeOperation.Telemetry), Is.EqualTo("Telemetry"));
            Assert.That(LatticeOperationTag.For(LatticeOperation.Replication), Is.EqualTo("Replication"));
            Assert.That(LatticeOperationTag.For(LatticeOperation.TreeLifecycle), Is.EqualTo("TreeLifecycle"));
            Assert.That(LatticeOperationTag.For(LatticeOperation.AppInstall), Is.EqualTo("AppInstall"));
        });
    }

    [Test]
    public void For_resolves_every_defined_operation_member_through_the_cached_table()
    {
        var field = typeof(LatticeOperationTag).GetField(
            "SingleFlagNames",
            BindingFlags.NonPublic | BindingFlags.Static);
        var names = (string[])field!.GetValue(null)!;
        var members = Enum.GetValues<LatticeOperation>()
            .Where(member => member != LatticeOperation.None)
            .ToArray();

        Assert.That(members, Does.Contain(LatticeOperation.AppInstall));
        Assert.Multiple(() =>
        {
            foreach (var member in members)
            {
                var bit = System.Numerics.BitOperations.TrailingZeroCount((uint)member);

                // Every defined member must sit inside the cached table so its tag
                // is served by the allocation-free indexed path, and resolve to its name.
                Assert.That(bit, Is.LessThan(names.Length), $"{member} is outside the cached tag table.");
                Assert.That(LatticeOperationTag.For(member), Is.EqualTo(member.ToString()));
                Assert.That(LatticeOperationTag.For(member), Is.SameAs(names[bit]));
            }
        });
    }

    [Test]
    public void For_maps_none_to_the_none_tag()
    {
        Assert.That(LatticeOperationTag.For(LatticeOperation.None), Is.EqualTo("none"));
    }

    [Test]
    public void For_falls_back_to_the_flags_string_for_a_composite_mask()
    {
        var composite = LatticeOperation.Read | LatticeOperation.Write;

        Assert.That(LatticeOperationTag.For(composite), Is.EqualTo(composite.ToString()));
    }

    [Test]
    public void Cached_single_flag_table_covers_bit_fifteen_app_install()
    {
        var field = typeof(LatticeOperationTag).GetField(
            "SingleFlagNames",
            BindingFlags.NonPublic | BindingFlags.Static);
        var names = (string[])field!.GetValue(null)!;

        // Bits 14 (TreeLifecycle) and 15 (AppInstall) must be inside the cached
        // table, so the common single-flag case takes the allocation-free indexed path.
        Assert.That(names.Length, Is.EqualTo(16));
        Assert.That(names[12], Is.EqualTo("Telemetry"));
        Assert.That(names[13], Is.EqualTo("Replication"));
        Assert.That(names[14], Is.EqualTo("TreeLifecycle"));
        Assert.That(names[15], Is.EqualTo("AppInstall"));
    }
}
