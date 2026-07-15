using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeApiMcpGroupCapabilityMap"/>, the fixed
/// projection from each facade group to the operation mask that makes it usable
/// and its display name.
/// </summary>
[TestFixture]
public sealed class LatticeApiMcpGroupCapabilityMapTests
{
    [Test]
    public void AllGroups_lists_the_groups_in_declaration_order()
    {
        Assert.That(LatticeApiMcpGroupCapabilityMap.AllGroups, Is.EqualTo(new[]
        {
            LatticeApiMcpGroup.State,
            LatticeApiMcpGroup.Data,
            LatticeApiMcpGroup.Backup,
            LatticeApiMcpGroup.Auth,
            LatticeApiMcpGroup.Telemetry,
        }));
    }

    [Test]
    public void Existing_group_ordinals_are_unchanged()
    {
        // The access-set bitmask keys on 1 << (int)group, so the four original
        // members must keep their ordinal values; Telemetry appends after Auth.
        Assert.Multiple(() =>
        {
            Assert.That((int)LatticeApiMcpGroup.State, Is.EqualTo(0));
            Assert.That((int)LatticeApiMcpGroup.Data, Is.EqualTo(1));
            Assert.That((int)LatticeApiMcpGroup.Backup, Is.EqualTo(2));
            Assert.That((int)LatticeApiMcpGroup.Auth, Is.EqualTo(3));
            Assert.That((int)LatticeApiMcpGroup.Telemetry, Is.EqualTo(4));
        });
    }

    [Test]
    public void State_mask_is_the_read_only_surface()
    {
        Assert.That(
            LatticeApiMcpGroupCapabilityMap.RequiredOperations(LatticeApiMcpGroup.State),
            Is.EqualTo(LatticeOperation.Read | LatticeOperation.RangeRead));
    }

    [Test]
    public void Data_mask_covers_the_full_mutation_surface()
    {
        var expected = LatticeOperation.Read
            | LatticeOperation.Write
            | LatticeOperation.Delete
            | LatticeOperation.RangeRead
            | LatticeOperation.RangeDelete
            | LatticeOperation.CrdtApply
            | LatticeOperation.AtomicWrite
            | LatticeOperation.BulkLoad;

        Assert.That(
            LatticeApiMcpGroupCapabilityMap.RequiredOperations(LatticeApiMcpGroup.Data),
            Is.EqualTo(expected));
    }

    [Test]
    public void Backup_mask_is_capture_and_restore()
    {
        Assert.That(
            LatticeApiMcpGroupCapabilityMap.RequiredOperations(LatticeApiMcpGroup.Backup),
            Is.EqualTo(LatticeOperation.Backup | LatticeOperation.Restore));
    }

    [Test]
    public void Auth_mask_is_admin_only()
    {
        Assert.That(
            LatticeApiMcpGroupCapabilityMap.RequiredOperations(LatticeApiMcpGroup.Auth),
            Is.EqualTo(LatticeOperation.Admin));
    }

    [Test]
    public void Telemetry_mask_is_telemetry_only()
    {
        Assert.That(
            LatticeApiMcpGroupCapabilityMap.RequiredOperations(LatticeApiMcpGroup.Telemetry),
            Is.EqualTo(LatticeOperation.Telemetry));
    }

    [Test]
    public void Telemetry_mask_does_not_overlap_any_other_group()
    {
        var telemetry = LatticeApiMcpGroupCapabilityMap.RequiredOperations(LatticeApiMcpGroup.Telemetry);

        Assert.Multiple(() =>
        {
            foreach (var group in LatticeApiMcpGroupCapabilityMap.AllGroups)
            {
                if (group == LatticeApiMcpGroup.Telemetry)
                {
                    continue;
                }

                var other = LatticeApiMcpGroupCapabilityMap.RequiredOperations(group);
                Assert.That(telemetry & other, Is.EqualTo(LatticeOperation.None),
                    $"No other operation - including {group} - may confer telemetry.");
            }
        });
    }

    [Test]
    public void State_and_auth_masks_do_not_overlap()
    {
        var state = LatticeApiMcpGroupCapabilityMap.RequiredOperations(LatticeApiMcpGroup.State);
        var auth = LatticeApiMcpGroupCapabilityMap.RequiredOperations(LatticeApiMcpGroup.Auth);

        Assert.That(state & auth, Is.EqualTo(LatticeOperation.None),
            "An admin-only grant must not make the read-only state group usable.");
    }

    [TestCase(LatticeApiMcpGroup.State, "state")]
    [TestCase(LatticeApiMcpGroup.Data, "data")]
    [TestCase(LatticeApiMcpGroup.Backup, "backup")]
    [TestCase(LatticeApiMcpGroup.Auth, "auth")]
    [TestCase(LatticeApiMcpGroup.Telemetry, "telemetry")]
    public void DisplayName_is_the_stable_lowercase_name(LatticeApiMcpGroup group, string expected)
    {
        Assert.That(LatticeApiMcpGroupCapabilityMap.DisplayName(group), Is.EqualTo(expected));
    }
}
