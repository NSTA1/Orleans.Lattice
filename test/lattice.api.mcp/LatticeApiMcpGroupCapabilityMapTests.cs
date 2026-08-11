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
            LatticeApiMcpGroup.Replication,
            LatticeApiMcpGroup.TreeAdmin,
            LatticeApiMcpGroup.RepoContext,
        }));
    }

    [Test]
    public void Existing_group_ordinals_are_unchanged()
    {
        // The access-set bitmask keys on 1 << (int)group, so the original
        // members must keep their ordinal values; Telemetry appends after Auth,
        // Replication appends after Telemetry, TreeAdmin appends after
        // Replication, and RepoContext appends after TreeAdmin.
        Assert.Multiple(() =>
        {
            Assert.That((int)LatticeApiMcpGroup.State, Is.EqualTo(0));
            Assert.That((int)LatticeApiMcpGroup.Data, Is.EqualTo(1));
            Assert.That((int)LatticeApiMcpGroup.Backup, Is.EqualTo(2));
            Assert.That((int)LatticeApiMcpGroup.Auth, Is.EqualTo(3));
            Assert.That((int)LatticeApiMcpGroup.Telemetry, Is.EqualTo(4));
            Assert.That((int)LatticeApiMcpGroup.Replication, Is.EqualTo(5));
            Assert.That((int)LatticeApiMcpGroup.TreeAdmin, Is.EqualTo(6));
            Assert.That((int)LatticeApiMcpGroup.RepoContext, Is.EqualTo(7));
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
    public void Replication_mask_is_replication_only()
    {
        Assert.That(
            LatticeApiMcpGroupCapabilityMap.RequiredOperations(LatticeApiMcpGroup.Replication),
            Is.EqualTo(LatticeOperation.Replication));
    }

    [Test]
    public void Replication_mask_does_not_overlap_any_other_group()
    {
        var replication = LatticeApiMcpGroupCapabilityMap.RequiredOperations(LatticeApiMcpGroup.Replication);

        Assert.Multiple(() =>
        {
            foreach (var group in LatticeApiMcpGroupCapabilityMap.AllGroups)
            {
                if (group == LatticeApiMcpGroup.Replication)
                {
                    continue;
                }

                var other = LatticeApiMcpGroupCapabilityMap.RequiredOperations(group);
                Assert.That(replication & other, Is.EqualTo(LatticeOperation.None),
                    $"No other operation - including {group} - may confer replication.");
            }
        });
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

    [Test]
    public void TreeAdmin_mask_is_admin_only()
    {
        Assert.That(
            LatticeApiMcpGroupCapabilityMap.RequiredOperations(LatticeApiMcpGroup.TreeAdmin),
            Is.EqualTo(LatticeOperation.Admin));
    }

    [Test]
    public void RepoContext_mask_matches_the_data_plane_surface()
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
            LatticeApiMcpGroupCapabilityMap.RequiredOperations(LatticeApiMcpGroup.RepoContext),
            Is.EqualTo(expected));
    }

    [TestCase(LatticeApiMcpGroup.State, "state")]
    [TestCase(LatticeApiMcpGroup.Data, "data")]
    [TestCase(LatticeApiMcpGroup.Backup, "backup")]
    [TestCase(LatticeApiMcpGroup.Auth, "auth")]
    [TestCase(LatticeApiMcpGroup.Telemetry, "telemetry")]
    [TestCase(LatticeApiMcpGroup.Replication, "replication")]
    [TestCase(LatticeApiMcpGroup.TreeAdmin, "treeadmin")]
    [TestCase(LatticeApiMcpGroup.RepoContext, "repocontext")]
    public void DisplayName_is_the_stable_lowercase_name(LatticeApiMcpGroup group, string expected)
    {
        Assert.That(LatticeApiMcpGroupCapabilityMap.DisplayName(group), Is.EqualTo(expected));
    }
}
