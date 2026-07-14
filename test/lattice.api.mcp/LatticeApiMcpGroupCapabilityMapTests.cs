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
    public void AllGroups_lists_the_four_groups_in_declaration_order()
    {
        Assert.That(LatticeApiMcpGroupCapabilityMap.AllGroups, Is.EqualTo(new[]
        {
            LatticeApiMcpGroup.State,
            LatticeApiMcpGroup.Data,
            LatticeApiMcpGroup.Backup,
            LatticeApiMcpGroup.Auth,
        }));
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
    public void DisplayName_is_the_stable_lowercase_name(LatticeApiMcpGroup group, string expected)
    {
        Assert.That(LatticeApiMcpGroupCapabilityMap.DisplayName(group), Is.EqualTo(expected));
    }
}
