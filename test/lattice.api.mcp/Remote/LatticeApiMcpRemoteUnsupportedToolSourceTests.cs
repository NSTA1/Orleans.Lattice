namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeApiMcpRemoteUnsupportedToolSource"/>, the fixed
/// set of tools whose backing gRPC method is not yet bound and which the remote
/// topology therefore defers (omits) from a session's tool set. Proves the
/// known-unbindable tools report unsupported, an unrelated tool does not, and the
/// null-argument guard.
/// </summary>
[TestFixture]
public sealed class LatticeApiMcpRemoteUnsupportedToolSourceTests
{
    [Test]
    public void IsUnsupported_null_tool_name_throws()
        => Assert.That(
            () => new LatticeApiMcpRemoteUnsupportedToolSource().IsUnsupported(null!),
            Throws.ArgumentNullException);

    [TestCase(LatticeApiMcpRemoteUnsupportedToolSource.StateGetTreeSummary)]
    [TestCase(LatticeApiMcpRemoteUnsupportedToolSource.StateGetShardSummaries)]
    [TestCase(LatticeApiMcpRemoteUnsupportedToolSource.StateGetPhysicalShardCount)]
    [TestCase(LatticeApiMcpRemoteUnsupportedToolSource.BackupInventory)]
    public void IsUnsupported_reports_each_unbindable_tool(string toolName)
        => Assert.That(new LatticeApiMcpRemoteUnsupportedToolSource().IsUnsupported(toolName), Is.True);

    [TestCase("lattice_state_list_trees")]
    [TestCase("lattice_backup_describe")]
    [TestCase("lattice_capabilities")]
    [TestCase("lattice_treeadmin_schema_get_policy")]
    [TestCase("lattice_treeadmin_schema_remediate")]
    [TestCase("")]
    public void IsUnsupported_supported_tool_is_false(string toolName)
        => Assert.That(new LatticeApiMcpRemoteUnsupportedToolSource().IsUnsupported(toolName), Is.False);

    [Test]
    public void Deferred_tool_names_are_exactly_the_four_unbindable_tools()
        => Assert.That(
            LatticeApiMcpRemoteUnsupportedToolSource.DeferredToolNames,
            Is.EquivalentTo(new[]
            {
                LatticeApiMcpRemoteUnsupportedToolSource.StateGetTreeSummary,
                LatticeApiMcpRemoteUnsupportedToolSource.StateGetShardSummaries,
                LatticeApiMcpRemoteUnsupportedToolSource.StateGetPhysicalShardCount,
                LatticeApiMcpRemoteUnsupportedToolSource.BackupInventory,
            }));
}
