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
    [TestCase("lattice_treeadmin_schema_get_policy")]
    [TestCase("lattice_treeadmin_schema_set_policy")]
    [TestCase("lattice_treeadmin_schema_remediate")]
    public void IsUnsupported_reports_each_unbindable_tool(string toolName)
        => Assert.That(new LatticeApiMcpRemoteUnsupportedToolSource().IsUnsupported(toolName), Is.True);

    [TestCase("lattice_state_list_trees")]
    [TestCase("lattice_backup_describe")]
    [TestCase("lattice_capabilities")]
    [TestCase("")]
    public void IsUnsupported_supported_tool_is_false(string toolName)
        => Assert.That(new LatticeApiMcpRemoteUnsupportedToolSource().IsUnsupported(toolName), Is.False);

    [Test]
    public void Deferred_tool_names_include_the_unbindable_state_and_backup_tools()
        => Assert.That(
            LatticeApiMcpRemoteUnsupportedToolSource.DeferredToolNames,
            Is.SupersetOf(new[]
            {
                LatticeApiMcpRemoteUnsupportedToolSource.StateGetTreeSummary,
                LatticeApiMcpRemoteUnsupportedToolSource.StateGetShardSummaries,
                LatticeApiMcpRemoteUnsupportedToolSource.StateGetPhysicalShardCount,
                LatticeApiMcpRemoteUnsupportedToolSource.BackupInventory,
            }));

    [Test]
    public void Deferred_tool_names_include_every_treeadmin_schema_tool()
        => Assert.That(
            LatticeApiMcpRemoteUnsupportedToolSource.DeferredToolNames,
            Is.SupersetOf(new[]
            {
                "lattice_treeadmin_schema_get_policy",
                "lattice_treeadmin_schema_list_dead_letters",
                "lattice_treeadmin_schema_count_dead_letters",
                "lattice_treeadmin_schema_get_version_config",
                "lattice_treeadmin_schema_get_remediation_status",
                "lattice_treeadmin_schema_scan_compliance",
                "lattice_treeadmin_schema_probe_capabilities",
                "lattice_treeadmin_schema_set_policy",
                "lattice_treeadmin_schema_clear_policy",
                "lattice_treeadmin_schema_set_version_config",
                "lattice_treeadmin_schema_clear_version_config",
                "lattice_treeadmin_schema_advance_target_version",
                "lattice_treeadmin_schema_advance_and_migrate",
                "lattice_treeadmin_schema_migrate_to_target",
                "lattice_treeadmin_schema_remediate",
            }));
}
