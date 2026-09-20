using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Regression guard for the container's operator reach. The orphaned-leaf remedy
/// shipped with no way to invoke it on a running deployment: this container maps
/// only health and metrics routes and publishes only its MCP port, so a verb that
/// is not advertised as an MCP tool is unreachable from outside no matter how
/// completely it is implemented underneath. These tests assert the container
/// registers the tree-administration tool group, that the read-only orphaned-leaf
/// audit is advertised by it, and that the mutating repair is not - the repair sits
/// behind the tree-lifecycle opt-in, which this container deliberately leaves off
/// because the seeded local-agent grant carries no tree-lifecycle capability, so
/// advertising it could only ever produce a refusal.
/// </summary>
[TestFixture]
[FastInProcessHostFixture("Builds the host's service provider in-process and never starts the silo, so there is no storage and no cluster. Measured at 28 ms for all 4 tests including the one-time host build.")]
public sealed class RepoContextTreeAdminToolRegistrationTests
{
    private const string AuditToolName = "lattice_treeadmin_orphaned_leaves_audit";
    private const string RepairToolName = "lattice_treeadmin_orphaned_leaves_repair";

    private string _root = null!;
    private WebApplication _app = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "repocontext-treeadmin-tools-" + Guid.NewGuid().ToString("N"));
        var config = RepoContextHostConfiguration.FromConfiguration(
            new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>
                {
                    [RepoContextHostConfiguration.DataRootKey] = _root,
                })
                .Build());

        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseTestServer();
        _app = RepoContextHostBuilder.Build(builder, config);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown()
    {
        _app?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        if (Directory.Exists(_root))
        {
            try
            {
                Directory.Delete(_root, recursive: true);
            }
            catch (IOException)
            {
                // Best-effort cleanup; a background handle may briefly hold a file.
            }
        }
    }

    private IReadOnlyList<string> TreeAdminToolNames()
        => _app.Services.GetServices<ILatticeApiMcpToolGroup>()
            .OfType<TreeAdminToolGroup>()
            .SelectMany(g => g.Tools)
            .Select(t => t.ProtocolTool.Name)
            .ToList();

    [Test]
    public void The_container_registers_the_tree_admin_tool_group()
    {
        Assert.That(
            _app.Services.GetServices<ILatticeApiMcpToolGroup>().OfType<TreeAdminToolGroup>().ToList(),
            Has.Count.EqualTo(1),
            "Without this group the whole-tree operator verbs have no invocation path on this container.");
    }

    [Test]
    public void The_orphaned_leaf_audit_is_advertised()
    {
        Assert.That(TreeAdminToolNames(), Does.Contain(AuditToolName),
            "The audit is the documented first step and must be reachable over this container's MCP listener.");
    }

    [Test]
    public void The_orphaned_leaf_audit_is_annotated_read_only()
    {
        var tool = _app.Services.GetServices<ILatticeApiMcpToolGroup>()
            .OfType<TreeAdminToolGroup>()
            .SelectMany(g => g.Tools)
            .Single(t => t.ProtocolTool.Name == AuditToolName);

        Assert.Multiple(() =>
        {
            Assert.That(tool.ProtocolTool.Annotations?.ReadOnlyHint, Is.True);
            Assert.That(tool.ProtocolTool.Annotations?.DestructiveHint, Is.False);
        });
    }

    [Test]
    public void The_orphaned_leaf_repair_is_not_advertised()
    {
        Assert.That(TreeAdminToolNames(), Does.Not.Contain(RepairToolName),
            "The tree-lifecycle opt-in is deliberately off here: the seeded local-agent grant carries no "
            + "tree-lifecycle capability, so an advertised repair could only ever be refused.");
    }
}
