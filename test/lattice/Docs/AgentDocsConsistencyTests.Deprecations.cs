using System.Text.Json;

namespace Orleans.Lattice.Tests.Docs;

/// <summary>
/// The retired-deprecation half of <see cref="AgentDocsConsistencyTests"/>: once
/// the next-major removal lands, no published facade operation or MCP tool may
/// still describe the retired <c>LATTICE0002</c> blocking verbs, and the
/// diagnostic id remains reserved for history only.
/// </summary>
public sealed partial class AgentDocsConsistencyTests
{
    private const string DeprecationDiagnostic = "LATTICE0002";

    private static readonly string[] RemovedFacadeMembers =
    {
        "ILatticeBackupControl.CreateBackupAsync",
        "ILatticeBackupControl.CreateIncrementalBackupAsync",
        "ILatticeBackupControl.CreateBackupSetAsync",
        "ILatticeBackupControl.RestoreBackupAsync",
        "ILatticeBackupControl.ColdRestoreAsync",
        "ILatticeBackupControl.CheckBackupHealthAsync",
        "ILatticeBackupControl.RebuildCatalogFromSinkAsync",
        "ILatticeBackupControl.ScrubCatalogAgainstSinkAsync",
        "ILatticeSchemaControl.RemediateAsync",
        "ILatticeSchemaControl.MigrateToTargetVersionAsync",
        "ILatticeSchemaControl.AdvanceAndMigrateAsync",
        "ILatticeSchemaControl.ScanComplianceAsync",
        "ILatticeTreeAdmin.RebuildViewAsync",
        "ILatticeTreeAdmin.ReconcileViewAsync",
        "ILatticeTreeAdmin.ReconcileTagIndexAsync",
        "ILatticeTreeAdmin.ExecuteWalMoveAsync",
    };

    private static readonly string[] RemovedMcpTools =
    {
        "lattice_backup_create",
        "lattice_backup_create_incremental",
        "lattice_backup_restore",
        "lattice_treeadmin_schema_remediate",
        "lattice_treeadmin_schema_migrate_to_target",
        "lattice_treeadmin_schema_advance_and_migrate",
        "lattice_treeadmin_schema_scan_compliance",
        "lattice_treeadmin_view_rebuild",
        "lattice_treeadmin_view_reconcile",
        "lattice_treeadmin_tag_index_reconcile",
        "lattice_treeadmin_wal_move_execute",
    };

    [Test]
    public void Removed_lattice0002_facade_members_are_gone_from_the_agent_specs()
    {
        var failures = new List<string>();
        var seenOperations = 0;

        foreach (var relative in ApiFiles())
        {
            using var document = ParseJson(relative, ReadAgentText(relative));
            foreach (var operation in SpecOperations(document.RootElement))
            {
                seenOperations++;

                var id = operation.GetProperty("id").GetString()!;
                var members = InProcessMembers(operation).ToList();

                if (operation.TryGetProperty("deprecated", out var marker) &&
                    marker.ValueKind == JsonValueKind.Object &&
                    marker.TryGetProperty("diagnostic", out var diagnostic) &&
                    diagnostic.ValueKind == JsonValueKind.String &&
                    diagnostic.GetString() == DeprecationDiagnostic)
                {
                    failures.Add($"{relative}#{id}: still carries retired diagnostic {DeprecationDiagnostic}.");
                }

                foreach (var member in members.Where(RemovedFacadeMembers.Contains))
                {
                    failures.Add($"{relative}#{id}: still references removed facade member {member}.");
                }
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(seenOperations, Is.GreaterThan(0), "No agent spec operations were read, so the gate proved nothing.");
            Assert.That(failures, Is.Empty, string.Join(Environment.NewLine, failures));
        });
    }

    [Test]
    public void Removed_lattice0002_mcp_tools_are_absent_from_the_mcp_spec()
    {
        using var document = ParseJson("api/mcp.json", ReadAgentText("api/mcp.json"));
        var operations = SpecOperations(document.RootElement);
        var ids = operations.Select(o => o.GetProperty("id").GetString()!).ToHashSet(StringComparer.Ordinal);
        var lingering = RemovedMcpTools.Where(ids.Contains).OrderBy(id => id, StringComparer.Ordinal).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(operations, Is.Not.Empty, "api/mcp.json exposed no operations, so the gate proved nothing.");
            Assert.That(lingering, Is.Empty, "Retired MCP tools still appear in api/mcp.json:" + Environment.NewLine + string.Join(Environment.NewLine, lingering));
        });
    }

    private static List<JsonElement> SpecOperations(JsonElement root) =>
        root.TryGetProperty("operations", out var operations) && operations.ValueKind == JsonValueKind.Array
            ? operations.EnumerateArray().ToList()
            : new List<JsonElement>();

    private static IEnumerable<string> InProcessMembers(JsonElement operation)
    {
        if (!operation.TryGetProperty("in_process", out var value))
        {
            yield break;
        }

        switch (value.ValueKind)
        {
            case JsonValueKind.String:
                yield return value.GetString()!;
                yield break;
            case JsonValueKind.Array:
                foreach (var member in value.EnumerateArray().Where(e => e.ValueKind == JsonValueKind.String).Select(e => e.GetString()!))
                {
                    yield return member;
                }

                yield break;
            case JsonValueKind.Object:
                if (value.TryGetProperty("interface", out var type) && value.TryGetProperty("method", out var method) &&
                    type.ValueKind == JsonValueKind.String && method.ValueKind == JsonValueKind.String)
                {
                    yield return type.GetString() + "." + method.GetString();
                }

                yield break;
        }
    }
}
