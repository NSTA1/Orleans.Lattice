using Orleans.Lattice.Api.Schema;

namespace Orleans.Lattice.Api.Mcp;

internal static class SchemaToolSupport
{
    internal static bool IsSupported(string name, LatticeSchemaCapabilities capabilities)
    {
        const string Prefix = "lattice_treeadmin_schema_";
        if (!name.StartsWith(Prefix, StringComparison.Ordinal))
            return true;
        return name[Prefix.Length..] switch
        {
            "probe_capabilities" => true,
            "get_registry" or "register" => capabilities.SupportsRuntimeRegistration,
            "get_version_config" or "set_version_config" or "clear_version_config"
                or "advance_target_version" => capabilities.SupportsVersioning,
            "migration_start" or "advance_and_migrate_start" =>
                capabilities.SupportsVersioning && capabilities.SupportsRemediation,
            "get_policy" or "set_policy" or "clear_policy" => capabilities.SupportsEnforcement,
            "list_dead_letters" or "count_dead_letters" => capabilities.SupportsDeadLetters,
            "get_remediation_status" or "remediation_start" or "operation_status" or "operation_list"
                or "operation_cancel" => capabilities.SupportsRemediation,
            "compliance_scan_start" or "compliance_scan_status" or "compliance_scan_list"
                or "compliance_scan_cancel" => capabilities.SupportsCompliance,
            _ => false,
        };
    }
}
