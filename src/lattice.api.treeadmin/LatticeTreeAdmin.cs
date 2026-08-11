using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.Schema;

namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// Default <see cref="ILatticeTreeAdmin"/> implementation. Registered as a silo
/// singleton by <c>AddLatticeTreeAdminApi</c>. It owns no admin plane of its own;
/// following <b>composition over absorption</b>, it wraps the existing schema
/// control facade (<see cref="ILatticeSchemaControl"/>) by delegation and presents
/// one coherent tree-administration surface every transport binding (gRPC, MCP)
/// adapts over.
/// </summary>
/// <remarks>
/// This is the scaffolding foundation: the only operation is the capability probe,
/// which composes the wrapped schema facade's own probe. The whole-tree lifecycle
/// operations (bulk-load, delete, resize, reshard, and the rest) land in the
/// dependent sub-issues; each will delegate to (or compose) the appropriate
/// single-responsibility facade rather than re-implementing it here.
/// </remarks>
internal sealed class LatticeTreeAdmin : ILatticeTreeAdmin
{
    private readonly ILatticeSchemaControl _schemaControl;

    /// <summary>Initializes a new <see cref="LatticeTreeAdmin"/>.</summary>
    /// <param name="schemaControl">
    /// The wrapped schema-management control facade this facade composes. Must not be
    /// <c>null</c>.
    /// </param>
    /// <param name="options">The facade options. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">A required dependency is <c>null</c>.</exception>
    public LatticeTreeAdmin(
        ILatticeSchemaControl schemaControl,
        IOptions<LatticeApiTreeAdminOptions> options)
    {
        ArgumentNullException.ThrowIfNull(schemaControl);
        ArgumentNullException.ThrowIfNull(options);

        _schemaControl = schemaControl;
    }

    /// <inheritdoc />
    public async Task<LatticeTreeAdminCapabilities> ProbeCapabilitiesAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        // Composition, not absorption: the schema portion of the tree-administration
        // capability report is delegated to the wrapped schema facade, which
        // evaluates its own fail-closed gates with no side effects.
        var schema = await _schemaControl
            .ProbeCapabilitiesAsync(treeId, cancellationToken)
            .ConfigureAwait(false);

        // Whole-tree administration authority is reported default-deny at this
        // scaffolding stage: this foundation owns no whole-tree lifecycle operations
        // and therefore no whole-tree admin gate yet. The dependent sub-issues that
        // add those operations will evaluate the Admin gate here and flip this flag.
        return new LatticeTreeAdminCapabilities
        {
            TreeId = treeId,
            CanAdministerTree = false,
            Schema = schema,
        };
    }
}
