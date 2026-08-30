using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.Schema.Domain;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Schema.Components;

/// <summary>
/// The compliance concern of the Schema area: a read-only audit of the selected
/// tree's current values against its enforcement policy, with the non-compliant
/// population broken down by failure reason.
/// </summary>
public partial class SchemaComplianceSection : ComponentBase
{
    /// <summary>
    /// The breakdown's column declaration, built once for the type rather than
    /// per component or per render. The per-cell
    /// <see cref="RenderFragment"/> the framework's column API requires still
    /// allocates per row per render - that is inherent to
    /// <see cref="RenderFragment{TValue}"/> and is why the breakdown is bounded
    /// by the audit rather than by the tree size.
    /// </summary>
    private static readonly LatticeTableColumn<LatticeSchemaComplianceRuleCount>[] BreakdownColumns =
    [
        new()
        {
            Header = "Reason",
            IsPrimary = true,
            Cell = static row => builder => builder.AddContent(0, row.Reason),
        },
        new()
        {
            Header = "Count",
            IsNumericOrCode = true,
            Cell = static row => builder => builder.AddContent(0, row.Count),
        },
    ];

    private SchemaReadView<LatticeSchemaComplianceReport>? _view;
    private int _revision = -1;

    /// <summary>
    /// Projects a breakdown row to its diffing key. The breakdown is grouped by
    /// reason, so the reason is already unique - and it is a reference, which
    /// keeps the framework's <c>object</c>-typed key from boxing the row struct
    /// once per row per render.
    /// </summary>
    private static object RowKey(LatticeSchemaComplianceRuleCount row) => row.Reason;

    /// <summary>The area's shared state. Must not be <see langword="null"/>.</summary>
    [Parameter]
    [EditorRequired]
    public SchemaSession Session { get; set; } = default!;

    /// <summary>
    /// The revision of the policy this audit was scanned against. An audit is
    /// only meaningful against the policy that was in force when it ran, so a
    /// changed revision discards the previous result rather than leaving a stale
    /// verdict under a reloaded policy.
    /// </summary>
    [Parameter]
    public int PolicyRevision { get; set; }

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        if (_revision == PolicyRevision)
        {
            return;
        }

        _revision = PolicyRevision;
        _view = null;
    }

    /// <summary>
    /// Runs the read-only audit of the selected tree against its policy.
    /// </summary>
    /// <remarks>
    /// Internal rather than private so a render test can reach the audited
    /// state, which is otherwise only produced by clicking the scan control.
    /// The same seam the Backups panel exposes for its row selection.
    /// </remarks>
    internal Task ScanAsync()
    {
        if (Session.TreeId is not { Length: > 0 } treeId)
        {
            return Task.CompletedTask;
        }

        Session.LastResult = null;
        return Session.RunAsync(async () => _view = await Session.Domain.ScanComplianceAsync(treeId));
    }
}
