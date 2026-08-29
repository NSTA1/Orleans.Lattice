using System.Globalization;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.Plugins.Schema.Domain;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Plugins.Schema.Components;

/// <summary>
/// The dead-letter concern of the Schema area: the read-only, bounded page of
/// items strict-mode ingest diverted rather than applied.
/// </summary>
public partial class SchemaDeadLettersTab : ComponentBase
{
    /// <summary>
    /// The page size the queue is read in. Bounded so a large queue cannot pull
    /// an unbounded page across the circuit.
    /// </summary>
    private const int PageSize = 100;

    /// <summary>
    /// The queue's column declaration, built once for the type. The per-cell
    /// fragment the column API requires still allocates per row per render,
    /// which is inherent to <see cref="RenderFragment{TValue}"/>; the page size
    /// above is what bounds it.
    /// </summary>
    private static readonly LatticeTableColumn<LatticeSchemaDeadLetterEntry>[] EntryColumns =
    [
        new()
        {
            Header = "Key",
            IsPrimary = true,
            IsNumericOrCode = true,
            Cell = static entry => builder => builder.AddContent(0, entry.Key),
        },
        new()
        {
            Header = "Reason",
            Cell = static entry => builder => builder.AddContent(0, entry.Reason),
        },
        new()
        {
            Header = "Source",
            Cell = static entry => builder => builder.AddContent(0, entry.Source),
        },
        new()
        {
            Header = "Bytes",
            IsNumericOrCode = true,
            Cell = static entry => builder => builder.AddContent(0, entry.ValueByteLength),
        },
        new()
        {
            Header = "When (UTC)",
            IsNumericOrCode = true,
            Cell = static entry => builder => builder.AddContent(
                0,
                entry.TimestampUtc.UtcDateTime.ToString("u", CultureInfo.InvariantCulture)),
        },
    ];

    private SchemaDeadLetterView? _view;
    private string? _boundTreeId;

    /// <summary>The area's shared state. Must not be <see langword="null"/>.</summary>
    [Parameter]
    [EditorRequired]
    public SchemaSession Session { get; set; } = default!;

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        // The queue loads on an explicit action, so switching trees discards the
        // previous tree's page rather than reloading - but it must never leave
        // one tree's dead letters displayed under another tree's heading.
        var treeId = Session.Grants.TreeId;
        if (string.Equals(_boundTreeId, treeId, StringComparison.Ordinal))
        {
            return;
        }

        _boundTreeId = treeId;
        _view = null;
    }

    private Task LoadAsync()
    {
        if (Session.TreeId is not { Length: > 0 } treeId)
        {
            return Task.CompletedTask;
        }

        Session.LastResult = null;
        return Session.RunAsync(async () => _view = await Session.Domain.ListDeadLettersAsync(treeId, PageSize));
    }
}
