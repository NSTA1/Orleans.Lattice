using System.Globalization;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.Schema.Domain;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Schema.Components;

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

    /// <summary>The area's shared state. Must not be <see langword="null"/>.</summary>
    [Parameter]
    [EditorRequired]
    public SchemaSession Session { get; set; } = default!;

    /// <summary>
    /// The page currently in view: the session's loaded page while it still
    /// belongs to the selected tree, otherwise nothing. Reading it through the
    /// session is what lets an explicitly loaded queue survive a visit to another
    /// concern, which unmounts this component.
    /// </summary>
    private SchemaDeadLetterView? View =>
        Session.DeadLetters is { } page && string.Equals(page.TreeId, Session.TreeId, StringComparison.Ordinal)
            ? page.View
            : null;

    private Task LoadAsync()
    {
        if (Session.TreeId is not { Length: > 0 } treeId)
        {
            return Task.CompletedTask;
        }

        Session.LastResult = null;
        return Session.RunAsync(async () =>
            Session.DeadLetters = new SchemaDeadLetterPage(
                treeId,
                await Session.Domain.ListDeadLettersAsync(treeId, PageSize)));
    }
}
