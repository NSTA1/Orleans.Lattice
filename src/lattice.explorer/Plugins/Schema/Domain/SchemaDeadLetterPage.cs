using Orleans.Lattice.Explorer.Schema;

namespace Orleans.Lattice.Explorer.Schema.Domain;

/// <summary>
/// A dead-letter page together with the tree it was read for.
/// <para>
/// The pairing is what makes the page safe to keep: the dead-letter queue is
/// loaded on an explicit action rather than on selection, so a loaded page must
/// survive the operator visiting another concern and coming back - but it must
/// never be shown under a different tree's heading. Carrying the tree id with
/// the page makes that a property of the value rather than a rule some component
/// has to remember.
/// </para>
/// </summary>
/// <param name="TreeId">The tree the page was read for.</param>
/// <param name="View">The read result.</param>
public sealed record SchemaDeadLetterPage(string TreeId, SchemaDeadLetterView View);
