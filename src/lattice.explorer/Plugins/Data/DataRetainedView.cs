using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Explorer.Plugins.Data;

/// <summary>
/// The retained view of one selection the value drill-down surface reopens on:
/// the committed key prefix, the page size, and the scan isolation.
/// <para>
/// A single record rather than three loose reads, so the view seeds itself in
/// one call and a later addition to the retained set does not change the
/// contract's shape.
/// </para>
/// </summary>
/// <param name="KeyPrefix">The committed starts-with key filter, or empty for none.</param>
/// <param name="PageSize">The retained page size, already normalised to a selectable value.</param>
/// <param name="ScanMode">The retained cursor isolation for a fresh scan.</param>
/// <param name="TagIndexName">The retained tag index, or <see langword="null"/> when none was chosen.</param>
public readonly record struct DataRetainedView(
    string KeyPrefix,
    int PageSize,
    EntryScanMode ScanMode,
    string? TagIndexName);
