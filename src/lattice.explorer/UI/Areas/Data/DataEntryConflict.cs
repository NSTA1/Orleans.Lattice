using Orleans.Lattice.Explorer.Core.Data;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>A write the value editor found at save that landed after it opened.</summary>
/// <param name="Entry">The entry as it stands now, or <see langword="null"/> when the key is gone.</param>
/// <param name="Version">Its version.</param>
internal sealed record DataEntryConflict(DataEntry? Entry, DataEntryVersion Version);
