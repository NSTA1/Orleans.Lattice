using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Explorer.Core.Data;

/// <summary>
/// A lightweight, in-process notification that the followed entry has changed,
/// emitted by <see cref="IEntryLiveFollower"/> for every forward-feed mutation
/// that targets the selected key. It carries only the metadata the Data tab
/// needs to decide to refetch (which key changed, the change kind, and the
/// revision's hybrid-logical clock); it never crosses the wire, so it is a plain
/// value type with no Orleans serialization. The follower never refetches - the
/// signal merely tells the tab to re-read the entry and re-render its value and
/// decoded CRDT current-state members.
/// </summary>
/// <param name="Key">
/// The followed key the change applies to. For a range delete that sweeps the
/// followed key, this is the followed key (not the range start), so the tab
/// always knows which entry to refetch.
/// </param>
/// <param name="Kind">The kind of the source mutation (set, delete, or range delete).</param>
/// <param name="Hlc">The hybrid-logical-clock stamp of the source mutation.</param>
public readonly record struct EntryChangeSignal(string Key, StateChangeKind Kind, HybridLogicalClock Hlc);
