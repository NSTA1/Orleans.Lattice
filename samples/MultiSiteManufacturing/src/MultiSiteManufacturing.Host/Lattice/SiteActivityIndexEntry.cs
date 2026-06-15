using MultiSiteManufacturing.Host.Domain;
using Orleans.Lattice;

namespace MultiSiteManufacturing.Host.Lattice;

/// <summary>
/// One row from <see cref="SiteActivityIndex.ListAtSiteAsync"/> - the
/// site, the part involved, the HLC at which the activity occurred
/// (used for most-recent-first ordering and relative-time display),
/// and a short label describing what happened.
/// </summary>
public readonly record struct SiteActivityIndexEntry(
    ProcessSite Site,
    PartSerialNumber Serial,
    HybridLogicalClock Hlc,
    string Activity);
