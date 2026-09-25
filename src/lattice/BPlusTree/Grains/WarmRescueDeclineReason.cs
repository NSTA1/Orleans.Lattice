namespace Orleans.Lattice.BPlusTree.Grains;

internal enum WarmRescueDeclineReason
{
    UnprovenBaseline,
    CacheRehydratedOrReset,
    TopologyChanged,
    ReplayIncomplete,
    UnknownPartition,
    CheckpointUnproven,
    ActivationAnchorAhead,
    WalGapBeyondCache,
    PendingTransactions,
    MutationInFlight,
    CaptureInFlight,
    SplitInFlight,
    RetiredOrSealed,
    CaptureDeclined,
    StorageFailure,
}
