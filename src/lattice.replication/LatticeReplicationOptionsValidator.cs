using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication;

/// <summary>
/// <see cref="IValidateOptions{TOptions}"/> implementation that fails fast
/// when <see cref="LatticeReplicationOptions"/> is misconfigured. Runs the
/// first time the options are resolved (lazy), so a host that registers
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>
/// without setting <see cref="LatticeReplicationOptions.ClusterId"/> sees a
/// clear validation error rather than producing
/// <see cref="WalRecord"/> records with no attributable origin.
/// </summary>
internal sealed class LatticeReplicationOptionsValidator : IValidateOptions<LatticeReplicationOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, LatticeReplicationOptions options)
    {
        var scope = string.IsNullOrEmpty(name)
            ? "default options instance"
            : $"options instance '{name}'";

        if (string.IsNullOrWhiteSpace(options.ClusterId))
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ClusterId)} "
                + $"must be set to a non-empty, globally-unique identifier for the local Orleans cluster ({scope}). "
                + "Replication stamps this value on every captured mutation so receivers can attribute "
                + "origin and break replication cycles; an empty value would produce unattributable "
                + "change-feed entries and is rejected.");
        }

        if (options.ReplogPartitions < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ReplogPartitions)} "
                + $"must be at least 1 ({scope}). The captured change-feed sink routes every "
                + $"{nameof(WalRecord)} to a single per-tree WAL grain keyed by "
                + "{treeId}/{partition}, where partition is hash(key) modulo this value; a value "
                + "of zero or less leaves no partitions to route to.");
        }

        if (options.WalMaxBatchEntries < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.WalMaxBatchEntries)} "
                + $"must be at least 1 ({scope}). The per-shard WAL grain refuses to flush a "
                + "zero-sized batch; a non-positive value would deadlock the commit-time observer.");
        }

        if (options.WalMaxBatchBytes < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.WalMaxBatchBytes)} "
                + $"must be at least 1 ({scope}). The byte-budget cap on a single batch must "
                + "permit at least one entry; a non-positive value would block every flush.");
        }

        if (options.MaxInboundDecompressedBytes < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.MaxInboundDecompressedBytes)} "
                + $"must be at least 1 ({scope}). The ceiling bounds the buffer the framing "
                + "decoder allocates to inflate an inbound compressed batch; a non-positive "
                + "value would reject every compressed batch.");
        }

        if (options.WalMaxPendingBatches < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.WalMaxPendingBatches)} "
                + $"must be at least 1 ({scope}). The in-memory backlog cap must permit at "
                + "least one pending batch alongside the in-flight flush.");
        }

        if (options.MaxApplyRetries < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.MaxApplyRetries)} "
                + $"must be at least 1 ({scope}). The dead-letter routing threshold cannot be "
                + "zero; a value of one parks an entry on the first failure.");
        }

        if (options.DeadLetterQueueCapacity < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.DeadLetterQueueCapacity)} "
                + $"must be at least 1 ({scope}). A zero-capacity queue cannot accept the "
                + "very entry the apply pipeline is trying to park.");
        }

        if (options.CausalBufferMaxEntries < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.CausalBufferMaxEntries)} "
                + $"must be at least 1 ({scope}). The per-tree causal-apply buffer must "
                + "permit at least one blocked entry before overflowing to the dead-letter queue.");
        }

        if (options.CausalBufferMaxBytes < 65536)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.CausalBufferMaxBytes)} "
                + $"must be at least 65536 (64 KB) ({scope}). A smaller cap would force "
                + "every typical entry to overflow to the dead-letter queue immediately on park.");
        }

        if (options.ShadowForwardDedupeCacheSize < 64)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ShadowForwardDedupeCacheSize)} "
                + $"must be at least 64 ({scope}). The shadow-forward dedupe cache must retain "
                + "enough recent identity tuples that a sustained burst of shadow-forwarded duplicates "
                + "cannot evict the cache faster than concurrent inbound deliveries race past the "
                + "per-origin high-water-mark check.");
        }

        if (options.ApplyMaxParallelRuns < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ApplyMaxParallelRuns)} "
                + $"must be at least 1 ({scope}). The receiver-side batch-apply path must permit at "
                + "least one run to apply for the inbound batch to make progress; a value of 1 keeps "
                + "apply fully sequential, and higher values bound how many independent (distinct-tree) "
                + "runs may apply concurrently.");
        }

        if (options.ContentHashDedupCacheSize < 64)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ContentHashDedupCacheSize)} "
                + $"must be at least 64 ({scope}). The content-hash dedup measurement cache must retain "
                + "enough recently-shipped keys that a pathological key burst cannot evict the cache faster "
                + "than it fills and starve the payload-re-send-rate measurement.");
        }

        if (options.ContentHashDedupElisionEnabled && !options.ContentHashDedupEnabled)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ContentHashDedupElisionEnabled)} "
                + $"requires {nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ContentHashDedupEnabled)} "
                + $"to also be set ({scope}). The content-hash payload-elision round trip is built on the same "
                + "per-(tree, peer) content hashing the re-send-rate measurement uses; enabling elision without the "
                + "master content-hash dedup switch has no hashing source and is rejected. Enable both to opt into "
                + "the sender-manifest / receiver-pull-missing exchange, or leave both off for the wire-identical default.");
        }

        if (options.WalRetention is { } retention && retention <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.WalRetention)} "
                + $"must be strictly greater than {nameof(TimeSpan)}.{nameof(TimeSpan.Zero)} when set ({scope}). "
                + "A zero or negative retention would render every entry trim-eligible the moment it lands; "
                + "leave the property unset to disable the wall-clock ceiling entirely.");
        }

        if (options.OperatorReseedMinInterval < TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.OperatorReseedMinInterval)} "
                + $"must be greater than or equal to {nameof(TimeSpan)}.{nameof(TimeSpan.Zero)} ({scope}). "
                + "A negative interval has no meaningful interpretation; set the value to "
                + $"{nameof(TimeSpan)}.{nameof(TimeSpan.Zero)} to disable operator re-seed rate limiting entirely.");
        }

        if (options.BootstrapTransientRetry is { } bootstrapRetry)
        {
            if (bootstrapRetry.MaxAttempts < 1)
            {
                return ValidateOptionsResult.Fail(
                    $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.BootstrapTransientRetry)}."
                    + $"{nameof(BoundedExponentialRetryPolicyOptions.MaxAttempts)} "
                    + $"must be at least 1 ({scope}). A zero or negative attempt budget would prevent "
                    + "the bootstrap drain from even making its first call. Set MaxAttempts to 1 to "
                    + "disable retries entirely while still running the initial attempt.");
            }

            if (bootstrapRetry.InitialDelay < TimeSpan.Zero)
            {
                return ValidateOptionsResult.Fail(
                    $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.BootstrapTransientRetry)}."
                    + $"{nameof(BoundedExponentialRetryPolicyOptions.InitialDelay)} "
                    + $"must be greater than or equal to {nameof(TimeSpan)}.{nameof(TimeSpan.Zero)} ({scope}). "
                    + "A negative initial backoff has no meaningful interpretation for the doubling schedule.");
            }

            if (bootstrapRetry.MaxDelay < bootstrapRetry.InitialDelay)
            {
                return ValidateOptionsResult.Fail(
                    $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.BootstrapTransientRetry)}."
                    + $"{nameof(BoundedExponentialRetryPolicyOptions.MaxDelay)} "
                    + $"must be greater than or equal to "
                    + $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.BootstrapTransientRetry)}."
                    + $"{nameof(BoundedExponentialRetryPolicyOptions.InitialDelay)} ({scope}). "
                    + "The doubling sequence is capped at MaxDelay, so a cap below the seed leaves the "
                    + "retry policy unable to apply even the first backoff delay.");
            }
        }

        if (options.ShipBatchSize < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ShipBatchSize)} "
                + $"must be at least 1 ({scope}). The per-peer shipper grain refuses to assemble a "
                + "zero-sized batch; a non-positive value would deadlock the outbound ship loop.");
        }

        if (options.ShipPartitionPageSize < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ShipPartitionPageSize)} "
                + $"must be at least 1 ({scope}). The per-peer shipper grain reads at least one entry "
                + "per partition per pump tick; a non-positive value would tight-loop the partition-resume drain.");
        }

        if (options.ShipCursorWriteInterval < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ShipCursorWriteInterval)} "
                + $"must be at least 1 ({scope}). Zero or negative values would suppress every durable cursor "
                + "write, so the WAL GC would never advance and a silo crash would replay the entire log.");
        }

        if (options.ShipCursorWriteMaxDelay != System.Threading.Timeout.InfiniteTimeSpan
            && options.ShipCursorWriteMaxDelay <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ShipCursorWriteMaxDelay)} "
                + $"must be strictly greater than {nameof(TimeSpan)}.{nameof(TimeSpan.Zero)} or equal to "
                + $"{nameof(System.Threading.Timeout)}.{nameof(System.Threading.Timeout.InfiniteTimeSpan)} ({scope}). "
                + "A zero or negative delay would force a durable cursor write on every advance regardless of the "
                + $"coalescing interval; set the value to {nameof(System.Threading.Timeout)}.{nameof(System.Threading.Timeout.InfiniteTimeSpan)} "
                + "to disable the time dimension and coalesce purely by ShipCursorWriteInterval.");
        }

        if (options.ShipMaxInFlight < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ShipMaxInFlight)} "
                + $"must be at least 1 ({scope}). The per-peer shipper grain must permit at least "
                + "one in-flight send for the outbound ship loop to make progress.");
        }

        if (options.ShipBackoffInitial <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ShipBackoffInitial)} "
                + $"must be strictly greater than {nameof(TimeSpan)}.{nameof(TimeSpan.Zero)} ({scope}). "
                + "A zero or negative initial backoff would tight-loop the shipper through transient "
                + "transport failures; the doubling sequence needs a non-zero seed value.");
        }

        if (options.ShipPhaseTimerPeriod <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ShipPhaseTimerPeriod)} "
                + $"must be strictly greater than {nameof(TimeSpan)}.{nameof(TimeSpan.Zero)} ({scope}). "
                + "A zero or negative phase-timer period would either tight-loop the shipper or never "
                + "fire the polling fallback when the doorbell signal is unavailable.");
        }

        if (options.LivenessProbeInterval != System.Threading.Timeout.InfiniteTimeSpan
            && options.LivenessProbeInterval <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.LivenessProbeInterval)} "
                + $"must be strictly greater than {nameof(TimeSpan)}.{nameof(TimeSpan.Zero)} or equal to "
                + $"{nameof(System.Threading.Timeout)}.{nameof(System.Threading.Timeout.InfiniteTimeSpan)} ({scope}). "
                + "A zero or negative interval would cause the shipper to fire an empty liveness probe on every "
                + $"pump tick; set the value to {nameof(System.Threading.Timeout)}.{nameof(System.Threading.Timeout.InfiniteTimeSpan)} "
                + "to disable the probe entirely.");
        }

        if (options.ShipSourceIdentityRefreshInterval < TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ShipSourceIdentityRefreshInterval)} "
                + $"must be greater than or equal to {nameof(TimeSpan)}.{nameof(TimeSpan.Zero)} ({scope}). "
                + $"{nameof(TimeSpan)}.{nameof(TimeSpan.Zero)} re-resolves the source physical identity on every "
                + "pump tick (the pre-cache behaviour); a negative interval is meaningless.");
        }

        if (options.ShipBackoffMax < options.ShipBackoffInitial)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ShipBackoffMax)} "
                + $"must be greater than or equal to {nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ShipBackoffInitial)} ({scope}). "
                + "The doubling sequence is capped at this value, so a cap below the seed leaves the "
                + "shipper unable to apply even the first backoff delay.");
        }

        if (options.ShipBackoffJitter is < 0.0 or > 1.0 or double.NaN)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ShipBackoffJitter)} "
                + $"must be in the closed interval [0.0, 1.0] ({scope}); got {options.ShipBackoffJitter}. "
                + "Jitter is a multiplicative factor applied as a +/- spread on each backoff delay; "
                + "0.0 disables jitter entirely, 1.0 randomises across the full +/-100 % range.");
        }

        if (options.MaintenanceGcInterval <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.MaintenanceGcInterval)} "
                + $"must be strictly greater than {nameof(TimeSpan)}.{nameof(TimeSpan.Zero)} ({scope}). "
                + "A zero or negative GC cadence would tight-loop the maintenance grain through "
                + $"{nameof(ILatticeWalGc)}.{nameof(ILatticeWalGc.RunOnceAsync)} calls.");
        }

        if (options.MaintenanceFallOffCheckInterval <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.MaintenanceFallOffCheckInterval)} "
                + $"must be strictly greater than {nameof(TimeSpan)}.{nameof(TimeSpan.Zero)} ({scope}). "
                + "A zero or negative fall-off cadence would tight-loop the maintenance grain through "
                + $"{nameof(ILatticeFallOffLogDetector)}.{nameof(ILatticeFallOffLogDetector.CheckAndTriggerAsync)} calls.");
        }

        if (options.DigestProbeInterval <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.DigestProbeInterval)} "
                + $"must be strictly greater than {nameof(TimeSpan)}.{nameof(TimeSpan.Zero)} ({scope}). "
                + "A zero or negative cadence would cause the anti-entropy digest-probe scheduler to "
                + "run a comparison pass on every phase tick instead of at the configured low frequency.");
        }

        if (options.DigestProbeJitter is < 0.0 or > 1.0 or double.NaN)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.DigestProbeJitter)} "
                + $"must be in the closed interval [0.0, 1.0] ({scope}); got {options.DigestProbeJitter}. "
                + "Jitter is a multiplicative factor applied as a +/- spread on the digest-probe interval; "
                + "0.0 disables jitter entirely, 1.0 randomises across the full +/-100 % range.");
        }

        if (options.MerkleWalkMaxDepth < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.MerkleWalkMaxDepth)} "
                + $"must be at least 1 ({scope}); got {options.MerkleWalkMaxDepth}. "
                + "The Merkle-walk localisation pass descends a shard's internal-node tree up to this "
                + "depth before aborting; a non-positive cap would localise nothing.");
        }

        if (options.MerkleWalkMaxBytes < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.MerkleWalkMaxBytes)} "
                + $"must be at least 1 ({scope}); got {options.MerkleWalkMaxBytes}. "
                + "The Merkle-walk localisation pass caps the cumulative digest bytes it inspects at this "
                + "value; a non-positive budget would abort before inspecting any node.");
        }

        if (options.LeafReReplayMaxEntries < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.LeafReReplayMaxEntries)} "
                + $"must be at least 1 ({scope}); got {options.LeafReReplayMaxEntries}. "
                + "The targeted leaf re-replay repair pass re-ships at most this many WAL entries per pass; "
                + "a non-positive cap would re-ship nothing.");
        }

        if (options.LeafReReplayMaxBytes < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.LeafReReplayMaxBytes)} "
                + $"must be at least 1 ({scope}); got {options.LeafReReplayMaxBytes}. "
                + "The targeted leaf re-replay repair pass caps the cumulative re-shipped payload bytes at this "
                + "value; a non-positive budget would re-ship nothing.");
        }

        if (options.BootstrapFallbackMaxEntries < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.BootstrapFallbackMaxEntries)} "
                + $"must be at least 1 ({scope}); got {options.BootstrapFallbackMaxEntries}. "
                + "The bootstrap-snapshot fallback re-ships at most this many committed-projection entries per pass; "
                + "a non-positive cap would re-ship nothing.");
        }

        if (options.BootstrapFallbackMaxBytes < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.BootstrapFallbackMaxBytes)} "
                + $"must be at least 1 ({scope}); got {options.BootstrapFallbackMaxBytes}. "
                + "The bootstrap-snapshot fallback caps the cumulative re-shipped payload bytes at this "
                + "value; a non-positive budget would re-ship nothing.");
        }

        // Reject the all-bits-zero "well-known None" sentinel range
        // only when the host has clearly typoed - i.e. the tag is
        // in the core-reserved range [0x02, 0x7F] but is not a
        // defined LatticeCompression member. Tags in [0x80, 0xFF]
        // are reserved for host-defined algorithms and are validated
        // at encode/decode time by the encoder's compressor lookup
        // (missing compressor -> NotSupportedException), not at
        // options-validation time, because the host may register
        // its compressor independently of the options binding.
        var compressionTag = (byte)options.FramingCompression;
        if (compressionTag is >= 0x02 and < 0x80 && !Enum.IsDefined(options.FramingCompression))
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.FramingCompression)} "
                + $"must be a defined {nameof(LatticeCompression)} value or a host-defined tag in the reserved [0x80, 0xFF] range ({scope}); "
                + $"got '0x{compressionTag:X2}'. "
                + "Core-defined tags are LatticeCompression.None (0x00), LatticeCompression.Zstd (0x01), and LatticeCompression.ZstdDictionary (0x02); "
                + "host-defined algorithms must cast a byte in [0x80, 0xFF] into LatticeCompression and register a matching ILatticeCompressor via AddLatticeCompressor.");
        }

        if (options.FramingCompression is LatticeCompression.Zstd or LatticeCompression.ZstdDictionary
            && (options.FramingCompressionLevel < 1 || options.FramingCompressionLevel > 22))
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.FramingCompressionLevel)} "
                + $"must be in the closed interval [1, 22] when {nameof(LatticeReplicationOptions.FramingCompression)} "
                + $"is {nameof(LatticeCompression.Zstd)} or {nameof(LatticeCompression.ZstdDictionary)} ({scope}); got {options.FramingCompressionLevel}. "
                + "Zstandard accepts levels 1 (fastest) through 22 (highest ratio); the canonical default is 3.");
        }

        if (options.FramingCompression == LatticeCompression.ZstdDictionary
            && options.FramingCompressionDictionaryId == 0)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.FramingCompressionDictionaryId)} "
                + $"must be a non-zero shared-dictionary id when {nameof(LatticeReplicationOptions.FramingCompression)} "
                + $"is {nameof(LatticeCompression.ZstdDictionary)} ({scope}); got 0. "
                + "The reserved id 0 means 'no dictionary'; select ZstdDictionary together with the id of a dictionary "
                + "registered via an ILatticeCompressionDictionaryProvider, or use LatticeCompression.Zstd for dictionary-less compression.");
        }

        if (options.FramingCompressionMinBatchBytes < 0)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.FramingCompressionMinBatchBytes)} "
                + $"must be non-negative ({scope}); got {options.FramingCompressionMinBatchBytes}. "
                + "A negative value has no defined meaning; a zero value disables the threshold so every "
                + "non-empty batch is compressed when the algorithm is non-None.");
        }

        if (options.MinimumSupportedWireVersion < 1
            || options.MinimumSupportedWireVersion > EncodedBatchHeader.CurrentWireVersion)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.MinimumSupportedWireVersion)} "
                + $"must lie in the closed interval [1, {EncodedBatchHeader.CurrentWireVersion}] ({scope}); "
                + $"got {options.MinimumSupportedWireVersion}. The minimum supported wire version is the "
                + "oldest framing version the sender will down-encode for; a peer below it fails fast, and a "
                + "value above the sender's own current version could never be satisfied by any peer.");
        }

        if (options.UnknownPeerWireVersionFloor < options.MinimumSupportedWireVersion
            || options.UnknownPeerWireVersionFloor > EncodedBatchHeader.CurrentWireVersion)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.UnknownPeerWireVersionFloor)} "
                + $"must lie in the closed interval "
                + $"[{nameof(LatticeReplicationOptions.MinimumSupportedWireVersion)}, "
                + $"{EncodedBatchHeader.CurrentWireVersion}] ({scope}); got {options.UnknownPeerWireVersionFloor} "
                + $"with a minimum of {options.MinimumSupportedWireVersion}. The unknown-peer floor is the "
                + "conservative version used until a peer advertises its capability; it cannot be below the "
                + "minimum supported version nor above the sender's current version.");
        }

        if (options.AdaptiveBatchIncrement < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.AdaptiveBatchIncrement)} "
                + $"must be at least 1 ({scope}). The adaptive batch-size controller's additive-increase "
                + "step must move the effective batch size forward by at least one entry per healthy ack; "
                + "a non-positive step would never let the controller re-grow after a back-off.");
        }

        if (options.AdaptiveBatchDecreaseFactor is <= 0.0 or >= 1.0 || double.IsNaN(options.AdaptiveBatchDecreaseFactor))
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.AdaptiveBatchDecreaseFactor)} "
                + $"must be in the open interval (0.0, 1.0) ({scope}); got {options.AdaptiveBatchDecreaseFactor}. "
                + "The multiplicative-decrease factor scales the effective batch size down on a latency rise or "
                + "send failure; a factor of 0 or below would collapse it instantly, and a factor of 1 or above "
                + "would never shrink it.");
        }

        if (options.AdaptiveBatchLatencyThreshold <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.AdaptiveBatchLatencyThreshold)} "
                + $"must be strictly greater than {nameof(TimeSpan)}.{nameof(TimeSpan.Zero)} ({scope}). "
                + "The controller compares the sliding-window mean ack latency against this threshold to choose "
                + "increase vs. decrease; a zero or negative threshold would force a multiplicative decrease on "
                + "every ack and stall the stream at a single-entry batch.");
        }

        if (options.AdaptiveBatchWindowLength < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.AdaptiveBatchWindowLength)} "
                + $"must be at least 1 ({scope}). The adaptive batch-size controller averages ack latency over a "
                + "sliding window of this many recent acks; a non-positive window has no samples to average.");
        }

        if (options.RemediationTrafficBudgetFraction is <= 0.0 or > 1.0
            || double.IsNaN(options.RemediationTrafficBudgetFraction))
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.RemediationTrafficBudgetFraction)} "
                + $"must be in the half-open interval (0.0, 1.0] ({scope}); got {options.RemediationTrafficBudgetFraction}. "
                + "The remediation traffic budget is this fraction of the ship-batch size; a fraction at or below 0 "
                + "would forbid all remediation, and a fraction above 1 would let repair traffic exceed the ordinary "
                + "ship-batch budget.");
        }

        if (options.RemediationTrafficWindow <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.RemediationTrafficWindow)} "
                + $"must be strictly greater than {nameof(TimeSpan)}.{nameof(TimeSpan.Zero)} ({scope}). "
                + "The remediation traffic budget is measured over a window of this length before its consumed-entry "
                + "counter resets; a zero or negative window has no defined accounting interval.");
        }

        if (options.RemediationFailureThreshold < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.RemediationFailureThreshold)} "
                + $"must be at least 1 ({scope}); got {options.RemediationFailureThreshold}. "
                + "The remediation circuit breaker opens after this many consecutive failures for a tree/peer; "
                + "a non-positive threshold has no defined trip point.");
        }

        if (options.RemediationCircuitResetInterval <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.RemediationCircuitResetInterval)} "
                + $"must be strictly greater than {nameof(TimeSpan)}.{nameof(TimeSpan.Zero)} ({scope}). "
                + "The remediation circuit breaker stays open for this cooldown before it half-opens; a zero or "
                + "negative cooldown would leave a tripped breaker permanently open or never cooling.");
        }

        if (options.ReplicatedTrees is { } trees)
        {
            foreach (var kvp in trees)
            {
                if (string.IsNullOrWhiteSpace(kvp.Key))
                {
                    return ValidateOptionsResult.Fail(
                        $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ReplicatedTrees)} "
                        + $"must not contain null, empty, or whitespace tree-id keys ({scope}). "
                        + "Every replicated tree must be declared by its concrete tree id so the "
                        + "commit-time observer can resolve the per-tree replication mode.");
                }

                if (!Enum.IsDefined(kvp.Value))
                {
                    return ValidateOptionsResult.Fail(
                        $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ReplicatedTrees)} "
                        + $"declares tree '{kvp.Key}' with an undefined "
                        + $"{nameof(LatticeMergeMode)} value '{(int)kvp.Value}' ({scope}). "
                        + $"Use one of {nameof(LatticeMergeMode.LwwRegister)}, "
                        + $"{nameof(LatticeMergeMode.OrSet)}, "
                        + $"{nameof(LatticeMergeMode.PnCounter)}, "
                        + $"{nameof(LatticeMergeMode.VersionVector)}, "
                        + $"{nameof(LatticeMergeMode.MvRegister)}, "
                        + $"{nameof(LatticeMergeMode.Sequence)}, "
                        + $"{nameof(LatticeMergeMode.OrFlag)}, "
                        + $"{nameof(LatticeMergeMode.RwFlag)}, "
                        + $"{nameof(LatticeMergeMode.GCounter)}, "
                        + $"{nameof(LatticeMergeMode.GSet)}, "
                        + $"{nameof(LatticeMergeMode.RwSet)}, "
                        + $"{nameof(LatticeMergeMode.MaxRegister)}, or "
                        + $"{nameof(LatticeMergeMode.MinRegister)}.");
                }
            }
        }

        return ValidateOptionsResult.Success;
    }
}

