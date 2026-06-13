using System.Buffers;
using System.Collections.Frozen;
using System.Diagnostics.Metrics;
using ZstdSharp;

namespace Orleans.Lattice;

/// <summary>
/// An <see cref="ILatticeCompressionDictionaryProvider"/> that trains a shared
/// Zstandard dictionary at runtime from a bounded, sampled reservoir of observed
/// payloads. It is the opt-in, runtime alternative to
/// <see cref="OperatorSuppliedCompressionDictionaryProvider"/> (offline,
/// pre-trained dictionaries).
/// <para>
/// Auto-training is off by default (see
/// <see cref="CompressionDictionaryTrainingOptions.Enabled"/>): while disabled,
/// <see cref="Observe"/> and <see cref="TryTrain"/> are allocation-free no-ops,
/// no telemetry is emitted, and <see cref="CurrentDictionaryId"/> stays
/// <c>0</c>. When enabled, hosts feed representative payloads through
/// <see cref="Observe"/> and drive training from a turn-safe schedule via
/// <see cref="TryTrain"/> (an explicit pass rather than a hidden timer, so
/// cadence is deterministic). Each successful pass builds a dictionary fully and
/// then atomically rolls over to a new monotonically increasing dictionary id;
/// a bounded ring of recent versions stays resolvable so a frame compressed
/// against a just-superseded version still decompresses, and an unknown or
/// reserved id resolves as absent so a consumer degrades safely rather than
/// mis-decoding.
/// </para>
/// <para>
/// Trained dictionary ids are enumerable through the
/// <see cref="ILatticeCompressionDictionaryCatalog"/> this provider also
/// implements, so a receiver advertises them to peers (via
/// <c>ReplicationAck.AdvertisedDictionaries</c>) and an opted-in sender can
/// gate dictionary compression on whether the target peer can resolve the
/// chosen dictionary. The advertised id set carries a content fingerprint
/// alongside each id, so two clusters that each auto-train an id 1 dictionary
/// over different corpora never negotiate a bare-id match. The dictionary
/// bytes themselves are distributed out of band (operator-provisioned) or via
/// the replication transport's pull-bytes seam; this provider produces and
/// resolves them but does not itself ship them.
/// </para>
/// </summary>
public sealed class AutoTrainingCompressionDictionaryProvider
    : ILatticeCompressionDictionaryProvider, ILatticeCompressionDictionaryCatalog, ILatticeCompressionDictionarySink, ILatticeActiveCompressionDictionary, ILatticeCompressionDictionarySampler, IDisposable
{
    private const int ProbeCompressionLevel = 3;
    private const ulong FnvOffsetBasis = 14695981039346656037UL;
    private const ulong FnvPrime = 1099511628211UL;

    // Observable gauges read the most recently constructed provider, matching
    // the DI singleton model. Registration is process-wide and idempotent so
    // the gauges cost nothing when no provider is constructed and nothing when
    // no listener is attached.
    private static readonly object GaugeRegistrationLock = new();
    private static volatile AutoTrainingCompressionDictionaryProvider? _current;
    private static bool _gaugesRegistered;

    private readonly CompressionDictionaryTrainingOptions _options;
    private readonly TimeProvider _time;
    private readonly CompressionDictionaryTrainingReservoir _reservoir;
    private readonly object _publishGate = new();
    private readonly Queue<uint> _retained = new();

    private volatile FrozenDictionary<uint, ReadOnlyMemory<byte>> _versions
        = FrozenDictionary<uint, ReadOnlyMemory<byte>>.Empty;

    // Sorted, immutable snapshot of the ids currently resolvable via
    // _versions. Rebuilt under _publishGate on every roll-over/eviction and
    // read lock-free by AvailableDictionaryIds, so capability advertisement
    // never allocates and never enumerates the live FrozenDictionary while it
    // is being swapped.
    private volatile uint[] _availableIds = Array.Empty<uint>();
    private uint _currentId;        // 0 until the first successful train.
    private uint _nextId;           // Next id to assign on roll-over.
    private ulong _currentHash;     // FNV-1a of the current dictionary bytes.
    private long _lastTrainTicks;   // UtcNow ticks of the last attempt (0 = never).
    private int _trainingInFlight;  // 0/1 single-pass guard.
    private volatile bool _disposed;

    /// <summary>
    /// Initialises the provider with the supplied options and the system clock.
    /// </summary>
    /// <param name="options">The training options. Validated on construction.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The options are outside their valid ranges.</exception>
    public AutoTrainingCompressionDictionaryProvider(CompressionDictionaryTrainingOptions options)
        : this(options, null)
    {
    }

    /// <summary>
    /// Initialises the provider with the supplied options and an explicit
    /// <paramref name="timeProvider"/> (used by tests to drive the training
    /// cadence deterministically).
    /// </summary>
    /// <param name="options">The training options. Validated on construction.</param>
    /// <param name="timeProvider">
    /// The clock used for the cadence gate; <see langword="null"/> uses
    /// <see cref="TimeProvider.System"/>.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The options are outside their valid ranges.</exception>
    public AutoTrainingCompressionDictionaryProvider(
        CompressionDictionaryTrainingOptions options,
        TimeProvider? timeProvider)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();

        _options = options;
        _time = timeProvider ?? TimeProvider.System;
        _reservoir = new CompressionDictionaryTrainingReservoir(
            options.MaxSampleCount,
            options.MaxReservoirBytes,
            options.MaxSampleBytes,
            options.SamplingRate);
        _nextId = options.FirstDictionaryId;

        lock (GaugeRegistrationLock)
        {
            _current = this;
            if (!_gaugesRegistered)
            {
                RegisterGauges();
                _gaugesRegistered = true;
            }
        }
    }

    /// <summary>Whether auto-training is enabled for this provider.</summary>
    public bool Enabled => _options.Enabled;

    /// <summary>
    /// The minimum interval the provider enforces between successive training
    /// passes (<see cref="CompressionDictionaryTrainingOptions.MinTrainingInterval"/>).
    /// A driver that pumps <see cref="TryTrain"/> reads this to size its poll
    /// cadence; <see cref="TryTrain"/> itself re-checks the interval, so calling
    /// it more often is harmless.
    /// </summary>
    public TimeSpan MinTrainingInterval => _options.MinTrainingInterval;

    /// <summary>
    /// The currently active dictionary id, or <c>0</c> when no dictionary has
    /// been trained yet.
    /// </summary>
    public uint CurrentDictionaryId => Volatile.Read(ref _currentId);

    /// <inheritdoc />
    public uint ActiveDictionaryId => Volatile.Read(ref _currentId);

    /// <summary>
    /// The stable ids of every auto-trained dictionary this provider can
    /// currently resolve - the live retained-version ring. Never includes the
    /// reserved id <c>0</c>, is empty until the first successful training pass
    /// (and while training is disabled), and is returned as an immutable
    /// snapshot ordered ascending by id so a receiver advertises a
    /// deterministic, concurrently-enumerable capability. The snapshot is
    /// rebuilt only when the version ring changes (a roll-over or eviction),
    /// so a steady-state advertisement is allocation-free.
    /// </summary>
    public IReadOnlyCollection<uint> AvailableDictionaryIds => _availableIds;

    /// <summary>
    /// Observes a payload for possible inclusion in the training reservoir. A
    /// no-op while training is disabled. Subject to the reservoir's per-sample
    /// cap and sampling rate; the bytes are copied in when admitted, so the
    /// caller may reuse <paramref name="payload"/> after the call returns.
    /// </summary>
    /// <param name="payload">The payload bytes to sample.</param>
    /// <exception cref="ObjectDisposedException">The provider has been disposed.</exception>
    public void Observe(ReadOnlySpan<byte> payload)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_options.Enabled)
        {
            return;
        }

        _reservoir.TryObserve(payload);
    }

    /// <summary>
    /// Attempts one training pass. Returns <see langword="false"/> without
    /// training when disabled, when the cadence window has not elapsed, when the
    /// reservoir holds fewer than the configured minimum samples, when another
    /// pass is already in flight, or when the underlying builder rejects the
    /// corpus (never throwing for an untrainable corpus). On a successful pass
    /// that produces a dictionary different from the current one, publishes a
    /// new version atomically and returns <see langword="true"/>; a freshly
    /// trained dictionary byte-identical to the current one is counted as a
    /// trained pass but returns <see langword="false"/> (no redundant version
    /// bump).
    /// </summary>
    /// <returns>
    /// <see langword="true"/> when a new dictionary version was published;
    /// otherwise <see langword="false"/>.
    /// </returns>
    /// <exception cref="ObjectDisposedException">The provider has been disposed.</exception>
    public bool TryTrain()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_options.Enabled)
        {
            return false;
        }

        var lastTicks = Interlocked.Read(ref _lastTrainTicks);
        if (lastTicks != 0)
        {
            var elapsed = _time.GetUtcNow().UtcTicks - lastTicks;
            if (elapsed < _options.MinTrainingInterval.Ticks)
            {
                LatticeMetrics.CompressionDictionaryTrainingRuns.Add(1, LatticeMetrics.OutcomeSkippedCadence);
                return false;
            }
        }

        if (_reservoir.SampleCount < _options.MinSamplesToTrain)
        {
            LatticeMetrics.CompressionDictionaryTrainingRuns.Add(1, LatticeMetrics.OutcomeSkippedInsufficientSamples);
            return false;
        }

        if (Interlocked.CompareExchange(ref _trainingInFlight, 1, 0) != 0)
        {
            // Another pass is already running; treat as a cadence skip.
            LatticeMetrics.CompressionDictionaryTrainingRuns.Add(1, LatticeMetrics.OutcomeSkippedCadence);
            return false;
        }

        try
        {
            var samples = _reservoir.Snapshot();

            // Record the attempt time so the cadence gate applies to the next
            // call regardless of whether this attempt publishes.
            Interlocked.Exchange(ref _lastTrainTicks, _time.GetUtcNow().UtcTicks);

            byte[] dictionary;
            try
            {
                dictionary = DictBuilder.TrainFromBuffer(samples, _options.DictionaryCapacityBytes);
            }
            catch (Exception)
            {
                // The builder rejects a too-small / too-homogeneous corpus.
                // Treat as "no dictionary this cycle"; never propagate.
                LatticeMetrics.CompressionDictionaryTrainingRuns.Add(1, LatticeMetrics.OutcomeSkippedInsufficientSamples);
                return false;
            }

            if (dictionary is null || dictionary.Length == 0)
            {
                LatticeMetrics.CompressionDictionaryTrainingRuns.Add(1, LatticeMetrics.OutcomeSkippedInsufficientSamples);
                return false;
            }

            var hash = ComputeHash(dictionary);

            LatticeMetrics.CompressionDictionaryTrainingRuns.Add(1, LatticeMetrics.OutcomeTrained);

            // Probe the trained-vs-baseline compression ratio before publishing.
            RecordRatioProbe(samples, dictionary);

            lock (_publishGate)
            {
                if (_currentId != 0 && hash == _currentHash)
                {
                    // Byte-identical re-train: keep the current version.
                    return false;
                }

                var id = _nextId;
                var advanced = id + 1u;
                _nextId = advanced == 0u ? _options.FirstDictionaryId : advanced;

                var next = new Dictionary<uint, ReadOnlyMemory<byte>>(_versions.Count + 1);
                foreach (var kv in _versions)
                {
                    next[kv.Key] = kv.Value;
                }
                next[id] = dictionary;

                _retained.Enqueue(id);
                while (_retained.Count > _options.RetainedVersionCount)
                {
                    var evicted = _retained.Dequeue();
                    next.Remove(evicted);
                }

                _versions = next.ToFrozenDictionary();

                // Publish a fresh sorted id snapshot for capability
                // advertisement. Built once per roll-over (cold path), so the
                // allocation is bounded by the training cadence, not the ship
                // path.
                var ids = new uint[next.Count];
                next.Keys.CopyTo(ids, 0);
                Array.Sort(ids);
                _availableIds = ids;

                _currentHash = hash;
                Volatile.Write(ref _currentId, id);
                return true;
            }
        }
        finally
        {
            Interlocked.Exchange(ref _trainingInFlight, 0);
        }
    }

    /// <inheritdoc />
    public bool TryGetDictionary(uint dictionaryId, out ReadOnlyMemory<byte> dictionary)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (dictionaryId == 0u)
        {
            dictionary = ReadOnlyMemory<byte>.Empty;
            return false;
        }

        return _versions.TryGetValue(dictionaryId, out dictionary);
    }

    /// <summary>
    /// Installs dictionary bytes pulled from a peer under
    /// <paramref name="dictionaryId"/> so this provider can resolve - and
    /// therefore compress and decompress against - a dictionary the peer
    /// trained. Rejects (returns <see langword="false"/>) the reserved id
    /// <c>0</c>, empty bytes, and an id that already resolves to
    /// <em>different</em> bytes (a fingerprint collision the caller must not
    /// silently overwrite); installing byte-identical content under an
    /// already-held id is an idempotent success. Installed ids join the
    /// retained-version ring and are advertised through
    /// <see cref="AvailableDictionaryIds"/> exactly like locally-trained ids,
    /// but they never become <see cref="CurrentDictionaryId"/> - a pulled
    /// dictionary is resolvable for decode and opt-in encode, while the
    /// active id that local training rolls forward stays this provider's own.
    /// A no-op while the provider is disposed-guarded.
    /// </summary>
    /// <param name="dictionaryId">The stable id to install the bytes under.</param>
    /// <param name="dictionary">The dictionary bytes to install.</param>
    /// <returns>
    /// <see langword="true"/> when the id resolves to <paramref name="dictionary"/>
    /// after the call; otherwise <see langword="false"/>.
    /// </returns>
    /// <exception cref="ObjectDisposedException">The provider has been disposed.</exception>
    public bool TryInstall(uint dictionaryId, ReadOnlyMemory<byte> dictionary)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (dictionaryId == 0u || dictionary.IsEmpty)
        {
            return false;
        }

        lock (_publishGate)
        {
            if (_versions.TryGetValue(dictionaryId, out var existing))
            {
                // Idempotent install: same id, byte-identical content succeeds;
                // a different payload under a live id is a collision we refuse.
                return existing.Span.SequenceEqual(dictionary.Span);
            }

            // Copy the bytes so a caller-owned/rented buffer cannot mutate the
            // installed dictionary after the fact.
            var copy = dictionary.ToArray();

            var next = new Dictionary<uint, ReadOnlyMemory<byte>>(_versions.Count + 1);
            foreach (var kv in _versions)
            {
                next[kv.Key] = kv.Value;
            }
            next[dictionaryId] = copy;

            _retained.Enqueue(dictionaryId);
            while (_retained.Count > _options.RetainedVersionCount)
            {
                var evicted = _retained.Dequeue();
                next.Remove(evicted);
            }

            _versions = next.ToFrozenDictionary();

            var ids = new uint[next.Count];
            next.Keys.CopyTo(ids, 0);
            Array.Sort(ids);
            _availableIds = ids;

            return true;
        }
    }

    /// <summary>
    /// Marks the provider disposed. Subsequent calls to <see cref="Observe"/>,
    /// <see cref="TryTrain"/>, and <see cref="TryGetDictionary"/> throw
    /// <see cref="ObjectDisposedException"/>, and the observable gauges stop
    /// reporting this instance.
    /// </summary>
    public void Dispose()
    {
        _disposed = true;
    }

    private void RecordRatioProbe(byte[][] samples, byte[] dictionary)
    {
        if (samples.Length == 0)
        {
            return;
        }

        // Cold publish path only (one probe per successful training pass), so
        // the two Compressor instances and the rented scratch buffer are an
        // intentional, bounded allocation rather than a hot-path cost.
        long baselineBytes = 0;
        long trainedBytes = 0;
        using var baseline = new Compressor(ProbeCompressionLevel);
        using var trained = new Compressor(ProbeCompressionLevel);
        trained.LoadDictionary(dictionary);

        foreach (var sample in samples)
        {
            if (sample.Length == 0)
            {
                continue;
            }

            var bound = Compressor.GetCompressBound(sample.Length);
            var buffer = ArrayPool<byte>.Shared.Rent(bound);
            try
            {
                baselineBytes += baseline.Wrap(sample, buffer);
                trainedBytes += trained.Wrap(sample, buffer);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        if (baselineBytes > 0)
        {
            LatticeMetrics.CompressionDictionaryTrainedBytesIn.Add(baselineBytes);
            LatticeMetrics.CompressionDictionaryTrainedBytesOut.Add(trainedBytes);
        }
    }

    private static ulong ComputeHash(ReadOnlySpan<byte> data)
    {
        var hash = FnvOffsetBasis;
        foreach (var b in data)
        {
            hash ^= b;
            hash *= FnvPrime;
        }
        return hash;
    }

    private static void RegisterGauges()
    {
        var meter = LatticeMetrics.Meter;

        meter.CreateObservableGauge(
            LatticeMetrics.CompressionDictionaryActiveVersionName,
            static () => ObserveActiveVersion(),
            unit: "{version}",
            description: "Currently active auto-trained dictionary id (0 = none trained yet).");

        meter.CreateObservableGauge(
            LatticeMetrics.CompressionDictionaryReservoirFillName,
            static () => ObserveReservoirFill(),
            description: "Auto-training reservoir occupancy, tagged kind=samples|bytes.");
    }

    private static IEnumerable<Measurement<long>> ObserveActiveVersion()
    {
        var current = _current;
        if (current is null || current._disposed || !current._options.Enabled)
        {
            yield break;
        }

        yield return new Measurement<long>(current.CurrentDictionaryId);
    }

    private static IEnumerable<Measurement<long>> ObserveReservoirFill()
    {
        var current = _current;
        if (current is null || current._disposed || !current._options.Enabled)
        {
            yield break;
        }

        yield return new Measurement<long>(current._reservoir.SampleCount, LatticeMetrics.ReservoirFillSamplesTag);
        yield return new Measurement<long>(current._reservoir.TotalBytes, LatticeMetrics.ReservoirFillBytesTag);
    }
}
