using NSubstitute;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// Substituted vector-metadata and vector-payload trees behind a substituted
/// <see cref="IGrainFactory"/>, holding real serialized records in ordinal key
/// order.
/// <para>
/// It reproduces the four tree operations the retrieval surface actually uses -
/// a bounded range entry scan, a bounded key-only walk, a batched multi-get, and
/// a presence probe - so the real store-of-record reader and the real exact scan
/// can be exercised in the unit lane, against the same key grammar and the same
/// ascending ordinal order a silo would give them.
/// </para>
/// </summary>
internal sealed class SubstitutedVectorTrees
{
    private readonly SortedDictionary<string, byte[]> _metadata = new(StringComparer.Ordinal);
    private readonly SortedDictionary<string, byte[]> _payloads = new(StringComparer.Ordinal);
    private readonly Serializer _serializer;

    /// <summary>Creates the substituted trees.</summary>
    /// <param name="serializer">The Orleans serializer records are written with. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="serializer"/> is null.</exception>
    public SubstitutedVectorTrees(Serializer serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        _serializer = serializer;

        MetadataTree = Build(_metadata);
        PayloadTree = Build(_payloads);

        GrainFactory = Substitute.For<IGrainFactory>();
        GrainFactory.GetGrain<ILattice>(Arg.Any<string>()).Returns(call =>
            call.ArgAt<string>(0) switch
            {
                RepoContextTrees.VectorPayload => PayloadTree,
                _ => MetadataTree,
            });
    }

    /// <summary>The substituted grain factory the surface resolves trees through.</summary>
    public IGrainFactory GrainFactory { get; }

    /// <summary>The substituted vector-metadata tree.</summary>
    public ILattice MetadataTree { get; }

    /// <summary>The substituted vector-payload tree.</summary>
    public ILattice PayloadTree { get; }

    /// <summary>
    /// Writes one vector exactly as <see cref="RepoContextVectorWriter"/> lays it
    /// out: a content-addressed payload record and a metadata presence record
    /// carrying the source key and the embedding space.
    /// </summary>
    /// <param name="repoId">The repository.</param>
    /// <param name="vectorId">The vector identifier.</param>
    /// <param name="sourceKey">The canonical source key.</param>
    /// <param name="space">The embedding space the vector was written under.</param>
    /// <param name="vector">The vector components.</param>
    public void Write(
        string repoId, string vectorId, string sourceKey, EmbeddingSpaceTag space, float[] vector)
    {
        var payload = VectorCodec.Encode(vector);
        var contentAddress = VectorCodec.ContentAddress(payload);
        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);

        _payloads[RepoContextKeys.VectorPayload(repoId, contentAddress)] = _serializer.SerializeToArray(
            VectorPayloadRecord.Create(repoId, contentAddress, space, payload));

        _metadata[RepoContextKeys.Vector(repoId, vectorId)] = _serializer.SerializeToArray(
            new VectorMetadataRecord
            {
                RepoId = repoId,
                VectorId = vectorId,
                Space = space,
                SourceKey = RepoContextValues.Lww(sourceKey, clock),
                ContentAddress = RepoContextValues.Lww(contentAddress, clock),
                CreatedAt = RepoContextValues.Lww(DateTime.UtcNow.Ticks, clock),
            });
    }

    /// <summary>Removes one vector's metadata presence record, as a retirement does.</summary>
    /// <param name="repoId">The repository.</param>
    /// <param name="vectorId">The vector identifier.</param>
    public void RetireMetadata(string repoId, string vectorId)
        => _metadata.Remove(RepoContextKeys.Vector(repoId, vectorId));

    /// <summary>Removes one payload record, so a metadata record is left with nothing to hydrate.</summary>
    /// <param name="repoId">The repository.</param>
    /// <param name="vector">The vector whose content-addressed payload to drop.</param>
    public void DropPayload(string repoId, float[] vector)
        => _payloads.Remove(
            RepoContextKeys.VectorPayload(repoId, VectorCodec.ContentAddress(VectorCodec.Encode(vector))));

    /// <summary>The metadata keys currently stored, in ascending ordinal order.</summary>
    public IReadOnlyList<string> MetadataKeys => [.. _metadata.Keys];

    private static ILattice Build(SortedDictionary<string, byte[]> records)
    {
        var tree = Substitute.For<ILattice>();

        tree.EntriesAsync().ReturnsForAnyArgs(call => Entries(
            records, call.ArgAt<string?>(0), call.ArgAt<string?>(1)));

        tree.KeysAsync().ReturnsForAnyArgs(call => Keys(
            records, call.ArgAt<string?>(0), call.ArgAt<string?>(1)));

        tree.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ReturnsForAnyArgs(call =>
                Task.FromResult(records.TryGetValue(call.ArgAt<string>(0), out var value) ? value : null));

        tree.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .ReturnsForAnyArgs(call =>
            {
                records[call.ArgAt<string>(0)] = call.ArgAt<byte[]>(1);
                return Task.CompletedTask;
            });

        tree.DeleteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ReturnsForAnyArgs(call => Task.FromResult(records.Remove(call.ArgAt<string>(0))));

        tree.DeleteRangeAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ReturnsForAnyArgs(call =>
            {
                var removed = 0;
                foreach (var entry in Window(records, call.ArgAt<string>(0), call.ArgAt<string>(1)))
                {
                    if (records.Remove(entry.Key))
                    {
                        removed++;
                    }
                }

                return Task.FromResult(removed);
            });

        tree.GetManyAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>())
            .ReturnsForAnyArgs(call =>
            {
                var found = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                foreach (var key in call.ArgAt<List<string>>(0))
                {
                    if (records.TryGetValue(key, out var value))
                    {
                        found[key] = value;
                    }
                }

                return Task.FromResult(found);
            });

        tree.ExistsAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ReturnsForAnyArgs(call => Task.FromResult(records.ContainsKey(call.ArgAt<string>(0))));

        return tree;
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> Entries(
        SortedDictionary<string, byte[]> records, string? startInclusive, string? endExclusive)
    {
        foreach (var entry in Window(records, startInclusive, endExclusive))
        {
            yield return entry;
            await Task.CompletedTask.ConfigureAwait(false);
        }
    }

    private static async IAsyncEnumerable<string> Keys(
        SortedDictionary<string, byte[]> records, string? startInclusive, string? endExclusive)
    {
        foreach (var entry in Window(records, startInclusive, endExclusive))
        {
            yield return entry.Key;
            await Task.CompletedTask.ConfigureAwait(false);
        }
    }

    private static List<KeyValuePair<string, byte[]>> Window(
        SortedDictionary<string, byte[]> records, string? startInclusive, string? endExclusive)
    {
        var window = new List<KeyValuePair<string, byte[]>>();
        foreach (var entry in records)
        {
            if (startInclusive is not null && string.CompareOrdinal(entry.Key, startInclusive) < 0)
            {
                continue;
            }

            if (endExclusive is not null && string.CompareOrdinal(entry.Key, endExclusive) >= 0)
            {
                break;
            }

            window.Add(entry);
        }

        return window;
    }
}
