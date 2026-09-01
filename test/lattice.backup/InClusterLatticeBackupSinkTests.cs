using System.Runtime.CompilerServices;
using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit tests for <see cref="InClusterLatticeBackupSink"/> that do not require a
/// live silo. Exercises: the <c>ThrowIfSeparator</c> validation path (lines
/// 274-276), which is triggered by passing an artifact id that contains the
/// reserved unit-separator character to
/// <see cref="ILatticeBackupSink.WriteArtifactAsync"/>; and the two
/// <c>ArtifactIdFromChunkKey</c> null-return paths (lines 258 and 264), which are
/// triggered by <see cref="ILatticeBackupSink.ListArtifactIdsAsync"/> when the
/// in-cluster store returns keys that lack the expected separator structure.
/// </summary>
[TestFixture]
public sealed class InClusterLatticeBackupSinkTests
{
    private static InClusterLatticeBackupSink CreateSink(ILattice? lattice = null)
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        if (lattice is not null)
        {
            grainFactory
                .GetGrain<ILattice>(Arg.Any<string>(), Arg.Any<string?>())
                .Returns(lattice);
        }

        return new InClusterLatticeBackupSink(grainFactory);
    }

    // ---- ThrowIfSeparator (lines 274-276) -----------------------------------

    [Test]
    public async Task WriteArtifactAsync_throws_when_artifact_id_contains_separator()
    {
        // Lines 274-276: ThrowIfSeparator fires before touching the grain store.
        var sink = CreateSink();
        var artifactIdWithSep = "artifact\u001fbad";

        await Assert.ThatAsync(
            async () => await sink.WriteArtifactAsync(artifactIdWithSep, AsyncEmpty()),
            Throws.ArgumentException.With.Message.Contains("unit-separator"));
    }

    // ---- ArtifactIdFromChunkKey null paths (lines 258, 264) -----------------

    [Test]
    public async Task ListArtifactIdsAsync_skips_keys_without_any_separator()
    {
        // Line 258: ArtifactIdFromChunkKey returns null when the key has no
        // separator at all; the null is filtered out in ListArtifactIdsAsync.
        var lattice = Substitute.For<ILattice>();
        lattice
            .KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(),
                Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(AsyncKeys("no-separator-here"));

        var sink = CreateSink(lattice);
        var results = new List<string>();
        await foreach (var id in sink.ListArtifactIdsAsync())
        {
            results.Add(id);
        }

        Assert.That(results, Is.Empty, "A key with no separator must produce no artifact id.");
    }

    [Test]
    public async Task ListArtifactIdsAsync_skips_keys_with_only_one_separator()
    {
        // Line 264: ArtifactIdFromChunkKey returns null when the key has only one
        // separator (no closing separator after the artifact id segment).
        var lattice = Substitute.For<ILattice>();
        lattice
            .KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(),
                Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(AsyncKeys("prefix\u001fonly-one-sep"));

        var sink = CreateSink(lattice);
        var results = new List<string>();
        await foreach (var id in sink.ListArtifactIdsAsync())
        {
            results.Add(id);
        }

        Assert.That(results, Is.Empty, "A key with only one separator must produce no artifact id.");
    }

    private static async IAsyncEnumerable<ReadOnlyMemory<byte>> AsyncEmpty(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await Task.CompletedTask;
        yield break;
    }

    private static async IAsyncEnumerable<string> AsyncKeys(
        params string[] keys)
    {
        foreach (var key in keys)
        {
            yield return key;
            await Task.Yield();
        }
    }
}
