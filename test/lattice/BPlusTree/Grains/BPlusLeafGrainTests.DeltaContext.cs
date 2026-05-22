using System.Text;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="LatticeDeltaContext"/> propagation through the
/// single-key write paths of <see cref="BPlusLeafGrain"/>. Confirms that
/// <see cref="LatticeMutation.Delta"/> is stamped from the ambient
/// context at commit time and defaults to <see langword="null"/> when no
/// producer wrapped the call in a delta scope.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [SetUp]
    public void ResetDeltaContext()
    {
        // Defensive: clear ambient context that may leak from another fixture
        // running on the same logical thread.
        LatticeDeltaContext.Current = null;
    }

    [Test]
    public async Task SetAsync_stamps_Delta_from_ambient_context()
    {
        var observer = new RecordingMutationObserver();
        var grain = CreateGrainWithObserver(observer);

        var payload = new byte[] { 0xAA, 0xBB, 0xCC };
        using (LatticeDeltaContext.With(payload))
        {
            await grain.SetAsync("k", Encoding.UTF8.GetBytes("v"));
        }

        Assert.That(observer.Mutations, Has.Count.EqualTo(1));
        var m = observer.Mutations[0];
        Assert.That(m.Delta, Is.EqualTo(payload));
    }

    [Test]
    public async Task SetAsync_emits_null_delta_when_context_unset()
    {
        var observer = new RecordingMutationObserver();
        var grain = CreateGrainWithObserver(observer);

        await grain.SetAsync("k", Encoding.UTF8.GetBytes("v"));

        Assert.That(observer.Mutations, Has.Count.EqualTo(1));
        Assert.That(observer.Mutations[0].Delta, Is.Null);
    }

    [Test]
    public async Task DeleteAsync_stamps_Delta_from_ambient_context()
    {
        var observer = new RecordingMutationObserver();
        var grain = CreateGrainWithObserver(observer);

        await grain.SetAsync("k", Encoding.UTF8.GetBytes("v"));

        var payload = new byte[] { 0x01, 0x02 };
        using (LatticeDeltaContext.With(payload))
        {
            await grain.DeleteAsync("k");
        }

        Assert.That(observer.Mutations, Has.Count.GreaterThanOrEqualTo(2));
        var deleteMutation = observer.Mutations[^1];
        Assert.That(deleteMutation.Kind, Is.EqualTo(MutationKind.Delete));
        Assert.That(deleteMutation.Delta, Is.EqualTo(payload));
    }

    [Test]
    public async Task DeleteAsync_emits_null_delta_when_context_unset()
    {
        var observer = new RecordingMutationObserver();
        var grain = CreateGrainWithObserver(observer);

        await grain.SetAsync("k", Encoding.UTF8.GetBytes("v"));
        await grain.DeleteAsync("k");

        var deleteMutation = observer.Mutations[^1];
        Assert.That(deleteMutation.Kind, Is.EqualTo(MutationKind.Delete));
        Assert.That(deleteMutation.Delta, Is.Null);
    }

    [Test]
    public async Task SetAsync_after_scope_disposes_emits_null_delta()
    {
        var observer = new RecordingMutationObserver();
        var grain = CreateGrainWithObserver(observer);

        using (LatticeDeltaContext.With(new byte[] { 9 }))
        {
            // no-op; just exercising scope shape
        }

        await grain.SetAsync("k", Encoding.UTF8.GetBytes("v"));
        Assert.That(observer.Mutations[0].Delta, Is.Null);
    }
}