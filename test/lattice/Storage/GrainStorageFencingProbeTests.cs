using NSubstitute;
using NUnit.Framework;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.Storage;

/// <summary>
/// Unit tests for <see cref="GrainStorageFencingProbe"/> (issue #4200): the probe
/// that tells a grain storage provider that enforces ETags from one that does not.
/// </summary>
[TestFixture]
public sealed class GrainStorageFencingProbeTests
{
    [Test]
    public async Task RunAsync_non_fencing_provider_is_unfenced()
    {
        var storage = new NonFencingGrainStorage();

        var result = await GrainStorageFencingProbe.RunAsync(storage);

        Assert.That(result.Verdict, Is.EqualTo(GrainStorageFencingVerdict.Unfenced));
        Assert.That(result.Fault, Is.Null);
        Assert.That(storage.Writes, Is.EqualTo(3), "two writes, then the stale-ETag write");
    }

    [Test]
    public async Task RunAsync_fencing_provider_is_fenced()
    {
        var storage = new FencingGrainStorage();

        var result = await GrainStorageFencingProbe.RunAsync(storage);

        Assert.That(result.Verdict, Is.EqualTo(GrainStorageFencingVerdict.Fenced));
        Assert.That(storage.Writes, Is.EqualTo(3));
    }

    [Test]
    public async Task RunAsync_fencing_provider_is_fenced_again_on_a_later_run()
    {
        var storage = new FencingGrainStorage();
        await GrainStorageFencingProbe.RunAsync(storage);

        var result = await GrainStorageFencingProbe.RunAsync(storage);

        Assert.That(result.Verdict, Is.EqualTo(GrainStorageFencingVerdict.Fenced), "an existing probe row is re-read, not blindly inserted");
    }

    [Test]
    public async Task RunAsync_lost_race_is_retried_from_a_fresh_read()
    {
        var storage = new FencingGrainStorage();
        storage.BeforeWrite = ordinal => ordinal == 1
            ? throw new InconsistentStateException("concurrent probe", "a", "b")
            : Task.CompletedTask;

        var result = await GrainStorageFencingProbe.RunAsync(storage);

        Assert.That(result.Verdict, Is.EqualTo(GrainStorageFencingVerdict.Fenced));
    }

    [Test]
    public async Task RunAsync_every_attempt_losing_a_race_is_inconclusive()
    {
        var storage = new FencingGrainStorage
        {
            BeforeWrite = _ => throw new InconsistentStateException("concurrent probe", "a", "b"),
        };

        var result = await GrainStorageFencingProbe.RunAsync(storage);

        Assert.That(result.Verdict, Is.EqualTo(GrainStorageFencingVerdict.Inconclusive));
        Assert.That(storage.Writes, Is.EqualTo(GrainStorageFencingProbe.MaxAttempts));
    }

    [Test]
    public async Task RunAsync_fault_before_the_stale_write_is_inconclusive()
    {
        var fault = new IOException("storage unreachable");
        var storage = new FencingGrainStorage { BeforeWrite = _ => throw fault };

        var result = await GrainStorageFencingProbe.RunAsync(storage);

        Assert.That(result.Verdict, Is.EqualTo(GrainStorageFencingVerdict.Inconclusive));
        Assert.That(result.Fault, Is.SameAs(fault));
    }

    [Test]
    public async Task RunAsync_other_exception_on_the_stale_write_is_inconclusive()
    {
        var fault = new IOException("412 surfaced as a provider-specific exception");
        var storage = new FencingGrainStorage
        {
            BeforeWrite = ordinal => ordinal == 3 ? throw fault : Task.CompletedTask,
        };

        var result = await GrainStorageFencingProbe.RunAsync(storage);

        Assert.That(result.Verdict, Is.EqualTo(GrainStorageFencingVerdict.Inconclusive));
        Assert.That(result.Fault, Is.SameAs(fault));
    }

    [Test]
    public async Task RunAsync_writes_under_the_reserved_grain_type_and_state_name()
    {
        var storage = Substitute.For<IGrainStorage>();

        await GrainStorageFencingProbe.RunAsync(storage);

        await storage.Received().WriteStateAsync(
            GrainStorageFencingProbe.StateName,
            GrainStorageFencingProbe.ProbeGrainId,
            Arg.Any<IGrainState<GrainStorageFencingProbeState>>());
        Assert.That(GrainStorageFencingProbe.ProbeGrainId.Type.ToString(), Does.StartWith("_lattice_"));
        Assert.That(GrainStorageFencingProbe.StateName, Does.StartWith("_lattice_"));
    }

    [Test]
    public void RunAsync_null_storage_throws()
    {
        Assert.ThrowsAsync<ArgumentNullException>(() => GrainStorageFencingProbe.RunAsync(null!));
    }
}
