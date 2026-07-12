namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="NullLatticeWriteInterceptor"/>: the core
/// always-accept default. It must accept every write with a synchronously-
/// completed, allocation-free accept decision so an unregistered interceptor is a
/// predictable no-op.
/// </summary>
[TestFixture]
public class NullLatticeWriteInterceptorTests
{
    private static LatticeWriteRequest SampleRequest() =>
        new("orders", "k1", [1, 2, 3], LatticeOperation.Write);

    [Test]
    public void InterceptsSystemOrigin_is_false()
    {
        ILatticeWriteInterceptor interceptor = new NullLatticeWriteInterceptor();

        Assert.That(interceptor.InterceptsSystemOrigin, Is.False);
    }

    [Test]
    public async Task OnWriteAsync_accepts_every_request()
    {
        ILatticeWriteInterceptor interceptor = new NullLatticeWriteInterceptor();
        var request = SampleRequest();

        var decision = await interceptor.OnWriteAsync(in request);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.Accept));
            Assert.That(decision.TransformedValue, Is.Null);
            Assert.That(decision.Reason, Is.Null);
        });
    }

    [Test]
    public void OnWriteAsync_completes_synchronously()
    {
        ILatticeWriteInterceptor interceptor = new NullLatticeWriteInterceptor();
        var request = SampleRequest();

        var pending = interceptor.OnWriteAsync(in request);

        Assert.That(pending.IsCompletedSuccessfully, Is.True);
        Assert.That(pending.Result.Kind, Is.EqualTo(LatticeWriteDecisionKind.Accept));
    }

    [Test]
    public void OnWriteAsync_allocates_nothing_on_the_hot_path()
    {
        ILatticeWriteInterceptor interceptor = new NullLatticeWriteInterceptor();
        var request = SampleRequest();

        // Warm up the JIT so first-call codegen does not count against the loop.
        _ = interceptor.OnWriteAsync(in request).IsCompletedSuccessfully;

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1_000; i++)
        {
            var pending = interceptor.OnWriteAsync(in request);
            _ = pending.Result.Kind;
        }
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.That(allocated, Is.EqualTo(0), "The null interceptor must not allocate per call.");
    }
}
