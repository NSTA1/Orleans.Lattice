namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="NullLatticeAccessGate"/>: the core allow-all
/// default. It must authorize every request with a synchronously-completed,
/// allocation-free allow decision so an unregistered gate is a predictable
/// no-op.
/// </summary>
[TestFixture]
public class NullLatticeAccessGateTests
{
    private static LatticeAccessRequest SampleRequest() =>
        new("orders", LatticeOperation.Write, LatticeSubject.Anonymous, key: "k1");

    [Test]
    public async Task AuthorizeAsync_allows_every_request()
    {
        ILatticeAccessGate gate = new NullLatticeAccessGate();
        var request = SampleRequest();

        var decision = await gate.AuthorizeAsync(in request);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.True);
            Assert.That(decision.Reason, Is.Null);
            Assert.That(decision.KeyFilter, Is.Null);
        });
    }

    [Test]
    public void AuthorizeAsync_completes_synchronously()
    {
        ILatticeAccessGate gate = new NullLatticeAccessGate();
        var request = SampleRequest();

        var pending = gate.AuthorizeAsync(in request);

        Assert.That(pending.IsCompletedSuccessfully, Is.True);
        Assert.That(pending.Result.Allowed, Is.True);
    }

    [Test]
    public void AuthorizeAsync_allocates_nothing_on_the_hot_path()
    {
        ILatticeAccessGate gate = new NullLatticeAccessGate();
        var request = SampleRequest();

        // Warm up the JIT so first-call codegen does not count against the loop.
        _ = gate.AuthorizeAsync(in request).IsCompletedSuccessfully;

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1_000; i++)
        {
            var pending = gate.AuthorizeAsync(in request);
            _ = pending.Result.Allowed;
        }
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.That(allocated, Is.EqualTo(0), "The null gate must not allocate per call.");
    }
}
