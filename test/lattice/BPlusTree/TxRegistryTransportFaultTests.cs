using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Pins the registry transport-failure classifier (issues #2215 / #3641): only a
/// transport failure is translated into an "outcome unavailable" signal; a
/// cancellation, a domain refusal, or a genuine fault propagates unchanged.
/// </summary>
[TestFixture]
public class TxRegistryTransportFaultTests
{
    [Test]
    public void IsTransportFailure_accepts_a_response_timeout() =>
        Assert.That(TxRegistryTransportFault.IsTransportFailure(new TimeoutException()), Is.True);

    [Test]
    public void IsTransportFailure_accepts_an_unavailable_silo() =>
        Assert.That(TxRegistryTransportFault.IsTransportFailure(new SiloUnavailableException()), Is.True);

    [Test]
    public void IsTransportFailure_accepts_an_orleans_exception() =>
        Assert.That(TxRegistryTransportFault.IsTransportFailure(new OrleansException("rejected")), Is.True);

    [Test]
    public void IsTransportFailure_rejects_cancellation()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TxRegistryTransportFault.IsTransportFailure(new OperationCanceledException()), Is.False);
            Assert.That(TxRegistryTransportFault.IsTransportFailure(new TaskCanceledException()), Is.False);
        });
    }

    [Test]
    public void IsTransportFailure_rejects_a_lattice_domain_fault_deriving_from_TimeoutException()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TxRegistryTransportFault.IsTransportFailure(
                new LatticeTransactionOutcomeUnavailableException("already translated")), Is.False);
            Assert.That(TxRegistryTransportFault.IsTransportFailure(
                new ShardActivationTimeoutException("seed")), Is.False);
        });
    }

    [Test]
    public void IsTransportFailure_rejects_a_genuine_fault()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TxRegistryTransportFault.IsTransportFailure(new InvalidOperationException()), Is.False);
            Assert.That(TxRegistryTransportFault.IsTransportFailure(new ArgumentException()), Is.False);
        });
    }
}
