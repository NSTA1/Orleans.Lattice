using Grpc.Core;
using NSubstitute;
using Orleans.Runtime;

namespace Orleans.Lattice.Api.Data.Grpc.Tests;

/// <summary>Transient cluster faults on the data facade (#5742).</summary>
public sealed partial class LatticeDataApiGrpcServiceUnitTests
{
    [Test]
    public void ReadRange_maps_a_grain_call_timeout_to_deadline_exceeded()
    {
        _api.ReadRangeAsync(Arg.Any<DataRangeRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<DataRangePage>(new TimeoutException("Response did not arrive on time for secret-grain")));

        var ex = Assert.ThrowsAsync<RpcException>(
            () => _service.ReadRange(new DataRangeRequest { TreeId = "t" }, Context()));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.DeadlineExceeded));
            Assert.That(ex.Status.Detail, Does.Not.Contain("secret-grain"));
        });
    }

    [Test]
    public void ReadRange_maps_an_unavailable_silo_to_unavailable()
    {
        _api.ReadRangeAsync(Arg.Any<DataRangeRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<DataRangePage>(new SiloUnavailableException("secret-silo is dead")));

        var ex = Assert.ThrowsAsync<RpcException>(
            () => _service.ReadRange(new DataRangeRequest { TreeId = "t" }, Context()));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Unavailable));
            Assert.That(ex.Status.Detail, Does.Not.Contain("secret-silo"));
        });
    }

    [Test]
    public void ReadRange_maps_an_aborted_remote_enumeration_to_unavailable()
    {
        // The cursor grain's remote enumerator was lost to a deactivation mid-read: a
        // transient fault a client clears by reading again, not a server bug.
        _api.ReadRangeAsync(Arg.Any<DataRangeRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<DataRangePage>(new EnumerationAbortedException("secret-enumerator was lost")));

        var ex = Assert.ThrowsAsync<RpcException>(
            () => _service.ReadRange(new DataRangeRequest { TreeId = "t" }, Context()));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Unavailable));
            Assert.That(ex.Status.Detail, Does.Not.Contain("secret-enumerator"));
        });
    }
}
