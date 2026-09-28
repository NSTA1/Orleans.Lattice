using Grpc.Core;
using Orleans.Lattice.Explorer.Shell.Transport;

namespace Orleans.Lattice.Explorer.Tests.Shell.Transport;

/// <summary>
/// The shared fault table: every gRPC status maps to one facade-shaped exception,
/// carrying the server's sanitised detail and the transport exception as its
/// inner exception.
/// </summary>
[TestFixture]
public sealed class ShellTransportFaultsTests
{
    private static readonly (StatusCode Status, Type Expected)[] Table =
    [
        (StatusCode.Cancelled, typeof(OperationCanceledException)),
        (StatusCode.PermissionDenied, typeof(LatticeAuthorizationDeniedException)),
        (StatusCode.Unauthenticated, typeof(LatticeAuthorizationDeniedException)),
        (StatusCode.InvalidArgument, typeof(ArgumentException)),
        (StatusCode.OutOfRange, typeof(ArgumentOutOfRangeException)),
        (StatusCode.NotFound, typeof(KeyNotFoundException)),
        (StatusCode.AlreadyExists, typeof(InvalidOperationException)),
        (StatusCode.FailedPrecondition, typeof(InvalidOperationException)),
        (StatusCode.ResourceExhausted, typeof(InvalidOperationException)),
        (StatusCode.Unimplemented, typeof(NotSupportedException)),
        (StatusCode.Unavailable, typeof(ShellTransportException)),
        (StatusCode.DeadlineExceeded, typeof(ShellTransportException)),
        (StatusCode.Aborted, typeof(ShellTransportException)),
        (StatusCode.Internal, typeof(ShellTransportException)),
        (StatusCode.Unknown, typeof(ShellTransportException)),
        (StatusCode.DataLoss, typeof(ShellTransportException)),
    ];

    private static IEnumerable<TestCaseData> Cases() =>
        Table.Select(row => new TestCaseData(row.Status, row.Expected).SetName($"{row.Status}_maps_to_{row.Expected.Name}"));

    [TestCaseSource(nameof(Cases))]
    public void Each_status_maps_to_its_facade_exception(StatusCode status, Type expected)
    {
        var fault = new RpcException(new Status(status, "the reason"));

        var mapped = ShellTransportFaults.Map(fault, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(mapped.GetType(), Is.EqualTo(expected));
            Assert.That(mapped.Message, Is.EqualTo("the reason"));
            Assert.That(mapped.InnerException, Is.SameAs(fault));
        });
    }

    [TestCase(StatusCode.Unavailable, true)]
    [TestCase(StatusCode.DeadlineExceeded, true)]
    [TestCase(StatusCode.Aborted, true)]
    [TestCase(StatusCode.Internal, false)]
    [TestCase(StatusCode.Unknown, false)]
    public void Only_a_retryable_status_is_transient(StatusCode status, bool transient)
    {
        var mapped = (ShellTransportException)ShellTransportFaults.Map(new RpcException(new Status(status, "x")), CancellationToken.None);

        Assert.That(mapped.IsTransient, Is.EqualTo(transient));
    }

    [Test]
    public void A_cancellation_carries_the_callers_token()
    {
        using var source = new CancellationTokenSource();
        source.Cancel();

        var mapped = (OperationCanceledException)ShellTransportFaults.Map(
            new RpcException(new Status(StatusCode.Cancelled, string.Empty)), source.Token);

        Assert.That(mapped.CancellationToken, Is.EqualTo(source.Token));
    }

    [Test]
    public void A_status_with_no_detail_gets_a_fixed_sentence_for_every_status()
    {
        foreach (var status in Enum.GetValues<StatusCode>())
        {
            var detail = ShellTransportFaults.Detail(new RpcException(new Status(status, "  ")));

            Assert.That(detail, Is.Not.Empty.And.Not.EqualTo("  "), status.ToString());
        }
    }

    [Test]
    public void The_server_detail_is_carried_verbatim()
    {
        Assert.That(
            ShellTransportFaults.Detail(new RpcException(new Status(StatusCode.NotFound, "Tree 'orders' is not registered."))),
            Is.EqualTo("Tree 'orders' is not registered."));
    }

    [Test]
    public void A_missing_fault_is_rejected()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => ShellTransportFaults.Map(null!, CancellationToken.None), Throws.ArgumentNullException);
            Assert.That(() => ShellTransportFaults.Detail(null!), Throws.ArgumentNullException);
            Assert.That(() => ShellTenantFaults.Map(null!, "t", CancellationToken.None), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void The_tenancy_refinement_rebuilds_not_found_and_already_exists_and_defers_the_rest()
    {
        Assert.Multiple(() =>
        {
            var notFound = ShellTenantFaults.Map(new RpcException(new Status(StatusCode.NotFound, "gone")), "contoso", default);
            Assert.That(notFound, Is.InstanceOf<Orleans.Lattice.Api.TenantAdmin.TenantNotFoundException>());
            Assert.That(((Orleans.Lattice.Api.TenantAdmin.TenantNotFoundException)notFound).TenantId, Is.EqualTo("contoso"));
            Assert.That(notFound.Message, Is.EqualTo("gone"));

            var exists = ShellTenantFaults.Map(new RpcException(new Status(StatusCode.AlreadyExists, "dup")), null, default);
            Assert.That(exists, Is.InstanceOf<Orleans.Lattice.Api.TenantAdmin.TenantAlreadyExistsException>());
            Assert.That(((Orleans.Lattice.Api.TenantAdmin.TenantAlreadyExistsException)exists).TenantId, Is.Empty);

            Assert.That(
                ShellTenantFaults.Map(new RpcException(new Status(StatusCode.PermissionDenied, "no")), "contoso", default),
                Is.InstanceOf<LatticeAuthorizationDeniedException>());
        });
    }
}
