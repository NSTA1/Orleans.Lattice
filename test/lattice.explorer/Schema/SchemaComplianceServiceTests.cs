using Grpc.Core;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.Schema;

[TestFixture]
public class SchemaComplianceServiceTests
{
    private static SchemaComplianceService Create(FakeSchemaAdminClient client) => new(client);

    private static LatticeSchemaComplianceReport Report() =>
        new()
        {
            TreeId = "t",
            HasPolicy = true,
            CompliantCount = 8,
            NonCompliantCount = 2,
            ScannedCount = 10,
            RuleBreakdown = new[] { new LatticeSchemaComplianceRuleCount { Reason = "not-utf8", Count = 2 } },
        };

    private static LatticeSchemaDeadLetterEntry Entry(string key) =>
        new(
            key,
            new byte[] { 1, 2, 3 },
            3,
            "not-utf8",
            LatticeSchemaDeadLetterSource.LocalRejected,
            DateTimeOffset.UnixEpoch);

    [Test]
    public void Constructor_null_client_throws()
    {
        Assert.That(() => new SchemaComplianceService(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task ScanComplianceAsync_success_returns_report()
    {
        var client = new FakeSchemaAdminClient { ComplianceResult = Report() };
        var service = Create(client);

        var view = await service.ScanComplianceAsync("t");

        Assert.Multiple(() =>
        {
            Assert.That(view.IsSuccess, Is.True);
            Assert.That(view.Value.CompliantCount, Is.EqualTo(8));
            Assert.That(view.Value.NonCompliantCount, Is.EqualTo(2));
            Assert.That(view.Value.RuleBreakdown, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task ScanComplianceAsync_denied_returns_denied_view()
    {
        var client = new FakeSchemaAdminClient { ReadThrows = new LatticeAuthorizationDeniedException("denied") };
        var service = Create(client);

        var view = await service.ScanComplianceAsync("t");

        Assert.That(view.Status, Is.EqualTo(SchemaOperationStatus.Denied));
    }

    [Test]
    public async Task ScanComplianceAsync_transport_failure_returns_failed_view()
    {
        var client = new FakeSchemaAdminClient { ReadThrows = new RpcException(new Status(StatusCode.Unavailable, "gone")) };
        var service = Create(client);

        var view = await service.ScanComplianceAsync("t");

        Assert.That(view.Status, Is.EqualTo(SchemaOperationStatus.Failed));
    }

    [Test]
    public void ScanComplianceAsync_empty_tree_throws()
    {
        var service = Create(new FakeSchemaAdminClient());

        Assert.That(() => service.ScanComplianceAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task ListDeadLettersAsync_success_returns_count_and_entries()
    {
        var client = new FakeSchemaAdminClient
        {
            DeadLetterCountResult = 5,
            DeadLettersResult = new[] { Entry("k1"), Entry("k2") },
        };
        var service = Create(client);

        var view = await service.ListDeadLettersAsync("t", 10);

        Assert.Multiple(() =>
        {
            Assert.That(view.IsSuccess, Is.True);
            Assert.That(view.Count, Is.EqualTo(5));
            Assert.That(view.Entries, Has.Count.EqualTo(2));
            Assert.That(client.LastMaxEntries, Is.EqualTo(10));
        });
    }

    [Test]
    public async Task ListDeadLettersAsync_denied_returns_denied_view()
    {
        var client = new FakeSchemaAdminClient { ReadThrows = new LatticeAuthorizationDeniedException("denied") };
        var service = Create(client);

        var view = await service.ListDeadLettersAsync("t", 10);

        Assert.Multiple(() =>
        {
            Assert.That(view.Status, Is.EqualTo(SchemaOperationStatus.Denied));
            Assert.That(view.Entries, Is.Empty);
        });
    }

    [Test]
    public async Task ListDeadLettersAsync_transport_failure_returns_failed_view()
    {
        var client = new FakeSchemaAdminClient { ReadThrows = new RpcException(new Status(StatusCode.Unavailable, "gone")) };
        var service = Create(client);

        var view = await service.ListDeadLettersAsync("t", 10);

        Assert.That(view.Status, Is.EqualTo(SchemaOperationStatus.Failed));
    }

    [Test]
    public void ListDeadLettersAsync_empty_tree_throws()
    {
        var service = Create(new FakeSchemaAdminClient());

        Assert.That(() => service.ListDeadLettersAsync(string.Empty, 10), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void ListDeadLettersAsync_non_positive_max_throws()
    {
        var service = Create(new FakeSchemaAdminClient());

        Assert.That(() => service.ListDeadLettersAsync("t", 0), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }
}
