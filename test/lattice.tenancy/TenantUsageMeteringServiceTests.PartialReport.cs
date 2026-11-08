namespace Orleans.Lattice.Tenancy.Tests;

public sealed partial class TenantUsageMeteringServiceTests
{
    [Test]
    public async Task A_partial_report_retains_the_trees_last_known_footprint()
    {
        // Issue #6017: a partial report is a lower bound - a WAL partition whose
        // scan was refused contributed nothing to it - so publishing it would
        // shrink the tenant's accounted usage exactly as an unreadable tree would.
        var harness = new MutableUsageHarness();
        harness.AddTree("t/acme/orders", bytes: 4096, keys: 32);
        var store = new RecordingStore();
        var service = Create(new FakeRegistry(Acme), store, harness.Factory);

        await service.MeterOnceAsync(CancellationToken.None);
        harness.Report("t/acme/orders", new TreeStorageUsageReport
        {
            TreeId = "t/acme/orders",
            TotalBytes = 1024,
            LiveKeys = 32,
            LeafStateBytes = 1024,
            Partial = true,
        });
        await service.MeterOnceAsync(CancellationToken.None);

        var latest = store.Published[^1].LocalSample("cluster-a");
        Assert.Multiple(() =>
        {
            Assert.That(latest.Bytes, Is.EqualTo(4096),
                "a partial report must not shrink the tenant's accounted footprint");
            Assert.That(latest.Keys, Is.EqualTo(32));
        });
    }

    [Test]
    public async Task Successive_partial_reports_with_rising_usage_raise_the_published_sample()
    {
        // Issue #6025 B1: a tree whose every report is partial must still be
        // metered from its rising partial figures, not frozen at its first one.
        var harness = new MutableUsageHarness();
        harness.AddTree("t/acme/orders", bytes: 0, keys: 0);
        harness.Report("t/acme/orders", PartialReport(bytes: 1000, keys: 10));
        var store = new RecordingStore();
        var service = Create(new FakeRegistry(Acme), store, harness.Factory);

        await service.MeterOnceAsync(CancellationToken.None);
        harness.Report("t/acme/orders", PartialReport(bytes: 2000, keys: 20));
        await service.MeterOnceAsync(CancellationToken.None);

        var latest = store.Published[^1].LocalSample("cluster-a");
        Assert.Multiple(() =>
        {
            Assert.That(latest.Bytes, Is.EqualTo(2000), "the published sample must follow a rising partial figure");
            Assert.That(latest.Keys, Is.EqualTo(20));
        });
    }

    private static TreeStorageUsageReport PartialReport(long bytes, long keys) => new()
    {
        TreeId = "t/acme/orders",
        TotalBytes = bytes,
        LiveKeys = keys,
        LeafStateBytes = bytes,
        Partial = true,
    };

    [Test]
    public async Task A_partial_report_with_nothing_on_record_is_metered_as_before()
    {
        var harness = new MutableUsageHarness();
        harness.AddTree("t/acme/orders", bytes: 0, keys: 0);
        harness.Report("t/acme/orders", new TreeStorageUsageReport
        {
            TreeId = "t/acme/orders",
            TotalBytes = 1024,
            LiveKeys = 8,
            LeafStateBytes = 1024,
            Partial = true,
        });
        var store = new RecordingStore();
        var service = Create(new FakeRegistry(Acme), store, harness.Factory);

        await service.MeterOnceAsync(CancellationToken.None);

        var latest = store.Published[^1].LocalSample("cluster-a");
        Assert.That(latest.Bytes, Is.EqualTo(1024),
            "with no earlier sample the partial figure is still the best one available");
    }
}
