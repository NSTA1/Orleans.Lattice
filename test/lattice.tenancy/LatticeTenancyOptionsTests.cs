namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>Unit tests for <see cref="LatticeTenancyOptions"/> defaults.</summary>
public sealed class LatticeTenancyOptionsTests
{
    [Test]
    public void Defaults_retain_metadata_history_seed_default_and_durable_view()
    {
        var options = new LatticeTenancyOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.HistoryRetentionMode, Is.EqualTo(HistoryRetentionMode.MetadataOnly));
            Assert.That(options.HistoryRetentionWindow, Is.Null);
            Assert.That(options.EnableDurableHistoryView, Is.True);
            Assert.That(options.SeedDefaultTenant, Is.True);
        });
    }

    [Test]
    public void Properties_round_trip_assigned_values()
    {
        var window = TimeSpan.FromDays(7);
        var options = new LatticeTenancyOptions
        {
            HistoryRetentionMode = HistoryRetentionMode.FullValue,
            HistoryRetentionWindow = window,
            EnableDurableHistoryView = false,
            SeedDefaultTenant = false,
        };

        Assert.Multiple(() =>
        {
            Assert.That(options.HistoryRetentionMode, Is.EqualTo(HistoryRetentionMode.FullValue));
            Assert.That(options.HistoryRetentionWindow, Is.EqualTo(window));
            Assert.That(options.EnableDurableHistoryView, Is.False);
            Assert.That(options.SeedDefaultTenant, Is.False);
        });
    }
}
