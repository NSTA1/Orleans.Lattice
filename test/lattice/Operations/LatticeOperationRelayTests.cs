using NSubstitute;
using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Tests.Operations;

/// <summary>
/// Unit tests for <see cref="LatticeOperationRelay"/>, the grain-side half of a
/// tracked grain call (#4124): it is the ambient progress sink while open, relays
/// coalesced reports to the operation's tracking grain, cancels its token when the
/// grain answers with the stop signal, banks on demand, and an untracked relay
/// reports nothing.
/// </summary>
[TestFixture]
public sealed class LatticeOperationRelayTests
{
    private const string Key = "default|relay-op";

    private static (IGrainFactory Factory, RecordingOperationGrain Grain) Wire()
    {
        var grain = new RecordingOperationGrain();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeOperationGrain>(Key, null).Returns(grain);
        return (factory, grain);
    }

    private static LatticeOperationTicket Ticket => new() { OperationKey = Key };

    [Test]
    public async Task A_tracked_relay_is_ambient_and_relays_reports_to_the_tracking_grain()
    {
        var (factory, grain) = Wire();

        using (var relay = LatticeOperationRelay.Open(factory, Ticket, CancellationToken.None))
        {
            Assert.That(LatticeOperationProgress.Current, Is.SameAs(relay.Progress));
            await LatticeOperationProgress.Current!.ReportAsync("Copying", 0, 10, "entries");
            await LatticeOperationProgress.Current!.ReportAsync("Copying", 10, 10, "entries");
            await relay.FlushAsync();
        }

        Assert.Multiple(() =>
        {
            Assert.That(LatticeOperationProgress.Current, Is.Null, "Disposing restores the previous sink.");
            Assert.That(grain.Reports.Select(r => r.CompletedUnits), Is.EqualTo(new long[] { 0, 10 }));
        });
    }

    [Test]
    public async Task The_stop_signal_cancels_the_relay_token()
    {
        var (factory, grain) = Wire();
        grain.StopAlways = true;
        using var relay = LatticeOperationRelay.Open(factory, Ticket, CancellationToken.None);

        await relay.Progress!.ReportAsync("Probing");

        Assert.Multiple(() =>
        {
            Assert.That(relay.Token.IsCancellationRequested, Is.True);
            Assert.That(async () => await relay.Progress!.ReportAsync("Probing", 1), Throws.InstanceOf<OperationCanceledException>());
        });
    }

    [Test]
    public async Task Banking_writes_the_coalesced_report_through()
    {
        var (factory, grain) = Wire();
        using var relay = LatticeOperationRelay.Open(factory, Ticket, CancellationToken.None);
        await relay.Progress!.ReportAsync("Projecting", 0, null, "keys");
        await relay.Progress!.ReportAsync("Projecting", 7, null, "keys");

        await relay.BankProgressAsync();

        Assert.That(grain.Reports[^1].CompletedUnits, Is.EqualTo(7), "The coalesced unit is banked.");
    }

    [Test]
    public async Task An_untracked_relay_reports_nothing_and_follows_the_call_token()
    {
        var factory = Substitute.For<IGrainFactory>();
        using var source = new CancellationTokenSource();
        using var relay = LatticeOperationRelay.Open(factory, null, source.Token);

        Assert.Multiple(() =>
        {
            Assert.That(relay.Progress, Is.Null);
            Assert.That(LatticeOperationProgress.Current, Is.Null);
        });

        await relay.FlushAsync();
        await relay.BankProgressAsync();
        source.Cancel();

        Assert.That(relay.Token.IsCancellationRequested, Is.True);
        factory.DidNotReceiveWithAnyArgs().GetGrain<ILatticeOperationGrain>(default(string)!, default);
    }

    [Test]
    public void Open_rejects_a_null_grain_factory() =>
        Assert.That(() => LatticeOperationRelay.Open(null!, Ticket, CancellationToken.None), Throws.ArgumentNullException);

    [Test]
    public void Dispose_is_idempotent()
    {
        var (factory, _) = Wire();
        var relay = LatticeOperationRelay.Open(factory, Ticket, CancellationToken.None);

        relay.Dispose();

        Assert.That(relay.Dispose, Throws.Nothing);
    }

    [Test]
    public void Ticket_For_composes_the_tracking_grain_key() =>
        Assert.That(LatticeOperationTicket.For("t1", "op-9").OperationKey, Is.EqualTo(LatticeOperationKey.For("t1", "op-9")));
}
