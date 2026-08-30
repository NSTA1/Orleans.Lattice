using Grpc.Core;
using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Explorer.Telemetry;

namespace Orleans.Lattice.Explorer.Tests.Telemetry;

/// <summary>
/// Covers the bounded parameters: the limits the server published, the pure
/// checks a panel evaluates against them, and the pre-flight refusal the seam
/// makes when a caller's chosen window is already ruled out.
/// </summary>
/// <remarks>
/// Every case supplies its own instant rather than reading a clock, so nothing
/// here depends on the wall clock, on timing, or on the order the tests run in.
/// </remarks>
[TestFixture]
public class TelemetryBoundsTests
{
    private static readonly ExplorerTelemetryBounds Bounds = new(
        MinStep: TimeSpan.FromSeconds(15),
        MaxStep: TimeSpan.FromHours(1),
        DefaultStep: TimeSpan.FromMinutes(1),
        MaxRange: TimeSpan.FromHours(24),
        MaxLookback: TimeSpan.FromDays(7),
        MaxPoints: 1440);

    private FakeTelemetryQueryClient _client = null!;

    [SetUp]
    public void SetUp() => _client = new FakeTelemetryQueryClient();

    private TelemetryQueryService Create() => new(_client);

    private static ExplorerTelemetryWindow Window(TimeSpan length, TimeSpan step) =>
        ExplorerTelemetryWindow.Between(SampleTelemetry.Anchor, SampleTelemetry.Anchor + length, step);

    [Test]
    public void An_unbounded_entry_accepts_anything() =>
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerTelemetryBounds.Unbounded.IsUnbounded, Is.True);
            Assert.That(
                ExplorerTelemetryBounds.Unbounded.ValidateWithoutClock(Window(TimeSpan.FromDays(365), TimeSpan.Zero)),
                Is.EqualTo(ExplorerTelemetryBoundsViolation.None));
            Assert.That(
                ExplorerTelemetryBounds.Unbounded.Validate(
                    Window(TimeSpan.FromDays(365), TimeSpan.Zero),
                    SampleTelemetry.Anchor.AddYears(10)),
                Is.EqualTo(ExplorerTelemetryBoundsViolation.None));
            Assert.That(Bounds.IsUnbounded, Is.False);
        });

    [Test]
    public void The_effective_step_defaults_then_clamps() =>
        Assert.Multiple(() =>
        {
            Assert.That(Bounds.EffectiveStep(TimeSpan.Zero), Is.EqualTo(TimeSpan.FromMinutes(1)));
            Assert.That(Bounds.EffectiveStep(TimeSpan.FromSeconds(-1)), Is.EqualTo(TimeSpan.FromMinutes(1)));
            Assert.That(Bounds.EffectiveStep(TimeSpan.FromSeconds(1)), Is.EqualTo(TimeSpan.FromSeconds(15)));
            Assert.That(Bounds.EffectiveStep(TimeSpan.FromHours(9)), Is.EqualTo(TimeSpan.FromHours(1)));
            Assert.That(Bounds.EffectiveStep(TimeSpan.FromMinutes(5)), Is.EqualTo(TimeSpan.FromMinutes(5)));
            Assert.That(
                ExplorerTelemetryBounds.Unbounded.EffectiveStep(TimeSpan.FromMinutes(5)),
                Is.EqualTo(TimeSpan.FromMinutes(5)));
        });

    [Test]
    public void Each_clock_independent_limit_is_named_separately() =>
        Assert.Multiple(() =>
        {
            Assert.That(
                Bounds.ValidateWithoutClock(
                    ExplorerTelemetryWindow.Between(
                        SampleTelemetry.Anchor,
                        SampleTelemetry.Anchor.AddHours(-1),
                        TimeSpan.FromMinutes(1))),
                Is.EqualTo(ExplorerTelemetryBoundsViolation.RangeNotAscending));
            Assert.That(
                Bounds.ValidateWithoutClock(Window(TimeSpan.FromHours(1), TimeSpan.FromSeconds(1))),
                Is.EqualTo(ExplorerTelemetryBoundsViolation.StepBelowMinimum));
            Assert.That(
                Bounds.ValidateWithoutClock(Window(TimeSpan.FromHours(1), TimeSpan.FromSeconds(-1))),
                Is.EqualTo(ExplorerTelemetryBoundsViolation.StepBelowMinimum));
            Assert.That(
                Bounds.ValidateWithoutClock(Window(TimeSpan.FromHours(2), TimeSpan.FromHours(2))),
                Is.EqualTo(ExplorerTelemetryBoundsViolation.StepAboveMaximum));
            Assert.That(
                Bounds.ValidateWithoutClock(Window(TimeSpan.FromDays(2), TimeSpan.FromMinutes(1))),
                Is.EqualTo(ExplorerTelemetryBoundsViolation.RangeTooLong));
            Assert.That(
                Bounds.ValidateWithoutClock(Window(TimeSpan.FromHours(20), TimeSpan.FromSeconds(15))),
                Is.EqualTo(ExplorerTelemetryBoundsViolation.TooManyPoints));
            Assert.That(
                Bounds.ValidateWithoutClock(Window(TimeSpan.FromHours(1), TimeSpan.FromMinutes(1))),
                Is.EqualTo(ExplorerTelemetryBoundsViolation.None));
        });

    [Test]
    public void The_retention_limit_is_checked_only_against_a_supplied_instant() =>
        Assert.Multiple(() =>
        {
            var window = Window(TimeSpan.FromHours(1), TimeSpan.FromMinutes(1));

            Assert.That(
                Bounds.Validate(window, SampleTelemetry.Anchor.AddDays(1)),
                Is.EqualTo(ExplorerTelemetryBoundsViolation.None));
            Assert.That(
                Bounds.Validate(window, SampleTelemetry.Anchor.AddDays(30)),
                Is.EqualTo(ExplorerTelemetryBoundsViolation.LookbackTooOld));
            Assert.That(
                Bounds.ValidateWithoutClock(window),
                Is.EqualTo(ExplorerTelemetryBoundsViolation.None),
                "the clock-independent check never reaches the retention limit");
        });

    [Test]
    public void The_clock_independent_check_accepts_the_unset_window_the_retention_check_would_refuse()
    {
        // The trap this split exists for. An unset window starts at the default
        // instant, which is older than any retention limit, so a pre-flight that
        // included the retention arm would refuse the very first request a panel
        // makes.
        Assert.Multiple(() =>
        {
            Assert.That(
                Bounds.ValidateWithoutClock(ExplorerTelemetryWindow.Unset),
                Is.EqualTo(ExplorerTelemetryBoundsViolation.None));
            Assert.That(
                Bounds.Validate(ExplorerTelemetryWindow.Unset, SampleTelemetry.Anchor),
                Is.EqualTo(ExplorerTelemetryBoundsViolation.LookbackTooOld));
        });
    }

    [Test]
    public async Task A_chosen_window_the_published_bounds_rule_out_is_refused_before_the_wire()
    {
        var service = Create();
        await service.GetCatalogAsync();

        var result = await service.QueryAsync(new ExplorerTelemetryRequest
        {
            QueryId = SampleTelemetry.RangeQueryId,
            Window = Window(TimeSpan.FromDays(2), TimeSpan.FromMinutes(1)),
        });

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.False);
            Assert.That(result.Status, Is.EqualTo(TelemetryQueryStatus.OutOfBounds));
            Assert.That(result.Violation, Is.EqualTo(ExplorerTelemetryBoundsViolation.RangeTooLong));
            Assert.That(result.Message, Does.Contain(SampleTelemetry.RangeQueryId));
            Assert.That(_client.QueryCallCount, Is.Zero, "a window the server already ruled out costs no round trip");
        });
    }

    [Test]
    public async Task A_window_within_the_published_bounds_is_sent()
    {
        var service = Create();
        await service.GetCatalogAsync();

        var result = await service.QueryAsync(new ExplorerTelemetryRequest
        {
            QueryId = SampleTelemetry.RangeQueryId,
            Window = Window(TimeSpan.FromHours(1), TimeSpan.FromMinutes(1)),
        });

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(_client.QueryCallCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Nothing_is_refused_before_the_bounds_have_been_discovered()
    {
        // No catalogue read has happened, so the seam knows no bounds and must not
        // invent any: the facade decides.
        var result = await Create().QueryAsync(new ExplorerTelemetryRequest
        {
            QueryId = SampleTelemetry.RangeQueryId,
            Window = Window(TimeSpan.FromDays(2), TimeSpan.FromMinutes(1)),
        });

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(_client.CatalogCallCount, Is.Zero, "checking a window never costs a discovery round trip");
            Assert.That(_client.QueryCallCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task An_id_the_remembered_catalogue_does_not_list_is_still_sent()
    {
        // Answering "unknown" locally would let a stale cache hide a query the
        // cluster does offer, and would invent a discovery answer from an
        // execution one. Whether a query exists is the facade's answer to give.
        var service = Create();
        await service.GetCatalogAsync();

        var result = await service.QueryAsync(new ExplorerTelemetryRequest
        {
            QueryId = SampleTelemetry.UnknownQueryId,
            Window = Window(TimeSpan.FromDays(2), TimeSpan.FromMinutes(1)),
        });

        Assert.Multiple(() =>
        {
            Assert.That(_client.QueryCallCount, Is.EqualTo(1));
            Assert.That(result.Status, Is.Not.EqualTo(TelemetryQueryStatus.UnknownQuery));
        });
    }

    [Test]
    public async Task A_bounds_refusal_from_the_facade_carries_its_message_and_an_unnamed_violation()
    {
        // The transport carries a status and a message, not the violation value,
        // so a refusal that crossed the wire cannot name the limit.
        _client.QueryThrows = new RpcException(
            new Status(StatusCode.OutOfRange, "the window is longer than this entry allows"));

        var result = await Create().QueryAsync(ExplorerTelemetryRequest.For(SampleTelemetry.RangeQueryId));

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(TelemetryQueryStatus.OutOfBounds));
            Assert.That(result.Violation, Is.EqualTo(ExplorerTelemetryBoundsViolation.Unspecified));
            Assert.That(result.Message, Is.EqualTo("the window is longer than this entry allows"));
        });
    }

    [Test]
    public async Task A_typed_bounds_refusal_keeps_the_limit_the_facade_named()
    {
        _client.QueryThrows = new TelemetryQueryBoundsException(
            SampleTelemetry.RangeQueryId,
            TelemetryBoundsViolation.TooManyPoints);

        var result = await Create().QueryAsync(ExplorerTelemetryRequest.For(SampleTelemetry.RangeQueryId));

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(TelemetryQueryStatus.OutOfBounds));
            Assert.That(result.Violation, Is.EqualTo(ExplorerTelemetryBoundsViolation.TooManyPoints));
        });
    }

    [Test]
    public void Every_violation_the_facade_can_name_projects_onto_its_own_value()
    {
        (TelemetryBoundsViolation Wire, ExplorerTelemetryBoundsViolation Expected)[] cases =
        [
            (TelemetryBoundsViolation.None, ExplorerTelemetryBoundsViolation.None),
            (TelemetryBoundsViolation.RangeNotAscending, ExplorerTelemetryBoundsViolation.RangeNotAscending),
            (TelemetryBoundsViolation.StepBelowMinimum, ExplorerTelemetryBoundsViolation.StepBelowMinimum),
            (TelemetryBoundsViolation.StepAboveMaximum, ExplorerTelemetryBoundsViolation.StepAboveMaximum),
            (TelemetryBoundsViolation.RangeTooLong, ExplorerTelemetryBoundsViolation.RangeTooLong),
            (TelemetryBoundsViolation.LookbackTooOld, ExplorerTelemetryBoundsViolation.LookbackTooOld),
            (TelemetryBoundsViolation.TooManyPoints, ExplorerTelemetryBoundsViolation.TooManyPoints),
        ];

        Assert.Multiple(() =>
        {
            foreach (var (wire, expected) in cases)
            {
                Assert.That(TelemetryProjection.ToViolation(wire), Is.EqualTo(expected));
            }

            Assert.That(
                TelemetryProjection.ToViolation((TelemetryBoundsViolation)999),
                Is.EqualTo(ExplorerTelemetryBoundsViolation.Unspecified),
                "a value this build does not know about must not masquerade as a named limit");
        });
    }

    [Test]
    public void A_window_reports_its_own_shape() =>
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerTelemetryWindow.Unset.IsUnset, Is.True);
            Assert.That(default(ExplorerTelemetryWindow).IsUnset, Is.True);
            Assert.That(ExplorerTelemetryWindow.At(SampleTelemetry.Anchor).IsUnset, Is.False);
            Assert.That(ExplorerTelemetryWindow.At(SampleTelemetry.Anchor).IsInstant, Is.True);
            Assert.That(ExplorerTelemetryWindow.At(SampleTelemetry.Anchor).PointCount, Is.EqualTo(1));
            Assert.That(Window(TimeSpan.FromHours(1), TimeSpan.FromMinutes(1)).Duration, Is.EqualTo(TimeSpan.FromHours(1)));
            Assert.That(Window(TimeSpan.FromHours(1), TimeSpan.FromMinutes(1)).PointCount, Is.EqualTo(61));
            Assert.That(Window(TimeSpan.FromHours(1), TimeSpan.Zero).PointCount, Is.EqualTo(1));
            Assert.That(Window(TimeSpan.FromHours(-1), TimeSpan.FromMinutes(1)).PointCount, Is.Zero);
            Assert.That(Window(TimeSpan.FromHours(-1), TimeSpan.FromMinutes(1)).IsAscending, Is.False);
            Assert.That(
                Window(TimeSpan.FromHours(1), TimeSpan.FromMinutes(1)).WithStep(TimeSpan.FromMinutes(5)).Step,
                Is.EqualTo(TimeSpan.FromMinutes(5)));
        });
}
