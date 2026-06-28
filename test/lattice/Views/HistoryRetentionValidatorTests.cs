using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Unit tests for <see cref="HistoryRetentionValidator"/>: the guard applied
/// before a durable-history retention override is persisted.
/// </summary>
[TestFixture]
public sealed class HistoryRetentionValidatorTests
{
    [Test]
    public void Validate_accepts_null_mode_and_window() =>
        Assert.That(() => HistoryRetentionValidator.Validate(null, null), Throws.Nothing);

    [Test]
    public void Validate_accepts_positive_window() =>
        Assert.That(
            () => HistoryRetentionValidator.Validate(HistoryRetentionMode.FullValue, TimeSpan.FromHours(1)),
            Throws.Nothing);

    [Test]
    public void Validate_rejects_zero_window() =>
        Assert.That(
            () => HistoryRetentionValidator.Validate(null, TimeSpan.Zero),
            Throws.TypeOf<ArgumentOutOfRangeException>());

    [Test]
    public void Validate_rejects_negative_window() =>
        Assert.That(
            () => HistoryRetentionValidator.Validate(null, TimeSpan.FromSeconds(-1)),
            Throws.TypeOf<ArgumentOutOfRangeException>());

    [Test]
    public void Validate_rejects_undefined_mode() =>
        Assert.That(
            () => HistoryRetentionValidator.Validate((HistoryRetentionMode)99, null),
            Throws.TypeOf<ArgumentOutOfRangeException>());
}
