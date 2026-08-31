using System.Runtime.CompilerServices;

namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers the optional population-bound member on <see cref="IGrainKeySource"/>:
/// its default, which is what lets an existing source keep compiling untouched,
/// and an override that supplies a real bound.
/// </summary>
[TestFixture]
public sealed class GrainKeySourceApproximateCountTests
{
    [Test]
    public async Task A_source_that_does_not_implement_the_bound_reports_none()
    {
        IGrainKeySource source = new StreamOnlyKeySource();

        Assert.That(await source.TryGetApproximateCountAsync(CancellationToken.None), Is.Null);
    }

    [Test]
    public async Task A_source_that_implements_the_bound_reports_it()
    {
        IGrainKeySource source = new CountingKeySource(1234);

        Assert.That(await source.TryGetApproximateCountAsync(CancellationToken.None), Is.EqualTo(1234));
    }

    [Test]
    public async Task A_source_may_report_no_bound_even_when_it_implements_the_member()
    {
        IGrainKeySource source = new CountingKeySource(null);

        Assert.That(await source.TryGetApproximateCountAsync(CancellationToken.None), Is.Null);
    }

    [Test]
    public async Task The_default_ignores_a_cancelled_token_because_it_does_no_work()
    {
        IGrainKeySource source = new StreamOnlyKeySource();
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        Assert.That(await source.TryGetApproximateCountAsync(cancelled.Token), Is.Null);
    }

    /// <summary>A key source written before the bound existed: it only streams keys.</summary>
    private sealed class StreamOnlyKeySource : IGrainKeySource
    {
        public async IAsyncEnumerable<string> EnumerateKeysAsync(
            string? resumeAfterExclusive,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            yield break;
        }
    }

    /// <summary>A key source that knows how big its population is.</summary>
    private sealed class CountingKeySource(long? count) : IGrainKeySource
    {
        public async IAsyncEnumerable<string> EnumerateKeysAsync(
            string? resumeAfterExclusive,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            yield break;
        }

        public ValueTask<long?> TryGetApproximateCountAsync(CancellationToken cancellationToken) =>
            ValueTask.FromResult(count);
    }
}
