using System.Runtime.ExceptionServices;

namespace Orleans.Lattice;

/// <summary>
/// Default <see cref="ILatticeRetryPolicy"/> implementation: bounded
/// exponential back-off with an optional retryable-exception
/// classifier. Hand-rolled so the core library carries no third-party
/// resilience dependency.
/// </summary>
/// <remarks>
/// <para>
/// On each failed attempt the policy delays for
/// <c>min(MaxDelay, InitialDelay * 2^(attempt-1))</c> and re-runs the
/// caller's lambda under the <em>same</em> ambient
/// <see cref="LatticeIdempotencyContext"/> scope. On budget
/// exhaustion the original failure is re-thrown verbatim (preserving
/// the stack trace via <see cref="ExceptionDispatchInfo"/>) so
/// existing caller-side exception-handling logic continues to work
/// unchanged.
/// </para>
/// <para>
/// Both <see cref="ExecuteAsync(Func{CancellationToken, Task}, CancellationToken)"/>
/// and the typed overload share a single retry loop via a closure so
/// the back-off schedule and classifier rules stay in lock-step.
/// </para>
/// </remarks>
public sealed class BoundedExponentialRetryPolicy : ILatticeRetryPolicy
{
    private readonly int _maxAttempts;
    private readonly TimeSpan _initialDelay;
    private readonly TimeSpan _maxDelay;
    private readonly Func<Exception, bool>? _classifier;

    /// <summary>
    /// Constructs a policy with explicit parameters. Prefer the
    /// <see cref="BoundedExponentialRetryPolicyOptions"/> overload
    /// when the values come from configuration.
    /// </summary>
    /// <param name="maxAttempts">Total attempts, including the first.
    /// Must be at least 1.</param>
    /// <param name="initialDelay">Delay before the first retry.
    /// Must be non-negative.</param>
    /// <param name="maxDelay">Upper bound on the back-off schedule.
    /// Must be greater than or equal to
    /// <paramref name="initialDelay"/>.</param>
    /// <param name="retryableExceptionClassifier">
    /// Optional filter selecting which exceptions are transient. When
    /// <c>null</c>, every exception is treated as retryable.
    /// </param>
    public BoundedExponentialRetryPolicy(
        int maxAttempts = 4,
        TimeSpan? initialDelay = null,
        TimeSpan? maxDelay = null,
        Func<Exception, bool>? retryableExceptionClassifier = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);
        var initial = initialDelay ?? TimeSpan.FromMilliseconds(50);
        var max = maxDelay ?? TimeSpan.FromSeconds(2);
        if (initial < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(initialDelay), "InitialDelay must be non-negative.");
        if (max < initial)
            throw new ArgumentOutOfRangeException(nameof(maxDelay), "MaxDelay must be >= InitialDelay.");

        _maxAttempts = maxAttempts;
        _initialDelay = initial;
        _maxDelay = max;
        _classifier = retryableExceptionClassifier;
    }

    /// <summary>
    /// Constructs a policy from a populated
    /// <see cref="BoundedExponentialRetryPolicyOptions"/> instance.
    /// </summary>
    public BoundedExponentialRetryPolicy(BoundedExponentialRetryPolicyOptions options)
        : this(
            (options ?? throw new ArgumentNullException(nameof(options))).MaxAttempts,
            options.InitialDelay,
            options.MaxDelay,
            options.RetryableExceptionClassifier)
    {
    }

    /// <inheritdoc />
    public async Task ExecuteAsync(Func<CancellationToken, Task> operation, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operation);
        await ExecuteAsync<object?>(async ct =>
        {
            await operation(ct).ConfigureAwait(false);
            return null;
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<T> ExecuteAsync<T>(Func<CancellationToken, Task<T>> operation, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ExceptionDispatchInfo? captured = null;
        for (var attempt = 1; attempt <= _maxAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                return await operation(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                if (_classifier is not null && !_classifier(ex))
                {
                    throw;
                }
                captured = ExceptionDispatchInfo.Capture(ex);
                if (attempt == _maxAttempts)
                {
                    break;
                }
                var delay = ComputeBackoff(attempt);
                if (delay > TimeSpan.Zero)
                {
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        captured!.Throw();
        // Unreachable: ExceptionDispatchInfo.Throw never returns.
        throw new InvalidOperationException("Unreachable.");
    }

    private TimeSpan ComputeBackoff(int attempt)
    {
        // attempt is 1-based; first retry uses initial delay, doubling thereafter.
        var shift = attempt - 1;
        if (shift >= 31)
            return _maxDelay;
        var multiplier = 1L << shift;
        var ticks = _initialDelay.Ticks * multiplier;
        if (ticks < 0 || ticks > _maxDelay.Ticks)
            return _maxDelay;
        return TimeSpan.FromTicks(ticks);
    }
}
