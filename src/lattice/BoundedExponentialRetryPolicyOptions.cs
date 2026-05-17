namespace Orleans.Lattice;

/// <summary>
/// Configuration for the shipped <see cref="BoundedExponentialRetryPolicy"/>
/// default implementation of <see cref="ILatticeRetryPolicy"/>. Captured
/// by value when the policy is constructed; mutating the options
/// instance after the fact has no effect.
/// </summary>
public sealed class BoundedExponentialRetryPolicyOptions
{
    /// <summary>
    /// Maximum number of attempts the policy will run before surfacing
    /// the original failure. Must be at least 1. Default is 4.
    /// </summary>
    public int MaxAttempts { get; set; } = 4;

    /// <summary>
    /// Initial back-off delay before the first retry. Must be
    /// non-negative. Default is 50 ms.
    /// </summary>
    public TimeSpan InitialDelay { get; set; } = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// Upper bound on the back-off delay. The exponential schedule
    /// doubles on each attempt and is clamped at this value. Must be
    /// greater than or equal to <see cref="InitialDelay"/>. Default is
    /// 2 s.
    /// </summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Optional classifier deciding whether a thrown exception is a
    /// transient storage fault that should be retried, or a permanent
    /// failure that should surface immediately. When <c>null</c> the
    /// policy retries on every exception (the operationally-simplest
    /// default for hosts wiring up retry for the first time). Hosts
    /// running in storage backends with known transient classes
    /// should narrow this to those exception types.
    /// </summary>
    public Func<Exception, bool>? RetryableExceptionClassifier { get; set; }
}
