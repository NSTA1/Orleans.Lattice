using System.Collections.Concurrent;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Control-arm results, once per distinct <see cref="TlcControlRunKey"/>
/// within this test process.
/// <para>
/// The <see cref="Lazy{T}"/> runs the first request and makes every concurrent
/// one wait for it, and a run that fails or throws is cached as such, so every
/// case sharing that control fails rather than only the first. Process-local by
/// design: nothing is reused across runs.
/// </para>
/// </summary>
/// <typeparam name="TResult">The result of one control run.</typeparam>
internal sealed class TlcControlRunCache<TResult>
{
    private readonly ConcurrentDictionary<string, Lazy<TResult>> _runs = new(StringComparer.Ordinal);

    /// <summary>The number of distinct controls requested so far, which is the number of control launches.</summary>
    public int Count => _runs.Count;

    /// <summary>
    /// Returns the result of the control run identified by <paramref name="key"/>,
    /// running <paramref name="run"/> only if no case has requested that key yet.
    /// </summary>
    /// <param name="key">The control's <see cref="TlcControlRunKey"/>.</param>
    /// <param name="run">Runs the control; invoked at most once per key.</param>
    public TResult GetOrRun(string key, Func<TResult> run)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(run);

        return _runs
            .GetOrAdd(key, _ => new Lazy<TResult>(run, LazyThreadSafetyMode.ExecutionAndPublication))
            .Value;
    }
}
