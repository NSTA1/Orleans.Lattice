namespace Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;

/// <summary>
/// A unit of evidence: the hits proposed inside it commit together on <see cref="Complete"/>,
/// or are discarded when it is disposed without completing. See <see cref="Coverage"/>.
/// </summary>
public sealed class CoverageScope : IDisposable
{
    private readonly CoverageRecorder _recorder;
    private readonly List<string> _proposed = [];
    private readonly Lock _gate = new();
    private bool _completed;
    private bool _disposed;

    internal CoverageScope(CoverageRecorder recorder, string name, CoverageScope? parent)
    {
        _recorder = recorder;
        Name = name;
        Parent = parent;
    }

    /// <summary>The scope's name.</summary>
    public string Name { get; }

    /// <summary>The scope that encloses this one, if any.</summary>
    public CoverageScope? Parent { get; }

    /// <summary>The hits proposed and not yet committed.</summary>
    public IReadOnlyList<string> Proposed
    {
        get
        {
            lock (_gate)
            {
                return [.. _proposed];
            }
        }
    }

    /// <summary>Whether <see cref="Complete"/> committed this scope's hits.</summary>
    public bool IsCompleted => _completed;

    /// <summary>
    /// Commits the proposed hits as evidence. Refused when the current test already holds a
    /// failed assertion (as inside <c>Assert.Multiple</c>), because then the hits are not
    /// evidence; the hits are discarded.
    /// </summary>
    /// <exception cref="InvalidOperationException">The scope is closed, or the test holds a failed assertion.</exception>
    public void Complete()
    {
        string[] hits;
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_completed)
            {
                throw new InvalidOperationException($"Coverage scope '{Name}' is already complete.");
            }

            hits = [.. _proposed];
            _proposed.Clear();
            _completed = true;
        }

        if (CurrentTestHasFailedAssertion())
        {
            throw new InvalidOperationException(
                $"Coverage scope '{Name}' cannot complete: the test holds a failed assertion, so its {hits.Length} proposed hit(s) are not evidence and were discarded.");
        }

        _recorder.Commit(Name, hits);
    }

    /// <summary>Closes the scope; hits proposed and not committed are discarded.</summary>
    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            _proposed.Clear();
        }

        _recorder.EndScope(this);
    }

    internal void Propose(string featureId)
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_completed)
            {
                throw new InvalidOperationException(
                    $"Coverage.Hit(\"{featureId}\") after scope '{Name}' completed: a hit belongs to the scope whose steps proved it.");
            }

            _proposed.Add(featureId);
        }
    }

    private static bool CurrentTestHasFailedAssertion()
    {
        try
        {
            return TestContext.CurrentContext.Result.Assertions.Any(assertion =>
                assertion.Status is NUnit.Framework.Interfaces.AssertionStatus.Failed or NUnit.Framework.Interfaces.AssertionStatus.Error);
        }
        catch (Exception ex) when (ex is InvalidOperationException or NullReferenceException)
        {
            return false;
        }
    }
}
