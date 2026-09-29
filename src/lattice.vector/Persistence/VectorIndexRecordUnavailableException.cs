namespace Orleans.Lattice.Vector.Persistence;

/// <summary>
/// Thrown by a full load of a <see cref="DurableVectorIndex"/> when a record the
/// committed manifest names was not returned by the read that asked for it, but
/// an independent read of the same key did return it.
/// <para>
/// That disagreement means the store could not serve a consistent read at that
/// moment - an admission refusal, a routing gap, or a partial answer under load -
/// not that the record is damaged. The durable index is left exactly as it was,
/// and the instance keeps every piece of progress its load banked, so the caller
/// retries <see cref="DurableVectorIndex.LoadOrResumeAsync(CancellationToken)"/>
/// after a backoff instead of rebuilding from source. A record that is absent on
/// every read path is still treated as unloadable and discarded.
/// </para>
/// <para>
/// This derives directly from <see cref="Exception"/> so that a consumer which
/// later makes it serializable does not need a hand-written deep copier.
/// </para>
/// </summary>
public sealed class VectorIndexRecordUnavailableException : Exception
{
    /// <summary>Creates the exception with a message naming the record that could not be read.</summary>
    /// <param name="message">A description of which read disagreed with which.</param>
    public VectorIndexRecordUnavailableException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with a message and an underlying cause.</summary>
    /// <param name="message">A description of which read disagreed with which.</param>
    /// <param name="innerException">The underlying cause.</param>
    public VectorIndexRecordUnavailableException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
