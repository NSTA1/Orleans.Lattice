namespace Orleans.Lattice.Schema;

/// <summary>
/// The keys of the string result map a finished schema operation carries, and the
/// <see cref="Outcome"/> values it records. A remediation that stopped at an
/// offending value finishes as failed, and its map names the value; one that was
/// cancelled before cutover finishes as cancelled.
/// </summary>
public static class SchemaOperationResultKeys
{
    /// <summary>How the remediation ended: <see cref="Completed"/>, <see cref="Aborted"/>, <see cref="Cancelled"/> or <see cref="Superseded"/>.</summary>
    public const string Outcome = "outcome";

    /// <summary>The values processed: the whole tree on completion, up to and including the offender on an abort.</summary>
    public const string ValuesProcessed = "valuesProcessed";

    /// <summary>The first key whose value could not be remediated (aborted only).</summary>
    public const string OffendingKey = "offendingKey";

    /// <summary>Why the offending value failed (aborted only).</summary>
    public const string Reason = "reason";

    /// <summary>
    /// The operation id the tree's remediation coordinator recorded
    /// (<see cref="LatticeSchemaRemediationReport.OperationId"/>). It differs from the
    /// tracked operation's own id when the start joined a remediation already in flight.
    /// </summary>
    public const string RemediationOperationId = "remediationOperationId";

    /// <summary>The <see cref="Outcome"/> of a remediation that cut the tree over.</summary>
    public const string Completed = "completed";

    /// <summary>The <see cref="Outcome"/> of a remediation that stopped at an offending value with nothing cut over.</summary>
    public const string Aborted = "aborted";

    /// <summary>The <see cref="Outcome"/> of a remediation cancelled before cutover, with nothing cut over.</summary>
    public const string Cancelled = "cancelled";

    /// <summary>
    /// The <see cref="Outcome"/> of an operation whose start joined a remediation already
    /// in flight, when that remediation ended under the operation that started it and a
    /// later remediation replaced its outcome on the tree's coordinator. The operation
    /// fails; <see cref="RemediationOperationId"/> names the remediation it followed,
    /// whose outcome is on that operation's record, and this operation applied nothing
    /// further.
    /// </summary>
    public const string Superseded = "superseded";
}
