namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Where the value on an effective-configuration line came from: an operator's
/// declaration, this host's own fallback, or the runtime itself.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this exists (issue #2586).</b> The report used to print a defaulted value in
/// exactly the shape it prints a declared one. A live container logged a warning naming
/// <c>LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD</c> as unset, predicting the precise failure
/// that followed, and then logged
/// <c>Repository-context effective configuration: LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD = 120s</c>
/// seconds later about the same variable. The variable was genuinely absent from the
/// container environment. The second line won, because a warning is read by whoever
/// happens to be watching when it scrolls past, whereas this report is read by whoever
/// later asks what the configuration <i>is</i> - which is the deliberate act of an
/// operator or an agent auditing a deployment, and is the audience that matters.
/// </para>
/// <para>
/// <b>Why provenance is distinct from "overridden".</b> The existing
/// <c>[OVERRIDDEN, default X]</c> marker is computed by comparing the resolved value with
/// the value this host reaches when nothing is supplied. That answers <i>did the value
/// move?</i>, which is not the same question as <i>did anybody set this?</i>, and the two
/// answers come apart in both directions. A key nothing declares can still resolve away
/// from the pristine default because a neighbouring key moved it - so
/// <c>[OVERRIDDEN]</c> alone reads as "somebody set this" when nobody did - and a key an
/// operator declared can resolve to exactly the default, which is the case that produced
/// this issue. The two markers are therefore orthogonal and both are printed.
/// </para>
/// <para>
/// <b>Why there is a third case rather than a boolean.</b> The report also carries
/// runtime facts that are not settings at all, <c>Environment.ProcessorCount</c> among
/// them. Leaving those unmarked would reintroduce the defect one level down: once every
/// setting is marked, an unmarked line reads as declared. Naming them explicitly keeps the
/// invariant total, which is what lets a test assert that <b>no</b> line can reach the log
/// without a provenance marker.
/// </para>
/// </remarks>
public enum RepoContextSettingProvenance
{
    /// <summary>
    /// The value was supplied to this process, so it reflects somebody's intent.
    /// </summary>
    /// <remarks>
    /// Marked rather than left implicit. Marking only the defaulted half would encode the
    /// distinction in an <i>absence</i>, which is exactly the encoding that failed: a
    /// reader who greps for a variable name and gets one line back cannot tell a
    /// convention they have never read about from a line that simply carries no marker.
    /// </remarks>
    Declared,

    /// <summary>
    /// Nothing was supplied, so the value is this host's own fallback.
    /// </summary>
    /// <remarks>
    /// This is the case issue #2586 was filed about, and the reason the marker has to
    /// share a line with the value: a reader who greps for the variable name must receive
    /// the qualification in the same result, or the qualification is a second thing to
    /// read and the failure recurs.
    /// </remarks>
    Defaulted,

    /// <summary>
    /// Not a setting: a fact about the runtime this process is executing in, which no
    /// declaration of this name produced.
    /// </summary>
    Runtime,
}
