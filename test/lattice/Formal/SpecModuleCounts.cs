namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// The counts a module's manifest declares, each of which a gate re-derives
/// from the module itself and requires to match exactly.
/// <para>
/// Equalities, not floors, on purpose. A gate that compares two derived sets
/// (properties against mutation targets, actions against note rows) stays green
/// when both sides shrink together, so deleting a property and its mutation in
/// one commit would pass unnoticed. A declared count makes that deletion a
/// deliberate act: the manifest, and the module README's counts table that is
/// checked against it, have to be edited in the same commit.
/// </para>
/// </summary>
/// <param name="Invariants">Names in the cfg's <c>INVARIANT(S)</c> blocks.</param>
/// <param name="Properties">Names in the cfg's <c>PROPERTY</c>/<c>PROPERTIES</c> blocks.</param>
/// <param name="Actions">Disjuncts of the module's <c>Next</c> relation, non-behavioural ones included.</param>
/// <param name="Mutations">The <c>.mutation</c> files in the module's mutation directory.</param>
/// <param name="BehaviourRows">
/// Rows of the refinement note's action and property tables that assert a
/// production behaviour, so must cite a detector.
/// </param>
/// <param name="DistinctStates">
/// The distinct states TLC reports for the base model. Checked by
/// <see cref="TlcModelCheckTests"/>, the only gate that runs TLC; a change to
/// the specification that moves it has to restate it.
/// </param>
public sealed record SpecModuleCounts(
    int Invariants,
    int Properties,
    int Actions,
    int Mutations,
    int BehaviourRows,
    long DistinctStates);
