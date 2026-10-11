# Admission mutation controls

These 17 controls cover every invariant, action property, temporal property
and behavioural action of `TreeLifecycleAdmission`. Each mutant is applied
to the current source and checked against a clean control with the same
single-property configuration. A named violation is required; a parser,
evaluation or unrelated-property failure is not evidence.

The two refusal-stall controls retain a refused announcement and make its
retry treat the completed competing record as permanently busy. Retaining
the announcement alone is not a liveness defect: after the peer withdraws,
the correct admission guard can admit the retry. The combined perturbation
strands real work without deleting fairness.
