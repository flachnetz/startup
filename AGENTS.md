# AGENTS.md — startup

## Platform auth work (in progress)

Keycloak service-to-service authentication, staff authorization, and the two-step refund
approval are planned centrally, not per repository. Before touching authentication,
authorization, `/internal` routes, history actions, or actor propagation, read the plan and
take an issue instead of improvising:

- plan: `BauerMediaGroup-Stardust/platform-gitops`, file `docs/plans/keycloak-service-auth.md`
  — section 0 is the entry point.
- tracker: beads in `BauerMediaGroup-Stardust/platform-gitops`, issue prefix
  `platform-gitops-`. Run `bd ready` from a checkout of that repository.
