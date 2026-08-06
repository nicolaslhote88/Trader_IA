// Terminal shadow: no safety, writer or broker node is reachable from here.
const row = $json || {};
const proposals = row.modelProposals || row.agentDecision?.consensus?.modelProposals || [];
return [{json: {
  shadow: true,
  shadow_mode: "NO_BROKER_NO_WRITER",
  run_id: row.run?.runId || null,
  decision: row.decision || "NO_TRADE",
  proposal_count: proposals.length,
  valid_proposal_count: proposals.filter((proposal) => proposal.parse_ok).length,
  global_context_snapshot_id: row.run?.global_context_snapshot_id || null,
  global_context_payload_hash: row.run?.global_context_payload_hash || null,
  captured_at: new Date().toISOString(),
}}];
