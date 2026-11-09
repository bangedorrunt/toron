export const title = "Toron Durable Workflow DAG and Approval Lifecycle";

export function state(vars: Record<string, any>): string {
  const phase = String(vars.phase ?? "Ready");
  const nodes = (vars.node_state ?? {}) as Record<string, string>;

  if (phase === "Ready") return "Workflow Ready (pending start)";
  if (phase === "Succeeded") return "Workflow Succeeded (all settled)";
  if (phase === "Failed") {
    const failedNode = Object.entries(nodes).find(([_, s]) => s === "Failed")?.[0] ?? "unknown";
    return `Workflow Failed (at node ${failedNode})`;
  }
  if (phase === "Cancelled") return "Workflow Cancelled";

  const inFlight = Object.entries(nodes).filter(
    ([_, s]) => s === "Executing" || s === "Parked" || s === "AwaitingApproval",
  );
  if (inFlight.length === 0) {
    return "Running: idle / resolving waves";
  }
  const [name, s] = inFlight[0];
  if (s === "Executing") {
    return `Executing node ${name}`;
  }
  if (s === "Parked") {
    return `Node ${name} parked (delay/backoff)`;
  }
  if (s === "AwaitingApproval") {
    return `Node ${name} awaiting operator approval`;
  }
  return `Running (${name}=${s})`;
}

export function step(
  action: string,
  _from: Record<string, any>,
  _to: Record<string, any>,
): string | null {
  if (action === "Done") return null;
  if (action === "StartWorkflow") return "start workflow";
  if (action.startsWith("ScheduleNode")) return "dispatch ready node (needs met)";
  if (action.startsWith("NodeSucceeds")) return "node completed successfully";
  if (action.startsWith("NodeParks")) return "node parks (interruption/delay)";
  if (action.startsWith("NodeWakes")) return "park deadline reached (resume node)";
  if (action.startsWith("NodeRequestsApproval")) return "node requires human gate";
  if (action.startsWith("OperatorApproves")) return "operator signs off";
  if (action.startsWith("OperatorRejects")) return "operator rejects";
  if (action.startsWith("NodeFails")) return "node failure";
  if (action === "WorkflowSucceeds") return "all nodes settled -> mark succeeded";
  if (action === "CancelWorkflow") return "workflow cancelled";
  return action;
}

export function group(vars: Record<string, any>): string {
  const phase = String(vars.phase ?? "Ready");
  if (phase === "Ready") return "Pending";
  if (phase === "Running") return "Active";
  if (phase === "Succeeded") return "Success";
  if (phase === "Failed") return "Failure";
  if (phase === "Cancelled") return "Cancelled";
  return "Other";
}

export const colors: Record<string, string> = {
  Pending: "blue",
  Active: "teal",
  Success: "purple",
  Failure: "orange",
  Cancelled: "tan",
};
