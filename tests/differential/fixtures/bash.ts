import { Pipeline } from "../../../src/pipeline";

export function build(): Pipeline {
  const pipeline = new Pipeline("Bash export", { pipelineId: "bash-export" });
  const source = pipeline.sourceFile("Input", { path: "/data/in.csv" });
  pipeline.bash("Refresh", source, {
    command: './refresh.sh --since "$SINCE"',
    workingDir: "/data/jobs",
    env: { SINCE: "${param.since}" },
    timeout: 600,
    retries: 2,
    retryDelay: 30000,
    maxMemoryMb: 512,
    maxCpuSeconds: 300,
    nodeKey: "refresh-export",
  });
  return pipeline;
}
