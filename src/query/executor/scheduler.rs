use super::task::QueryTask;

pub fn shard_tasks(
    docs: Vec<(String, Vec<u8>)>,
    workers: usize,
    plan: super::super::plan::QueryPlan,
) -> Vec<QueryTask> {
    let worker_count = workers.max(1);
    let chunk_size = (docs.len() / worker_count).max(1);
    docs.chunks(chunk_size)
        .map(|chunk| QueryTask {
            docs: chunk.to_vec(),
            plan: plan.clone(),
        })
        .collect()
}
