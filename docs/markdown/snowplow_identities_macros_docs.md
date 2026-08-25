{% docs macro_get_cluster_by_values %}
{% raw %}

A macro to manage the cluster by fields for various models in the package.

#### Returns

The field to cluster by based on model name and target type.

{% endraw %}
{% enddocs %}

{% docs macro_get_incremental_filter %}
Emits the incremental `where` predicate for a self-watermarking model.

Each model asks its own table for the newest `load_tstamp` it already holds and processes forward from there in day-aligned windows, so there is no shared manifest and no run hooks. A failed run leaves the affected model slightly behind, and it catches up on the next run.

The window re-scans `snowplow__lookback_days` of overlap for late-arriving data; combined with each model's `unique_key` upsert, that makes re-running a batch a no-op, which is what replaces the manifest's crash-recovery guarantee.

Progress is throttled to `snowplow__backfill_limit_days` new days per run. A gap between events larger than that stalls progress, because the window can never reach the next event. Keep the limit comfortably above the largest gap you expect.
{% enddocs %}
