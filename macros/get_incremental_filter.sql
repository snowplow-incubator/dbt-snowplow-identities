{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{#
  Self-watermarking incremental filter: each model processes forward from the newest
  load_tstamp in its own table, in day-aligned windows.

    relation_alias        alias of the source relation in the query (e.g. 'e'), or none.
    use_atomic_partition  also emit a predicate on atomic's physical partition column so the
                          warehouse can prune. Configure with snowplow__partition_tstamp and
                          snowplow__partition_tstamp_type.

  Window is watermark - (lookback_days - 1) to watermark + backfill_days + 1. The lookback
  overlap plus each model's unique_key upsert makes re-running a batch a no-op. A gap between
  events larger than snowplow__backfill_limit_days stalls progress.
#}

{% macro get_incremental_filter(relation_alias=none, use_atomic_partition=false) %}
  {%- set alias = (relation_alias ~ '.') if relation_alias is not none else '' -%}
  {%- set start_date = var('snowplow__start_date') -%}
  {%- set lookback = var('snowplow__lookback_days', 1) -%}
  {%- set backfill = var('snowplow__backfill_limit_days', 30) -%}
  {%- set partition_col = var('snowplow__partition_tstamp', 'load_tstamp') -%}
  {%- set partition_type = var('snowplow__partition_tstamp_type', 'timestamp') | lower -%}
  {%- set partition_buffer = var('snowplow__partition_buffer_days', 2) -%}
  {%- set start_ts = "cast('" ~ start_date ~ "' as timestamp)" -%}

  {%- if partition_type not in ['timestamp', 'date'] -%}
    {{ exceptions.raise_compiler_error(
      "snowplow__partition_tstamp_type must be 'timestamp' or 'date', got '" ~ partition_type ~ "'."
    ) }}
  {%- endif -%}

  {%- if is_incremental() -%}
    {%- set wm_day -%}
      (select coalesce(max(cast(load_tstamp as date)), cast({{ start_ts }} as date)) from {{ this }})
    {%- endset -%}
    {%- set lower_day = dbt.dateadd('day', 1 - lookback, wm_day) -%}
    {%- set upper_day = dbt.dateadd('day', 1 + backfill, wm_day) -%}
    {%- do snowplow_identities.log_incremental_window(start_date, lookback, backfill) -%}
  {%- else -%}
    {%- set lower_day = "cast(" ~ start_ts ~ " as date)" -%}
    {%- set upper_day = dbt.dateadd('day', backfill, "cast(" ~ start_ts ~ " as date)") -%}
    {%- do snowplow_identities.log_full_refresh_window(start_date, backfill) -%}
  {%- endif -%}

  {{ alias }}load_tstamp >= cast({{ lower_day }} as timestamp)
  and {{ alias }}load_tstamp < cast({{ upper_day }} as timestamp)
  {%- if use_atomic_partition and partition_col != 'load_tstamp' %}
  and {{ alias }}{{ partition_col }} >= cast({{ dbt.dateadd('day', -partition_buffer, lower_day) }} as {{ partition_type }})
  and {{ alias }}{{ partition_col }} < cast({{ dbt.dateadd('day', partition_buffer, upper_day) }} as {{ partition_type }})
  {%- endif -%}
{% endmacro %}


{#
  Log the window a model is about to process, replacing the manifest framework's
  print_run_limits. Bounds in the predicate are a subquery, so this resolves the real dates
  with one max(load_tstamp) query. Restricted to `dbt run` / `dbt build`: during unit tests
  is_incremental() is overridden while {{ this }} may not exist.
#}

{% macro log_incremental_window(start_date, lookback, backfill) %}
  {%- if not execute -%}{{ return('') }}{%- endif -%}
  {%- set which = flags.WHICH | default('', true) -%}
  {%- if which not in ['run', 'build'] -%}{{ return('') }}{%- endif -%}

  {%- set start_ts = "cast('" ~ start_date ~ "' as timestamp)" -%}
  {%- set wm_day -%}
    (select coalesce(max(cast(load_tstamp as date)), cast({{ start_ts }} as date)) from {{ this }})
  {%- endset -%}

  {%- set query -%}
    select
      cast({{ dbt.dateadd('day', 1 - lookback, wm_day) }} as {{ dbt.type_string() }}) as lower_bound,
      cast({{ dbt.dateadd('day', 1 + backfill, wm_day) }} as {{ dbt.type_string() }}) as upper_bound,
      cast({{ wm_day }} as {{ dbt.type_string() }}) as watermark
  {%- endset -%}

  {%- set results = run_query(query) -%}
  {%- if results and results.rows | length > 0 -%}
    {%- set row = results.rows[0] -%}
    {{ snowplow_utils.log_message(
      "Snowplow: " ~ this.identifier ~ " at watermark " ~ row[2]
      ~ " -- processing load_tstamp from " ~ row[0] ~ " to " ~ row[1]
    ) }}
  {%- endif -%}
{% endmacro %}


{% macro log_full_refresh_window(start_date, backfill) %}
  {%- if not execute -%}{{ return('') }}{%- endif -%}
  {%- set which = flags.WHICH | default('', true) -%}
  {%- if which not in ['run', 'build'] -%}{{ return('') }}{%- endif -%}
  {{ snowplow_utils.log_message(
    "Snowplow: " ~ this.identifier ~ " building from scratch -- processing "
    ~ backfill ~ " day(s) of load_tstamp from snowplow__start_date " ~ start_date
    ~ ". Re-run to advance further."
  ) }}
{% endmacro %}
