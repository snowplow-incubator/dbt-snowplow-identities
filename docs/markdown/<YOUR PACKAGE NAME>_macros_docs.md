{% docs macro_allow_refresh %}
{% raw %}
This macro is used to determine if a full-refresh is allowed (depending on the environment), using the `snowplow__allow_refresh` variable.

#### Returns
`snowplow__allow_refresh` if environment is not `dev`, `none` otherwise. Returns `none` if the `--full-refresh` flag is not present.

{% endraw %}
{% enddocs %}

{% docs macro_get_cluster_by_values %}
{% raw %}

A macro to manage the cluster by fields for various models in the package.

#### Returns

The field to cluster by based on model name and target type.

{% endraw %}
{% enddocs %}
