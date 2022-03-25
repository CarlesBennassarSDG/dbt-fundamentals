{%- macro set_snowflake_query_tag(tag = '') %}
    {% set new_tag = tag + '___' + model.name %}
    {% do run_query("alter session set query_tag = '{}'".format(new_tag)) %}
{%- endmacro %}