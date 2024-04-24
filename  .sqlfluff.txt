[sqlfluff]
# verbose is an integer (0-2) indicating the level of log output
verbose = 0
# Turn off color formatting of output
nocolor = False
# Supported dialects https://docs.sqlfluff.com/en/stable/dialects.html
# Or run 'sqlfluff dialects'
dialect = duckdb
# One of [dbt|raw|jinja|python|placeholder]
templater = jinja
# Comma separated list of rules to check, default to all
rules = AL01,AL02,AL03,AL05,AM02,AM05,CP01,CP02,CP03,CP04,CP05,CV02,CV03,CV04,CV05,CV10,CV11,JJ01,LT02,LT04,LT05,LT06,LT07,LT08,LT09,LT10,LT11,RF01,RF02,RF03,RF04,RF05,RF06,ST03,ST04
# Comma separated list of rules to exclude, or None
# Below controls SQLFluff output, see max_line_length for SQL output
output_line_length = 80
# Number of passes to run before admitting defeat
runaway_limit = 10
# Ignore errors by category (one or more of the following, separated by commas: lexing,linting,parsing,templating)
ignore = None
# Warn only for rule codes (one of more rule codes, seperated by commas: e.g. LT01,LT02)
# Also works for templating and parsing errors by using TMP or PRS
warnings = None
# Ignore linting errors found within sections of code coming directly from
# templated code (e.g. from within Jinja curly braces. Note that it does not
# ignore errors from literal code found within template loops.
ignore_templated_areas = True
# can either be autodetect or a valid encoding e.g. utf-8, utf-8-sig
encoding = autodetect
# Ignore inline overrides (e.g. to test if still required)
disable_noqa = False
# Comma separated list of file extensions to lint
# NB: This config will only apply in the root folder
sql_file_exts = .sql,.sql.j2,.dml,.ddl
# Allow fix to run on files, even if they contain parsing errors
# Note altering this is NOT RECOMMENDED as can corrupt SQL
fix_even_unparsable = False
# Very large files can make the parser effectively hang.
# The more efficient check is the _byte_ limit check which
# is enabled by default. The previous _character_ limit check
# is still present for backward compatibility. This will be
# removed in a future version.
# Set either to 0 to disable.
large_file_skip_char_limit = 0
large_file_skip_byte_limit = 30000
# CPU processes to use while linting.
# If positive, just implies number of processes.
# If negative or zero, implies number_of_cpus - specified_number.
# e.g. -1 means use all processors but one. 0  means all cpus.
processes = 1
# Max line length is set by default to be in line with the dbt style guide.
# https://github.com/dbt-labs/corp/blob/main/dbt_style_guide.md
# Set to zero or negative to disable checks.
max_line_length = 120

[sqlfluff:indentation]
# See https://docs.sqlfluff.com/en/stable/layout.html#configuring-indent-locations
indented_joins = False
indented_using_on = True
indented_on_contents = True
indent_unit = space
tab_space_size = 4
indented_ctes = False
indented_then = True
indented_then_contents = True
allow_implicit_indents = True
template_blocks_indent = True
# This is a comma seperated list of elements to skip
# indentation edits to.
skip_indentation_in = script_content
# If comments are found at the end of long lines, we default to moving
# them to the line _before_ their current location as the convention is
# that a comment precedes the line it describes. However if you prefer
# comments moved _after_, this configuration setting can be set to "after".
trailing_comments = before

# Layout configuration
# See https://docs.sqlfluff.com/en/stable/layout.html#configuring-layout-and-spacing

[sqlfluff:rules:capitalisation.keywords] 
capitalisation_policy = lower
extended_capitalisation_policy = lower

[sqlfluff:rules:capitalisation.identifiers]  
extended_capitalisation_policy = lower

[sqlfluff:rules:capitalisation.functions] 
extended_capitalisation_policy = lower

[sqlfluff:rules:capitalisation.literals] 
extended_capitalisation_policy = lower

[sqlfluff:rules:convention.select_trailing_comma]
select_clause_trailing_comma = forbid

[sqlfluff:rules:convention.quoted_literals]
preferred_quoted_literal_style = consistent
preferred_type_casting_style = cast

[sqlfluff:layout:type:comma] 
spacing_before = touch
line_position = trailing