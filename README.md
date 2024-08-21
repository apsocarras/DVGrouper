
### dv-grouper 

A package for reading, grouping, validating, and documenting DataFrames using Polars, Pandas, PySpark, or GeoPandas. 

#### Reading and Validation

* Read/group related files into DataFrames accessible with `"."` attribute syntax. 
    * Nested directories retain their hierarchical structure (`/` -> `.`)
    * Filter against an expected list of files or a regex pattern (to either include or exclude) 
* Specify a Pandera schema for each DataFrame to validate against when reading from a file or adding an existing DataFrame to the DVGrouper.
    * These schemas are also accessible in `dvg.__internal.schemas.py` (**Do not edit this file directly**).

#### User-Defined Functions 

Write and assign your own functions...
    * ...to a particular DataFrame using the `@dvg.tag_df` decorator (or `<yourDVG>.add_method(func, df=<df_name>)`)
    * ...to all DataFrames in a DVG using the `@dvg.tag_dvg` decorator (or `<yourDVG>.add_method(func, df="all")`)

Tagging a function to a particular DVGrouper/DataFrame automatically includes the function in the correct page of your `MKDocs` documentation and adds type hints to the function definition, in order to validate inputs against the DataFrame's Pandera schema. 

Some Examples: 
* Check a list of columns for statistical normality and include metadata file (`include_in_metadata`).
* Run a P-Test for a correlation between two columns.
* Generate and store plots in a given folder & embed in your project documentation.
* Output any failed schema validation attempts to a log file.

#### Metadata and Documentation (`MKDocs`)

* Collect metadata on column schema (column types, example values, index column, no. distinct values, no. duplicate values, no. missing values), no. of rows, file and column size, ranges of data (e.g. for a "years" column), time last read, I/O time, time last updated.
    * Auto-generate a YAML file with this metadata (`metadata.yaml`) 
    * Incorporate with a separate file (`codebook.yaml`) for manually editing arbitrary fields (e.g. links to external data sources, written descriptions of columns).
* Automatically generate a directory of `MKDocs`-compatible markdown documentation for your DataFrames' metadata and their associated functions. 
    * Add a new DVG to an existing project's documentation.
    * Inject Markdown text blocks (including image or web links) relative to a specified header in the documentation. 

#### REST API (`FastAPI`)

`DVGrouper` encourages the development of data-oriented Python APIs for DataFrame workflows. You can quickly extend your own Python API into a REST API with `FastAPI`.

* Initialize `FastAPI` project and directory structure.
    * Automatically include GET routes for metadata on your DVGroupers, DataFrames, and associated functions.
* Tag your functions with HTTP routes and see these integrated into your `MKDocs` documentation.


