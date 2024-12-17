## dv-grouper 

A package for reading, grouping, validating, and documenting DataFrames using Polars and Pandera. 

For data complex enough to warrant modeling, validation, testing, and documentation, but small enough to where you can work with everything on one machine.

---


`DVGrouper` ← `DVBundle` ← `pl.DataFrame`

A `DVGrouper` holds a collection of `DVBundle` objects, each of which is a `pl.DataFrame` with associated attributes: 

0. **Data Loaders**: In-built functions to load data from local or blob storage (AWS, Azure, GCP).
1. **Validation Schema**: Pandera `DataFrameModel` used when reading data files and assigning a new `DataFrame` to the `DVBundle`.
2. **Annotated Functions**: Functions which have this `DataFrame` as either an input or an output.
3. **Other Metadata**: A `DVBundle` automatically generates a dict of metadata on the source `DataFrame`'s columns and other details.

### Validating Data 

A `DVGrouper` allows you to read & group related `.parquet` files into `DVBundles` accessible with `"."` attribute syntax.
* Parses file names into valid Python attribute names.
* Nested directories retain their hierarchical structure (`/` becomes `.`).
* Filter against an expected list of files or a regex pattern to avoid accidental reads of other files. 

```python
```

Prior to reading any data, you define `DVBundle` objects and assign them to the DVGrouper. After associating each DVBundle with a `source` path and a `DataFrameModel`, you can load all data and validate each on read.   

```python
```

You can also assign an existing DataFrame to a `DVBundle` after giving it a `DataFrameModel` and it will validate on assignment. 

```python
```

This is particularly powerful if you combine two of your data sources in your grouper and want to validate and store the result in-memory.

```python
```

Note that the `DataFrameModel` in each `DVBundle` can be used to validate any DataFrame without adding it. 

```python
```

### Annotating Functions

Whenever you include a `DVBundle` in a function signature (either in the parameters or the return type hint) use `@bundle.tag` to tag the function as being associated with the DVBundle. This will add the function to the `DVBundle.functions` tuple and add the DVBundle to the function (in a tuple called `bundles`).

```python
```

`@dvg.tag` can similarly be used to tag a function as belonging to a particular `DVGrouper`.  


### Metadata

Once assigned a DataFrame, a `DVBundle` can create a dict of metadata containing information on:   
- **Schema**: column names, types, size in-memory, and descriptions; column values (example values, no. of distinct values, no. of missing values, ranges of values (for `date` columns)); no. of rows, no. of missing rows  
- **Source File**: path, size, time when read (unless assigned an existing DF)
- **Annotated Functions**: See "Annotating Functions"
- **Descriptions**: A descriptive name for the DataFrame and any long-form text provided 

#### Custom Metadata Functions 

You can also define a list of custom functions to run when the metadata dict is generated. 

For example, generate and store plots in a given folder: 

```python 
```

Other ideas: 
* Check a list of columns for statistical normality. 
* Run a P-Test for a correlation between two columns.
* Output any failed schema validation attempts to a log file.

### Documentation with `MKDocs`

A `DVGrouper` object can read from the metadata of each `DVBundle` to create `MKDocs`-compatible markdown for documenting all the data sources in its collection. 

```python 
```
<!-- TODO: python command to generate docs structure -->

```bash
```
<!-- TODO: directory structure -->

Each DVBundle will have its own dedicated page in the static site:  
  
```
```
<!-- TODO: GIF or screenshot -->

#### Custom Markdown Templating Functions 

You can override the default markdown template function for a `DVGrouper`: 

```
```
Follow the type signature of the default function in the `DVGrouper` class definition.

#### Example Site

For larger projects, you can create multiple `DVGroupers` and include their auto-generated output in various sections. This could involve using custom markdown templating functions to generate documentation for each grouper and then writing a final build script which incorporates these sections together. 

 See [`mkdocs_examples/`](mkdocs_examples/) for an example of how you might approach this using the [MKDocs Material](https://squidfunk.github.io/mkdocs-material/) theme.



