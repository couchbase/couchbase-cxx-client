# cbc - Couchbase client {#cbc}

### NAME

`cbc` - command line utility for Couchbase Server

### SYNOPSIS

`cbc <command> [<argument>...]`<br/>
`cbc (-h|--help)`<br/>
`cbc --version`

### DESCRIPTION

`cbc` allows to connect to Couchbase Server and perform some common operations, like accessing documents, running queries,
etc.

### COMMANDS

<dl>
<dt>version</dt>
<dd>
Display version information.
</dd>

<dt>get</dt>
<dd>
Retrieve document from the server. See [cbc-get](md_docs_2cbc-get) for more information.
</dd>

<dt>query</dt>
<dd>
Perform N1QL query. See [cbc-query](md_docs_2cbc-query) for more information.
</dd>
</dl>

### OPTIONS

<dl>
<dt>`-h` `--help`</dt><dd>Show help message.</dd>
<dt>`--version`</dt><dd>Show version.</dd>
</dl>

### SEE ALSO

[cbc-get](md_docs_2cbc-get), [cbc-query](md_docs_2cbc-query).
