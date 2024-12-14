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
Retrieve document from the server. See [cbc-get](#cbc-get) for more information.
</dd>

<dt>upsert</dt>
<dd>
Store the document on the server. See [cbc-upsert](#cbc-upsert) for more information.
</dd>

<dt>remove</dt>
<dd>
Remove the document on the server. See [cbc-remove](#cbc-remove) for more information.
</dd>

<dt>query</dt>
<dd>
Perform N1QL query. See [cbc-query](#cbc-query) for more information.
</dd>

<dt>analytics</dt>
<dd>
Perform Analytics query. See [cbc-analytics](#cbc-analytics) for more information.
</dd>

<dt>pillowfight</dt>
<dd>
Run simple workload generator. See [cbc-pillowfight](#cbc-pillowfight) for more information.
</dd>
</dl>

<dt>keygen</dt>
<dd>
Generate batches of keys with specific properties. See [cbc-keygen](#cbc-keygen) for more information.
</dd>

### OPTIONS

<dl>
<dt>`-h` `--help`</dt><dd>Show help message.</dd>
<dt>`--version`</dt><dd>Show version.</dd>
</dl>

### SEE ALSO

[cbc-get](#cbc-get), [cbc-upsert](#cbc-upsert), [cbc-remove](#cbc-remove), [cbc-query](#cbc-query).
