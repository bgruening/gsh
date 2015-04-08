## FUSE bindings for galaxy

### To test ###
Spin up a local galaxy instance e.g. `bgruening/galaxy-rna-workbench-extra`.
run `python gfs.py -m /your/mount/of/choice localhost:8080 b1c0dc86e63137e7727a2b9941c65de8`

### Currently Supported Operations
- histories
 - view (ls) - sizes are not yet reported, create/modified times are not yet mapped
 - create (mkdir) - requires a name to be supplied, this isn't necessarily a bad thing
 - delete (rmdir) - does not require the directory/history to be empty
 - rename (mv) - restricted to renaming the history, i.e. destination must be within /histories

- history dataset (simple, no dataset collections yet)
 - view (ls) - doesn't show deleted datasets, no sizes, times not yet mapped
 - delete (rm)
 
### Major Issues ###
- There is no caching yet, so list/tab complete ops can be very slow.
- No read/create operations on datasets yet.

### Significant Dependencies
 - [bioblend] (https://github.com/galaxyproject/bioblend)
 - [fusepy](https://github.com/terencehonles/fusepy)
