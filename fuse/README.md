## FUSE bindings for galaxy

### Currently Supported Operations
- histories
 - view (ls) - sizes are not yet reported, create/modified times are not yet mapped
 - create (mkdir) - requires a name to be supplied, this isn't necessarily a bad thing
 - delete (rmdir) - does not require the directory/history to be empty
 - rename (mv) - restricted to renaming the history, i.e. destination must be within /histories

- history dataset (simple, no dataset collections yet)
 - view (ls) - doesn't show deleted datasets, no sizes, times not yet mapped
 - delete (rm)
 
### Significant Dependencies
 - [bioblend] (https://github.com/galaxyproject/bioblend)
 - [fusepy](https://github.com/terencehonles/fusepy)
