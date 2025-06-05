# External Contract

dojo branch : dev-1.6.0

external_contracts can be anything

code_cairo:
world_contract.cairo

- contract_name -> can be anything defined, just as torii contracts already
- instance_name -> used to uniquely identify contracts.
- starting_block -> will be added to know where to start contract indexing

spawn and move:
[dojo_dev.toml](https://github.com/dojoengine/dojo/blob/dev-1.6.0/examples/spawn-and-move/dojo_dev.toml) -> [[external_contracts]]

indexing :

- either fork and create a new engine.start() process with new `ExternalContractRegistered`
- or restart current process but it will stop other contracts indexing
- on `ExternalContractUpgraded` either restart task or compare with what's changed to update in db

in fn process() -> create new engine from process

external_contract_registered.rs :

- check if contract can be indexed
- check if contract has already been registered
- add contract to db `contracts` table
- start contract indexing in a forked tokio task

external_contract_upgraded.rs :

- check if contract exists, if no, ignore
- upgrade contracts metadata, trigger update on tokens metadata

## Additonnal checks

- on program exit, stop `external_contracts_registered` forked tasks
- change base `engine.start()` to not only use args contracts but also those dynamically registered, so that when restarted, contracts are still indexed
