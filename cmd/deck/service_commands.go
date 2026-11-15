package main

// Task 8 preserves this hook when it installs cli.App dispatch. Task 6 extends
// it with service startup after constructing its runtime handler.
func init() { serviceCommand = keyCommand }
